// LEARN: unwrap_or_default
// LEARN: #[cfg(feature = "datafusion")]

// TODO: improve the errors
// IDEA: read the csv with datafusion context

mod helpers;

use std::env;
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Buf;
use serde_json::Value;
use log::{error, info, warn};

use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{header, Error, Request, Response, StatusCode};

// this is helper for hyper
use http_body_util::{Full, BodyExt};

use tokio::net::TcpListener;

use deltalake::DeltaTableError;
use deltalake::DeltaTableBuilder;
use deltalake::arrow::csv::ReaderBuilder;
use deltalake::schema::Schema as DeltaSchema;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::arrow::datatypes::{
    Field as ArrowField,
    Schema as ArrowSchema,
    DataType as ArrowDataType,
};

use helpers::blob_path::BlobPath;
use helpers::merge_func::user_list_merge;
use helpers::blob_event::{BlobEvent, Root};
use helpers::delta_ops::create_and_write_table;
use helpers::azure_storage::{fetch_file, get_azure_store};

async fn handler(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    info!("Rust HTTP trigger function begun");

    // Collect the body into bytes
    let whole_body = req.collect().await;

    // Read the request body as bytes.
    let body_bytes = match whole_body {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return Ok(
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Full::new(Bytes::from("{\"body\": \"Error reading request body\"}")))
                    .unwrap()
                )
            }
        };

    // Check if the Body is Empty
    if body_bytes.is_empty() {
        return Ok(
            Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from("{\"body\": \"No Content Success Status Response was sent!\"}")))
            .unwrap()
        )
    }

    // Parse the JSON request.
    let json_request: Result<Value, serde_json::Error> = serde_json::from_slice(&body_bytes);

    match json_request {

        // If the JSON request is valid
        Ok(json_request) => {

            // Deserialize the JSON request
            let deserialized_data: Root = serde_json::from_str(&json_request.to_string()).unwrap();

            // Convert the Root instance to a BlobEvent instance
            let blob_event = BlobEvent::from(deserialized_data);
            info!("{:?}", blob_event);

            // Create a blob_path object
            let blob_path = BlobPath::from_blob_url(&blob_event.blob_url).unwrap();
            info!("--- Blob Path: {:?}", &blob_path);

            // Create an azure store object for specific container
            let azure_store = get_azure_store("data");

            // Fetch the file from azure storage
            let fetched = fetch_file(azure_store.clone(), blob_path).await;

            // Define the Arrow schema for reading the CSV
            let schema = ArrowSchema::new(vec![
                ArrowField::new("username", ArrowDataType::Utf8, false),
                ArrowField::new("email", ArrowDataType::Utf8, false),
                ArrowField::new("account_type", ArrowDataType::Utf8, false),
                ArrowField::new("payment_method", ArrowDataType::Utf8, true),
                ArrowField::new("credit_card_type", ArrowDataType::Utf8, true),
                ArrowField::new("payment_id", ArrowDataType::Utf8, true),
            ]);

            let schema = Arc::new(schema);

            // Create a DeltaTable schema
            let delta_schema = schema.clone();
            let delta_schema = DeltaSchema::try_from(delta_schema).unwrap();

            // Create a csv reader
            let reader = fetched.reader();
            let mut csv = ReaderBuilder::new(schema)
                                        .has_header(true)
                                        .build(reader)
                                        .unwrap();

            // Create a RecordBatch Object from CSV
            let record_batch = csv.next().unwrap().unwrap();

            // Creating a Datafusion Dataframe from a record batch
            let ctx = SessionContext::new();
            let source_table = ctx.read_batch(record_batch.clone()).unwrap();

            info!("--- Table Schema {:?}", source_table.schema());

            // Get the need env variable and create backend config for object store in ADLS
            let azure_storage_access_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
            let mut backend_config: HashMap<String, String> = HashMap::new();
            backend_config.insert(
                "azure_storage_access_key".to_string(),
                azure_storage_access_key,
            );

            let target_table_path = "abfs://data@ds0learning0adls.dfs.core.windows.net/userslist/";

            // TODO: Add partitioning later
            //let partition_columns = vec!["account_type".to_string()];

            // Get the table source, if it doesn't exist create it
            let _ = match DeltaTableBuilder::from_uri(target_table_path)
                .with_storage_options(backend_config.clone())
                .load()
                .await
            {
                // if the table exists, merge the data in it
                Ok(table) => {
                    user_list_merge(table, source_table).await;
                    Ok(())
                },

                // if the table does not exist, create it
                Err(DeltaTableError::NotATable(e)) => {
                    warn!("-!!!- Warning: Table does not exist! {}", e);
                    let new_table = create_and_write_table(&record_batch, delta_schema, target_table_path, backend_config).await.unwrap();
                    info!("--- Created Delta Table and wrote the data in it!");
                    info!("--- Schema of Delta Table: \n {:#?}", new_table.get_schema());
                    Ok(())
                }
                // Propagate other errors
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            };

            // Return an appropriate response.
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from("{\"body\": \"Success Status Response Code!\"}")))
                .unwrap();
            Ok(response)
        }
        Err(err) => {
            // Handle JSON parsing error here.
            error!("Error parsing JSON: {:?}", err);
            // Return an appropriate error response.
            let response = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(
                    format!("{{\"body\": \"Error parsing JSON: {:?}\"}}", err)
                )))
                .unwrap();
            Ok(response)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    info!("Rust Function Starts!");

    // Set the RUST_LOG env variable to "INFO" to see the logs
    std::env::set_var("RUST_LOG", "INFO");

    // LEARN: Logging
    // LEARN: https://docs.rs/env_logger/0.10.0/env_logger/#enabling-logging
    env_logger::init();

    // Get port from env variable and set to 3000 if not set
    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let port: u16 = match env::var(port_key) {
        Ok(val) => val.parse().expect("Custom Handler port is not a number!"),
        Err(_) => 3000,
    };
    info!("Port: {}", port);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    // We create a TcpListener and bind it to 127.0.0.1:port
    let listener = TcpListener::bind(addr).await.unwrap();

    info!("Listening on http://{}", addr);
    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await.unwrap();

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(handler))
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
