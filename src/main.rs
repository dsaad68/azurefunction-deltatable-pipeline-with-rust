// LEARN: unwrap_or_default
// LEARN: #[cfg(feature = "datafusion")]

// TODO 1: improve the errors
// TODO 2: read the csv with datafusion context

mod helpers;

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::sync::Arc;

use bytes::Buf;

use serde_json::{json, Value};

// #[allow(unused_imports)]
use log::{error, info, warn};

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Error, Request, Response, Server, StatusCode};

// use deltalake::operations::create::CreateBuilder;
// use deltalake::operations::DeltaOps;
// use deltalake::protocol::SaveMode;
use deltalake::schema::Schema as DeltaSchema;
// use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use deltalake::DeltaTableError;

use deltalake::arrow::csv::ReaderBuilder;
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

// #[allow(unused)]
// use deltalake::datafusion::logical_expr::{col, lit};
use deltalake::datafusion::prelude::SessionContext;

use helpers::azure_storage::{fetch_file, get_azure_store};
use helpers::blob_event::{BlobEvent, Root};
use helpers::blob_path::BlobPath;
use helpers::merge_func::user_list_merge;
use helpers::delta_ops::create_and_write_table;

async fn handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    info!("Rust HTTP trigger function begun");

    // Read the request body as bytes.
    let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("Error reading request body"))
                .unwrap())
        }
    };

    // Check if the Body is Empty
    if body_bytes.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                "{ 'body' : 'No Content Success Status Response Code!'}",
            ))
            .unwrap());
    }

    // Parse the JSON request.
    let json_request: Result<Value, serde_json::Error> = serde_json::from_slice(&body_bytes);

    match json_request {
        Ok(json_request) => {
            let deserialized_data: Root = serde_json::from_str(&json_request.to_string()).unwrap();

            // Convert the Root instance to a BlobEvent instance
            let blob_event = BlobEvent::from(deserialized_data);
            info!("{:?}", blob_event);

            // Create a blob_path object
            let blob_path = BlobPath::from_blob_url(&blob_event.blob_url).unwrap();
            info!("--- Blob Path: {:?}", &blob_path);

            // Create an azure store object for specific container
            let azure_store = get_azure_store("data");

            info!("--- Getting File from Azure Storage [ ]");

            // Fetch the file from azure storage
            let fetched = fetch_file(azure_store.clone(), blob_path).await;

            info!("--- Getting File from Azure Storage [X]");

            let schema = ArrowSchema::new(vec![
                ArrowField::new("username", ArrowDataType::Utf8, false),
                ArrowField::new("email", ArrowDataType::Utf8, false),
                ArrowField::new("account_type", ArrowDataType::Utf8, false),
                ArrowField::new("payment_method", ArrowDataType::Utf8, true),
                ArrowField::new("credit_card_type", ArrowDataType::Utf8, true),
                ArrowField::new("payment_id", ArrowDataType::Utf8, true),
            ]);

            let schema = Arc::new(schema);

            let delta_schema = schema.clone();
            let delta_schema = DeltaSchema::try_from(delta_schema).unwrap();
            //let partition_columns = vec!["VendorName".to_string()];

            let reader = fetched.reader();

            // Create a csv reader
            let mut csv = ReaderBuilder::new(schema)
                .has_header(true)
                .build(reader)
                .unwrap();

            // Create a RecordBatch Object from CSV
            let record_batch = csv.next().unwrap().unwrap();

            // STEP 1: Creating a Datafusion Dataframe from a record batch
            let ctx = SessionContext::new();
            let source_table = ctx.read_batch(record_batch.clone()).unwrap();

            info!("--- Table Schema {:?}", source_table.schema());

            // STEP 2: get the need variable and create backend config
            let azure_storage_access_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
            let mut backend_config: HashMap<String, String> = HashMap::new();
            backend_config.insert(
                "azure_storage_access_key".to_string(),
                azure_storage_access_key,
            );

            let target_table_path =
                "abfs://data@ds0learning0adls.dfs.core.windows.net/userslist/";

            // STEP 3: get the table source, if it doesn't exist create it
            let _ = match DeltaTableBuilder::from_uri(target_table_path)
                .with_storage_options(backend_config.clone())
                .load()
                .await
            {
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

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{ 'body' :'Request body printed!'}"))
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
                .body(Body::from(
                    serde_json::to_string(
                        &json!({ "Body": format!("Error parsing JSON: {:?}", err) }),
                    )
                    .unwrap(),
                ))
                .unwrap();
            Ok(response)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "INFO");

    // LEARN: Logging
    // LEARN: https://docs.rs/env_logger/0.10.0/env_logger/#enabling-logging
    env_logger::init();

    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let port: u16 = match env::var(port_key) {
        Ok(val) => val.parse().expect("Custom Handler port is not a number!"),
        Err(_) => 3000,
    };
    let addr = ([127, 0, 0, 1], port).into();

    let server = Server::bind(&addr).serve(make_service_fn(|_| async move {
        Ok::<_, Infallible>(service_fn(handler))
    }));

    info!("Listening on http://{}", addr);

    server.await
}
