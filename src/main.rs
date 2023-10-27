// LEARN: unwrap_or_default
// LEARN: #[cfg(feature = "datafusion")]

mod helpers;

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::sync::Arc;

use bytes::Buf;

use serde_json::{json, Value};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Error, Request, Response, Server, StatusCode};

use deltalake::arrow::csv::ReaderBuilder;
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
#[allow(unused_imports)]
use deltalake::operations::create::CreateBuilder;
#[allow(unused_imports)]
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::operations::DeltaOps;
#[allow(unused_imports)]
use deltalake::protocol::*;
#[allow(unused_imports)]
use deltalake::DeltaTable;
#[allow(unused_imports)]
use deltalake::DeltaTableBuilder;
use deltalake::schema::Schema as DeltaSchema;

#[allow(unused)]
use deltalake::datafusion::logical_expr::{col, lit};
use deltalake::datafusion::prelude::SessionContext;

use helpers::azure_storage::{fetch_file, get_azure_store};
use helpers::blob_event::{BlobEvent, Root};
use helpers::blob_path::BlobPath;

async fn handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    println!("Rust HTTP trigger function begun");

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
            info!("{:?}", &blob_path);

            // Create an azure store object
            let azure_store = get_azure_store("samples-workitems");

            // Fetch the file from azure storage
            let fetched = fetch_file(azure_store.clone(), blob_path).await;

            let schema = ArrowSchema::new(vec![
                ArrowField::new("username", ArrowDataType::Utf8, false),
                ArrowField::new("email", ArrowDataType::Utf8, false),
                ArrowField::new("account_type", ArrowDataType::Utf8, false),
                ArrowField::new("payment_method", ArrowDataType::Utf8, true),
                ArrowField::new("credit_card_type", ArrowDataType::Utf8, true),
                ArrowField::new("payment_id", ArrowDataType::Utf8, true),
            ]);

            let schema = Arc::new(schema);
            
            #[allow(unused)]
            let table_schema = schema.clone();
            let delta_schema = schema.clone();
            #[allow(unused)]
            let delta_schema = DeltaSchema::try_from(delta_schema).unwrap();
            //let partition_columns = vec!["VendorName".to_string()];

            let reader = fetched.reader();

            // Create a csv reader
            let mut csv = ReaderBuilder::new(schema)
                .has_header(true)
                .build(reader)
                .unwrap();

            //    .with_delimiter(b';')

            // Create a RecordBatch Object from CSV
            let record_batch = csv.next().unwrap().unwrap();

            // STEP 1: Creating a Datafusion Dataframe from a record batch
            let ctx = SessionContext::new();
            let source_table = ctx.read_batch(record_batch.clone()).unwrap();

            println!("{:?}", source_table.schema());

            // STEP 2: get the need variable and create backend config
            let azure_storage_access_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
            let mut backend_config: HashMap<String, String> = HashMap::new();
            backend_config.insert(
                "azure_storage_access_key".to_string(),
                azure_storage_access_key,
            );

            let target_table_path =
                "abfs://samples-workitems@ds0learning0adls.dfs.core.windows.net/userslist/";

            // let target_table = CreateBuilder::new()
            //     .with_location(target_table_path)
            //     .with_storage_options(backend_config.clone())
            //     .with_columns(delta_schema.get_fields().clone())
            //     .await
            //     .unwrap();

            // let target_table = DeltaOps::from(target_table)
            //     .write(vec![record_batch.clone()])
            //     .with_save_mode(SaveMode::Overwrite)
            //     .await
            //     .unwrap();

            // STEP 3: get the table source, if it doesn't exist create it
            // TODO: Handle the error when there is no table at this URI
            let target_table = DeltaTableBuilder::from_uri(target_table_path)
                .with_storage_options(backend_config)
                .load()
                .await
                .unwrap();
            
            println!("{:?}", target_table.get_schema());

            // STEP 4: Merge Operation
            #[allow(unused)]
            let (mut table, metrics) = DeltaOps(target_table)
                .merge(source_table, col("username").eq(col("source.username")))
                .with_source_alias("source")
                .when_matched_update(|update| {
                    update
                        .update("account_type", col("source.account_type"))
                        .update("payment_method", col("source.payment_method"))
                        .update("credit_card_type", col("source.credit_card_type"))
                        .update("payment_id", col("source.payment_id"))
                })
                .unwrap()
                .when_not_matched_insert(|insert| {
                    insert
                        .set("username", col("source.username"))
                        .set("email", col("source.email"))
                        .set("account_type", col("source.account_type"))
                        .set("payment_method", col("source.payment_method"))
                        .set("credit_card_type", col("source.credit_card_type"))
                        .set("payment_id", col("source.payment_id"))
                })
                .unwrap()
                .await
                .unwrap();

                // TODO: add log about tables and metrics

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
    // LEARN: Logging
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
