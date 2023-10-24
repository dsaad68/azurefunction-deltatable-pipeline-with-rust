mod helpers;

use std::env;
use std::collections::HashMap;
use std::sync::Arc;
use std::convert::Infallible;

use bytes::Buf;

use serde_json::{Value, json};

#[allow(unused_imports)]
use log::{ info, error, debug, warn };

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Error, Request, Response, Server, StatusCode};

#[allow(unused_imports)]
use deltalake::protocol::*;
#[allow(unused_imports)]
use deltalake::DeltaTable;
use deltalake::arrow::csv::ReaderBuilder;
#[allow(unused_imports)]
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
#[allow(unused_imports)]
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::DeltaOps;
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use deltalake::schema::Schema as DeltaSchema;
// use deltalake::parquet::{
//     basic::{Compression, Encoding},
//     file::properties::*,
// };

#[allow(unused_imports)]
use deltalake::datafusion::prelude::SessionContext;
#[allow(unused_imports)]
use deltalake::datafusion::logical_expr::{col,lit};

#[allow(unused_imports)]
use deltalake::DeltaTableBuilder;

use helpers::blob_path::BlobPath;
#[allow(unused_imports)]
use helpers::delta_ops::get_delta_store;
use helpers::blob_event::{BlobEvent,Root};
use helpers::azure_storage::{get_azure_store, fetch_file};


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
            .body(Body::from("{ 'body' : 'No Content Success Status Response Code!'}"))
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
            let blob_path = BlobPath::from_blob_url(& blob_event.blob_url).unwrap();
            info!("{:?}", & blob_path);

            // Create an azure store object
            let azure_store = get_azure_store("samples-workitems");

            // Fetch the file from azure storage
            let fetched = fetch_file(azure_store.clone(), blob_path).await;

            let schema = ArrowSchema::new(vec![
                ArrowField::new("VendorID", ArrowDataType::Int32, false),
                ArrowField::new("VendorName", ArrowDataType::Utf8, false),
                ArrowField::new("AccountNumber", ArrowDataType::Utf8, false),
                ArrowField::new("CreditRating", ArrowDataType::Int32, false),
                ArrowField::new("ActiveFlag", ArrowDataType::Int32, false),
            ]);

            let schema = Arc::new(schema);
            #[allow(unused)]
            let table_schema = schema.clone();

            let delta_schema = schema.clone();
            let delta_schema = DeltaSchema::try_from(delta_schema).unwrap();
            
            let reader = fetched.reader();
            
            // Create a csv reader
            let mut csv = ReaderBuilder::new(schema).has_header(true).build(reader).unwrap();
            
            // Create a RecordBatch Object from CSV
            let record_batch = csv.next().unwrap().unwrap();
            
            // let output_path = "https://ds0learning0adls.blob.core.windows.net";
            // #[allow(unused)]
            // let delta_store = get_delta_store("samples-workitems/vendors", output_path);
            #[allow(unused)]
            let partition_columns = vec!["VendorName".to_string()];

            // STEP 1: get the need variable and create backend config
            let azure_storage_access_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
            let mut backend_config: HashMap<String, String> = HashMap::new();
            backend_config.insert("azure_storage_access_key".to_string(), azure_storage_access_key);

            let output_path = "abfs://samples-workitems@ds0learning0adls.dfs.core.windows.net/vendors/";

            // LEARN: unwrap_or_default
            // STEP 2: Check if the table exist
            let table = CreateBuilder::new()
                                .with_location(output_path)
                                .with_storage_options(backend_config.clone())
                                .with_columns(delta_schema.get_fields().clone())
                                .await
                                .unwrap();

            // STEP 3: Create refrence to the table 
            // let table = DeltaTableBuilder::from_uri(output_path)
            //     .with_storage_options(backend_config.clone())
            //     .build()
            //     .unwrap();

            // STEP 4: write some data 
            #[allow(unused)]
            let ops = DeltaOps::from(table)
                        .write(vec![record_batch.clone()])
                        .with_save_mode(SaveMode::Overwrite)
                        .await
                        .unwrap();

            // LEARN: #[cfg(feature = "datafusion")]

            // STEP3: Check if there table already there
            // #[allow(unused)]
            // let source_table = CreateBuilder::new()
            //                 .with_object_store(delta_store.clone())
            //                 .with_columns(delta_schema.get_fields().clone())
            //                 .with_partition_columns(& partition_columns)
            //                 .with_save_mode(SaveMode::Append)
            //                 .await
            //                 .unwrap();

            // STEP 5: Merge Operation
            // #[allow(unused)]
            // let (mut table, metrics) = DeltaOps(incoming_table)
            // .merge(source, col("id").eq(col("source.id")))
            // .with_source_alias("source")
            // .when_matched_update(|update| {
            //     update
            //         .update("value", col("source.value"))
            //         .update("modified", col("source.modified"))
            // })
            // .unwrap()
            // .when_not_matched_by_source_update(|update| {
            //     update
            //         .predicate(col("value").eq(lit(1)))
            //         .update("value", col("value") + lit(1))
            // })
            // .unwrap()
            // .when_not_matched_insert(|insert| {
            //     insert
            //         .set("id", col("source.id"))
            //         .set("value", col("source.value"))
            //         .set("modified", col("source.modified"))
            // })
            // .unwrap()
            // .await
            // .unwrap();

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
                .body(Body::from(serde_json::to_string(&json!({ "Body": format!("Error parsing JSON: {:?}", err) })).unwrap()))
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