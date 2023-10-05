use std::env;
use std::convert::Infallible;

use chrono::prelude::*;
use chrono::NaiveDateTime;
use chrono_tz::Europe::Berlin;

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Error, Request, Response, Server, StatusCode};

use serde_json::{Value, json};
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
struct EventData {
    api: String,
    #[serde(rename = "contentType")]
    content_type: String,
    #[serde(rename = "contentLength")] 
    content_length: i64,
    #[serde(rename = "blobType")] 
    blob_type: String,
    #[serde(rename = "blobUrl")] 
    blob_url: String,
    url: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct EventGridEvent {
    data: EventData,
    #[serde(rename = "eventTime")] 
    event_time: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    #[serde(rename = "eventGridEvent")] 
    event_grid_event: EventGridEvent,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Root {
    #[serde(rename = "Data")]
    data: Data
}

#[derive(Debug)]
struct BlobEvent {
    api: String,
    content_type: String,
    content_length: i64,
    blob_type: String,
    blob_url: String,
    url: String,
    event_time: NaiveDateTime
}


impl From<Root> for BlobEvent {
    fn from(root: Root) -> Self {
        let event_data = root.data.event_grid_event.data;
        let event_time = DateTime::parse_from_rfc3339(root.data.event_grid_event.event_time.as_str())
        .unwrap()
        .with_timezone(&Berlin)
        .naive_local();
        
        BlobEvent {
            api: event_data.api,
            content_type: event_data.content_type,
            content_length: event_data.content_length,
            blob_type: event_data.blob_type,
            blob_url: event_data.blob_url,
            url: event_data.url,
            event_time
        }
    }
}

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
            println!("{:?}", blob_event);

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{ 'body' :'Request body printed!'}"))
                .unwrap();
            Ok(response)
        }
        Err(err) => {
            // Handle JSON parsing error here.
            eprintln!("Error parsing JSON: {:?}", err);
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

    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let port: u16 = match env::var(port_key) {
        Ok(val) => val.parse().expect("Custom Handler port is not a number!"),
        Err(_) => 3000,
    };
    let addr = ([127, 0, 0, 1], port).into();

    let server = Server::bind(&addr).serve(make_service_fn(|_| async move {
        Ok::<_, Infallible>(service_fn(handler))
    }));

    println!("Listening on http://{}", addr);

    server.await

}