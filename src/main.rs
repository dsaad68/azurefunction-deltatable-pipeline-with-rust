use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Error, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::env;
use serde_json::Value;
use serde_json::json;

async fn handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
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

    println!("{:?}", body_bytes);

    // Check if the Body is Empty
    // Azure function code 204
    if body_bytes.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{ 'body' : 'Request body is empty'}"))
            .unwrap());
    }

    // Parse the JSON request.
    let json_request: Result<Value, serde_json::Error> = serde_json::from_slice(&body_bytes);

    //println!("{:?}", json_request);

    match json_request {
        Ok(json_request) => {
            // Extract the "Metadata" part.
            let metadata = json_request.get("Metadata").unwrap_or(&Value::Null);

            // Convert metadata back to JSON and print it.
            let metadata_json = serde_json::to_string(&metadata).unwrap();
            println!("Metadata: {}", metadata_json);

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{ 'body' :'Request body printed'}"))
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