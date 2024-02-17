use chrono::prelude::*;
use chrono::NaiveDateTime;
use chrono_tz::Europe::Berlin;

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
    data: Data,
}

#[derive(Debug)]
pub struct BlobEvent {
    pub api: String,
    pub content_type: String,
    pub content_length: i64,
    pub blob_type: String,
    pub blob_url: String,
    pub blob_size_mb: f64,
    pub url: String,
    pub event_time: NaiveDateTime,
}

impl From<Root> for BlobEvent {
    fn from(root: Root) -> Self {
        let event_data = root.data.event_grid_event.data;
        let blob_size_mb = event_data.content_length as f64 / 1_048_576f64;
        let event_time =
            DateTime::parse_from_rfc3339(root.data.event_grid_event.event_time.as_str())
                .unwrap()
                .with_timezone(&Berlin)
                .naive_local();

        BlobEvent {
            api: event_data.api,
            content_type: event_data.content_type,
            content_length: event_data.content_length,
            blob_type: event_data.blob_type,
            blob_url: event_data.blob_url,
            blob_size_mb,
            url: event_data.url,
            event_time,
        }
    }
}
