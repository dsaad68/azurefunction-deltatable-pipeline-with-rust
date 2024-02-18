use bytes::Bytes;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use object_store::azure::MicrosoftAzureBuilder;

use crate::BlobPath;

pub fn get_azure_store(container_name: &str) -> Arc<dyn ObjectStore> {
    let azure_store = MicrosoftAzureBuilder::from_env()
        .with_container_name(container_name)
        .build()
        .unwrap();

    Arc::new(azure_store)
}

pub async fn fetch_file(azure_store: Arc<dyn ObjectStore>, blob_path: BlobPath) -> Bytes {
    let path: Path = blob_path.complete_path.into();

    azure_store.get(&path).await.unwrap().bytes().await.unwrap()
}
