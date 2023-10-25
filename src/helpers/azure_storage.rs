use std::sync::Arc;

use bytes::Bytes;

use crate::BlobPath;

use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;

#[allow(unused)]
pub fn get_azure_store(container_name: &str) -> Arc<dyn ObjectStore> {
    let azure_store = MicrosoftAzureBuilder::from_env()
        .with_container_name(container_name)
        .build()
        .unwrap();

    Arc::new(azure_store)
}

#[allow(unused)]
pub async fn fetch_file(azure_store: Arc<dyn ObjectStore>, blob_path: BlobPath) -> Bytes {
    let path: Path = blob_path.complete_path.try_into().unwrap();

    azure_store.get(&path).await.unwrap().bytes().await.unwrap()
}
