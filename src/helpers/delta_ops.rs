use deltalake::storage::DeltaObjectStore;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;
use url::Url;
#[allow(unused)]
pub fn get_delta_store(container_name: &str, output_url: &str) -> Arc<DeltaObjectStore> {
    let azure_store = MicrosoftAzureBuilder::from_env()
        .with_container_name(container_name)
        .with_url(output_url)
        .build()
        .unwrap();

    let azure_store = Arc::new(azure_store);

    let output_url = Url::parse(output_url)
        .map_err(|e| format!("Failed to parse URL: {}", e))
        .unwrap();
    Arc::new(DeltaObjectStore::new(azure_store, output_url))
}
