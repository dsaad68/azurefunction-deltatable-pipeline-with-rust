use object_store::azure::MicrosoftAzureBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use deltalake::operations::create::CreateBuilder;
use deltalake::schema::Schema as DeltaSchema;
use deltalake::storage::DeltaObjectStore;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use deltalake::DeltaTableError;

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

#[allow(unused)]
pub async fn try_get_delta_table(
    target_table_path: &str,
    backend_config: HashMap<String, String>,
    delta_schema: DeltaSchema,
) -> Result<DeltaTable, DeltaTableError> {
    match DeltaTableBuilder::from_uri(target_table_path)
        .with_storage_options(backend_config.clone())
        .load()
        .await
    {
        Ok(table) => Ok(table),
        Err(DeltaTableError::NotATable(e)) => {
            println!("{}", e);
            CreateBuilder::new()
                .with_location(target_table_path)
                .with_storage_options(backend_config.clone())
                .with_columns(delta_schema.get_fields().clone())
                .await
        }
        Err(e) => {
            println!("{}", e);
            Err(e)
        }
    }
}
