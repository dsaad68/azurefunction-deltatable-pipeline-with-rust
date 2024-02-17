use std::collections::HashMap;

use deltalake::DeltaTable;
use deltalake::DeltaTableError;
use deltalake::DeltaTableBuilder;
use deltalake::protocol::SaveMode;
use deltalake::operations::DeltaOps;
use deltalake::schema::Schema as DeltaSchema;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::operations::create::CreateBuilder;

pub async fn create_and_write_table(
    record_batch: &RecordBatch,
    delta_schema: DeltaSchema,
    target_table_path: &str,
    backend_config: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let new_table = CreateBuilder::new()
        .with_location(target_table_path)
        .with_storage_options(backend_config.clone())
        .with_columns(delta_schema.get_fields().clone())
        .await?;

    DeltaOps::from(new_table)
        .write(vec![record_batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .await

    // TODO: Print Table Schema
}

// CHECK: Unused
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
