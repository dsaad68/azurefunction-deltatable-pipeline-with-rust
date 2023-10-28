

use log::info;

use deltalake::DeltaTable;
#[allow(unused_imports)]
use deltalake::DeltaTableError;
use deltalake::operations::DeltaOps;
use deltalake::datafusion::dataframe::DataFrame;

use deltalake::datafusion::logical_expr::col;

#[allow(unused)]
pub async fn user_list_merge(target_table: DeltaTable, source_df: DataFrame) {

    let (table, metrics) = DeltaOps(target_table)
                .merge(source_df, col("username").eq(col("source.username")))
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

    info!("Table Version: {:?}",table.version());
    info!("Number of files: {:?}",table.get_file_uris().count());
    info!("Number of source rows: {:?}",metrics.num_source_rows);
    info!("Number of rows insereted in target table: {:?}",metrics.num_target_rows_inserted);
    info!("Number of rows updated in target table: {:?}",metrics.num_target_rows_updated);
    info!("Number of rows deleted in target table: {:?}",metrics.num_target_rows_deleted);
    info!("Number of rows copied in target table: {:?}",metrics.num_target_rows_copied);
    info!("Number of rows output rows: {:?}",metrics.num_output_rows);
    info!("Number of rows files added to target table: {:?}",metrics.num_target_files_added);
    info!("Number of rows files added from target table: {:?}",metrics.num_target_files_removed);
    info!("Execution time in ms: {:?}",metrics.execution_time_ms);
    info!("Scan time in ms: {:?}",metrics.scan_time_ms);
    info!("Rewrite time in ms: {:?}",metrics.rewrite_time_ms);

}