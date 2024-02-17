use log::info;

use deltalake::DeltaTable;
#[allow(unused_imports)]
use deltalake::DeltaTableError;
use deltalake::operations::DeltaOps;
use deltalake::datafusion::logical_expr::col;
use deltalake::datafusion::dataframe::DataFrame;


pub async fn user_list_merge(target_table: DeltaTable, source_df: DataFrame) {

    let (table, metrics) = DeltaOps(target_table)
                .merge(source_df, col("username").eq(col("source.username")))
                .with_source_alias("source")
                .with_target_alias("target")
                .when_matched_update(|update| {
                    update
                        .update("target.account_type", col("source.account_type"))
                        .update("target.payment_method", col("source.payment_method"))
                        .update("target.credit_card_type", col("source.credit_card_type"))
                        .update("target.payment_id", col("source.payment_id"))
                })
                .unwrap()
                .when_not_matched_insert(|insert| {
                    insert
                        .set("target.username", col("source.username"))
                        .set("target.email", col("source.email"))
                        .set("target.account_type", col("source.account_type"))
                        .set("target.payment_method", col("source.payment_method"))
                        .set("target.credit_card_type", col("source.credit_card_type"))
                        .set("target.payment_id", col("source.payment_id"))
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