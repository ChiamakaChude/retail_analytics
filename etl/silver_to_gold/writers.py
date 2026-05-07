import logging
from utils.logging import log_event
from utils.path_resolver import resolve_path

logger = logging.getLogger(__name__)



def write_gold(transformed_datasets, resolved_datasets):

    try:

        log_event(logger, "INFO", "gold_write_started")

        for table_name, df in transformed_datasets.items():
            
            config = resolved_datasets[table_name]

            target_path = resolve_path(config["gold"]["path"])

            log_event(logger, "INFO", "gold_table_write_started", table=table_name, path=target_path)
            
            writer = df.write.mode(config["gold"].get("mode", "overwrite"))

            #partition_cols = config["gold"].get("partition_by")

            #if partition_cols:
            #    log_event(logger, "INFO", "gold_table_partitioning", table=table_name, columns=partition_cols)
            #    writer = writer.partitionBy(partition_cols)
            
            writer.parquet(target_path)

            log_event(logger, "INFO", "gold_write_completed", path=target_path)

    except Exception as e:
        log_event(logger, "ERROR", "gold_write_failed", error=str(e))
        raise