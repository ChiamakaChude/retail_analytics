# Read data from gold layer, load into Redshift
import os
import logging
from utils.logging import configure_logging, log_event
from utils.path_resolver import resolve_path


logger = logging.getLogger(__name__)


def read_gold_datasets(spark, resolved_datasets):
    dataframes = {}
    failed_datasets = {}

    for name, config in resolved_datasets.items():
        try:
            log_event(logger, "INFO", "dataset_read_started", dataset=name)

            reader = spark.read.format("parquet")

            path = resolve_path(config["gold"]["path"])
            logger.info(f"Reading dataset '{name}' from path: {path}")

            df = reader.load(path)

            row_count = df.count()

            log_event(logger,"INFO","dataset_read_success",dataset=name,rows=row_count)

            dataframes[name] = df

        except Exception as e:
            log_event(logger, "ERROR", "dataset_read_failed", dataset=name, error=str(e))

            failed_datasets[name] = str(e)

    if failed_datasets:
        logger.error(f"Some datasets failed: {failed_datasets}")
        raise Exception(f"Dataset ingestion failed for: {list(failed_datasets.keys())}")

    return dataframes