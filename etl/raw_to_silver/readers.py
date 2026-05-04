# csv/parquet/s3 reading logic
from csv import reader
from importlib.resources import path
import logging
from utils.path_resolver import resolve_path
from utils.path_resolver import resolve_path
from utils.logging import configure_logging, log_event
from utils.path_resolver import resolve_path



logger = logging.getLogger(__name__)

def read_datasets(spark, resolved_datasets):
    dataframes = {}
    failed_datasets = {}

    for name, config in resolved_datasets.items():
        try:
            log_event(logger, "INFO", "dataset_read_started", dataset=name)

            reader = spark.read.format("csv").option("header", True)

            # apply schema if exists
            if config.get("schema"):
                reader = reader.schema(config["schema"])

            path = resolve_path(config["source"]["path"])
            logger.info(f"Reading dataset '{name}' from path: {path}")

            df = reader.load(path)

            row_count = df.count()

            log_event(
                logger,
                "INFO",
                "dataset_read_success",
                dataset=name,
                rows=row_count
            )

            dataframes[name] = df

        except Exception as e:
            log_event(
                logger,
                "ERROR",
                "dataset_read_failed",
                dataset=name,
                error=str(e)
            )

            # store failure but DO NOT crash pipeline immediately
            failed_datasets[name] = str(e)

    # final decision point
    if failed_datasets:
        logger.error(f"Some datasets failed: {failed_datasets}")
        raise Exception(f"Dataset ingestion failed for: {list(failed_datasets.keys())}")

    return dataframes