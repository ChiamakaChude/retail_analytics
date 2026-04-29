# csv/parquet/s3 reading logic
from csv import reader
from importlib.resources import path
import logging
from utils.path_resolver import resolve_path
from utils.path_resolver import resolve_path
from utils.logging import configure_logging, log_event
from utils.path_resolver import resolve_path



configure_logging()
logger = logging.getLogger(__name__)

def read_datasets(spark, resolved_datasets):
    dataframes = {}

    for name, config in resolved_datasets.items():
        try:
            log_event(logger, "INFO", "dataset_read_started", dataset=name)

            reader = spark.read.option("header", True)

            if config.get("schema"):
                reader = reader.schema(config["schema"])

            path = resolve_path(config["source"]["path"])
            df = reader.csv(path)

            row_count = df.count()

            log_event(logger, "INFO", "dataset_read_success", dataset=name, rows=row_count)

            dataframes[name] = df

        except Exception as e:
            log_event(logger, "ERROR", "dataset_read_failed", dataset=name, error=str(e))
            raise

    return dataframes