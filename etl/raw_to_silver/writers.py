# s3 writers (partitioning, formats, etc.)
from csv import reader
from importlib.resources import path
import logging
from utils.path_resolver import resolve_path
from utils.path_resolver import resolve_path
from utils.logging import configure_logging, log_event
from utils.path_resolver import resolve_path
from utils.config import ENV

logger = logging.getLogger(__name__)


def write_datasets(dataframes, resolved_datasets):
    for name, df in dataframes.items():

        try:
            config = resolved_datasets[name]

            log_event(logger, "INFO", "dataset_write_started", dataset=name)

            # ------------------------
            # Resolve target path
            # ------------------------
            target_path = resolve_path(config["target"]["path"])
            
            logger.info(f"Writing dataset '{name}' to: {target_path}")

            writer = df.write.mode(config["target"].get("mode", "overwrite"))

            # ------------------------
            # Partitioning (if exists)
            # ------------------------
            partition_cols = config["target"].get("partition_by")

            if partition_cols:
                writer = writer.partitionBy(partition_cols)

            # ------------------------
            # Write to parquet
            # ------------------------
            writer.parquet(target_path)

            log_event(
                logger,
                "INFO",
                "dataset_write_success",
                dataset=name,
                path=target_path
            )

        except Exception as e:
            log_event(
                logger,
                "ERROR",
                "dataset_write_failed",
                dataset=name,
                error=str(e)
            )
            raise