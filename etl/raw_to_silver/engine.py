# orchestrates pipeline
import logging
from utils.logging import configure_logging, log_event
from utils.schema_resolver import load_dataset_config
from etl.readers import read_datasets
from etl.transforms import transform_all
from etl.writers import write_datasets



configure_logging()
logger = logging.getLogger(__name__)


def orchestrate_pipeline(spark):
    log_event(logger, "INFO", "pipeline_started")
    resolved_datasets = load_dataset_config()

    dataframes = read_datasets(spark, resolved_datasets)

    transformed_dataframes = transform_all(dataframes)

    write_datasets(transformed_dataframes, resolved_datasets)