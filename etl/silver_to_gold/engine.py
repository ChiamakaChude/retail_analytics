# orchestrates pipeline
import logging
from utils.logging import configure_logging, log_event
from utils.schema_resolver import load_dataset_config
from etl.silver_to_gold.readers import read_silver_datasets
from etl.silver_to_gold.transformer import build_orders_gold
from etl.silver_to_gold.writers import write_gold



configure_logging()
logger = logging.getLogger(__name__)


def orchestrate_silver_to_gold_pipeline(spark, job_name):

    log_event(logger, "INFO", "pipeline_started", job=job_name)

    resolved_datasets = load_dataset_config()

    datasets = read_silver_datasets(spark, resolved_datasets)

    transformed_datasets = build_orders_gold(datasets)

    write_gold(transformed_datasets, resolved_datasets)

