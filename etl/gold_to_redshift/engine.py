# orchestrates pipeline
import logging
from utils.logging import configure_logging, log_event
from utils.schema_resolver import load_model_config, load_dataset_config
from etl.gold_to_redshift.readers import read_gold_datasets
from etl.gold_to_redshift.loaders import load_gold_to_redshift



configure_logging()
logger = logging.getLogger(__name__)


def orchestrate_gold_to_redshift_pipeline(spark, job_name):

    log_event(logger, "INFO", "pipeline_started", job=job_name)
    
    resolved_datasets = load_dataset_config()
    resolved_models = load_model_config()

    datasets = read_gold_datasets(spark, resolved_datasets)

    load_gold_to_redshift(resolved_datasets, resolved_models)
