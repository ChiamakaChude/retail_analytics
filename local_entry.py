# local entry point for testing and development
import os
import logging
from utils.logging import configure_logging, log_event
from etl.engine import orchestrate_pipeline
from utils.spark_factory import get_spark
from utils.config import ENV

configure_logging()
logger = logging.getLogger(__name__)

logger.info("Starting local entry point...Environment: %s", ENV)

spark = get_spark(env=ENV)

def run_pipeline():
    log_event(logger, "INFO", "pipeline_started")
    try:
        orchestrate_pipeline(spark)
        log_event(logger, "INFO", "pipeline_completed_successfully")
    except Exception as e:
        log_event(logger, "ERROR", "pipeline_failed", error=str(e))


if __name__ == "__main__":
    run_pipeline()