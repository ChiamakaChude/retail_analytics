# Silver to Gold pipeline entry point

import sys
import logging

from utils.logging import configure_logging, log_event
from etl.silver_to_gold.engine import orchestrate_silver_to_gold_pipeline


# -------------------
# Logging
# -------------------
configure_logging()
logger = logging.getLogger(__name__)


# -------------------
# Pipeline execution
# -------------------
def run_silver_to_gold_pipeline(spark, job_name):

    log_event(logger, "INFO", "Silver to gold pipeline started", job=job_name)

    try:
        orchestrate_silver_to_gold_pipeline(spark, job_name)

        log_event(logger, "INFO", "Silver to gold pipeline completed successfully", job=job_name)

    except Exception as e:
        log_event(logger, "ERROR", "Silver to gold pipeline failed", error=str(e), job=job_name)
        raise