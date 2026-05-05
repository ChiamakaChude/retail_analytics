# Raw to Silver pipeline entry point

import sys
import logging

from utils.logging import configure_logging, log_event
from etl.raw_to_silver.engine import orchestrate_raw_to_silver_pipeline


# -------------------
# Logging
# -------------------
configure_logging()
logger = logging.getLogger(__name__)


# -------------------
# Pipeline execution
# -------------------
def run_raw_to_silver_pipeline(spark, job_name):

    log_event(logger, "INFO", "Raw to silver pipeline started", job=job_name)

    try:
        orchestrate_raw_to_silver_pipeline(spark, job_name)

        log_event(logger, "INFO", "Raw to silver pipeline completed successfully", job=job_name)

    except Exception as e:
        log_event(logger, "ERROR", "Raw to silver pipeline failed", error=str(e), job=job_name)
        raise