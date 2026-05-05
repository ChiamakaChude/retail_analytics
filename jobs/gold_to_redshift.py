# Raw to Silver pipeline entry point

import sys
import logging

from utils.logging import configure_logging, log_event
#from etl.raw_to_silver.engine import orchestrate_raw_to_silver_pipeline


# -------------------
# Logging
# -------------------
configure_logging()
logger = logging.getLogger(__name__)


# -------------------
# Pipeline execution
# -------------------
def run_gold_to_redshift_pipeline(spark, job_name):

    log_event(logger, "INFO", "Gold to Redshift pipeline started", job=job_name)

    try:
        #orchestrate_raw_to_silver_pipeline(spark, job_name)

        log_event(logger, "INFO", "Gold to Redshift pipeline completed successfully", job=job_name)

    except Exception as e:
        log_event(logger, "ERROR", "Gold to Redshift pipeline failed", error=str(e), job=job_name)
        raise