# local entry point for testing and development
import os
from dotenv import load_dotenv
load_dotenv()  # loads .env into os.environ

import argparse
import logging
from utils.logging import configure_logging, log_event
from utils.spark_factory import get_spark
from utils.config import ENV, RAW_TO_SILVER_JOB_NAME, SILVER_TO_GOLD_JOB_NAME, GOLD_TO_REDSHIFT_JOB_NAME
from jobs.raw_to_silver import run_raw_to_silver_pipeline
from jobs.silver_to_gold import run_silver_to_gold_pipeline


os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

configure_logging()
logger = logging.getLogger(__name__)

logger.info("Starting local entry point...Environment: %s", ENV)

spark = get_spark(env=ENV)


try:

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    args = parser.parse_args()

    job = args.job

    log_event(logger, "INFO", "selected_job", job=job)

    if job == RAW_TO_SILVER_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=RAW_TO_SILVER_JOB_NAME)
        run_raw_to_silver_pipeline(spark, job)

    elif job == SILVER_TO_GOLD_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=SILVER_TO_GOLD_JOB_NAME)
        run_silver_to_gold_pipeline(spark, job)

    elif job == GOLD_TO_REDSHIFT_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=GOLD_TO_REDSHIFT_JOB_NAME)

    else:
        log_event(logger, "ERROR", "unknown_job_selected", job=job)
        raise ValueError(f"Unknown job name: {job}") 

except Exception as e:
    log_event(logger, "ERROR", "pipeline_failed", error=str(e))
    raise