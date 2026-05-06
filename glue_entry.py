# AWS Glue entry point

import sys
import logging

from utils.logging import configure_logging, log_event
from utils.config import ENV, RAW_TO_SILVER_JOB_NAME, SILVER_TO_GOLD_JOB_NAME, GOLD_TO_REDSHIFT_JOB_NAME
from utils.spark_factory import get_spark

from jobs.raw_to_silver import run_raw_to_silver_pipeline
from jobs.silver_to_gold import run_silver_to_gold_pipeline
from jobs.gold_to_redshift import run_gold_to_redshift_pipeline

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# -------------------
# Glue setup
# -------------------
args = getResolvedOptions(sys.argv, ['job_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['job_name'], args)

job_name = args['job_name']


# -------------------
# Logging
# -------------------
configure_logging()
logger = logging.getLogger(__name__)

# -------------------
# Spark selection (Glue vs Local)
# -------------------
spark = get_spark(env=ENV)


# -------------------
# Pipeline route selection based on job name
# -------------------


try:

    log_event(logger, "INFO", "selected_job", job=job_name)
    
    if job_name == RAW_TO_SILVER_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=RAW_TO_SILVER_JOB_NAME)
        run_raw_to_silver_pipeline(spark, job_name)

    elif job_name == SILVER_TO_GOLD_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=SILVER_TO_GOLD_JOB_NAME)
        run_silver_to_gold_pipeline(spark, job_name)

    elif job_name == GOLD_TO_REDSHIFT_JOB_NAME:
        log_event(logger, "INFO", "selected_job", job=GOLD_TO_REDSHIFT_JOB_NAME)
        run_gold_to_redshift_pipeline(spark, job_name)

    else:
        log_event(logger, "ERROR", "unknown_job_selected", job=job_name)
        raise ValueError(f"Unknown job name: {job_name}") 

except Exception as e:
    log_event(logger, "ERROR", "pipeline_failed", error=str(e))
    raise

job.commit()