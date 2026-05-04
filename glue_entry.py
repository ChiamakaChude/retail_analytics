# AWS Glue entry point

import sys
import logging

from utils.logging import configure_logging, log_event
from etl.engine import orchestrate_pipeline
from utils.config import ENV
from utils.spark_factory import get_spark

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# -------------------
# Glue setup
# -------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


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
# Pipeline execution
# -------------------
def run_pipeline():
    log_event(logger, "INFO", "pipeline_started")

    try:
        orchestrate_pipeline(spark)

        log_event(logger, "INFO", "pipeline_completed_successfully")

    except Exception as e:
        log_event(logger, "ERROR", "pipeline_failed", error=str(e))
        raise


# -------------------
# MAIN EXECUTION
# -------------------
run_pipeline()

job.commit()