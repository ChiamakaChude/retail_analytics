import os
import logging
from utils.logging import log_event
from utils.config import ENV, AWS_ENV_NAME

logger = logging.getLogger(__name__)

def get_spark(env=ENV):
    try:
        if env == AWS_ENV_NAME:
            log_event(logger, "INFO", "creating_glue_spark_session")

            from awsglue.context import GlueContext
            from pyspark.context import SparkContext

            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            return glueContext.spark_session

        else:
            log_event(logger, "INFO", "creating_local_spark_session")
            from pyspark.sql import SparkSession

            return (
                SparkSession.builder
                .appName("retail-analytics-local")
                .master("local[*]")
                .getOrCreate()
            )
    except Exception as e:
        log_event(logger, "ERROR", "spark_session_creation_failed", env=env, error=str(e))
        raise