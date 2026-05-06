import os
import logging
import psycopg2

from utils.logging import log_event

logger = logging.getLogger(__name__)

def connect_to_redshift()-> psycopg2.extensions.connection:
    """Establishes a connection to the Redshift cluster using credentials from environment variables."""
    try:
        log_event(logger, "INFO", "checking_redshift_credentials", user=os.getenv("REDSHIFT_USER"))
        log_event(logger, "INFO", "attempting_redshift_connection")
        conn = psycopg2.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT"),
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )

        log_event(logger, "INFO", "redshift_connection_successful")
        return conn
    
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {e}")
        raise