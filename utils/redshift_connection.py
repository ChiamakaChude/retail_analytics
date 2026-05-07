import os
import logging
import psycopg2

from utils.logging import log_event
from utils.config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

logger = logging.getLogger(__name__)

def connect_to_redshift()-> psycopg2.extensions.connection:
    """Establishes a connection to the Redshift cluster using credentials from environment variables."""
    try:
        log_event(logger, "INFO", "checking_redshift_credentials", user=REDSHIFT_USER)
        log_event(logger, "INFO", "attempting_redshift_connection")
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )

        log_event(logger, "INFO", "redshift_connection_successful")
        return conn
    
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {e}")
        raise