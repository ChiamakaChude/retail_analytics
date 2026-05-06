import os
import logging
from utils.redshift_connection import connect_to_redshift
from utils.logging import log_event
from gold_to_redshift.validators import ensure_table_exists

logger = logging.getLogger(__name__)

def load_gold_to_redshift(resolved_datasets, resolved_models):
    """Loads transformed datasets into Redshift using staging + COPY."""

    log_event(logger, "INFO", "starting_redshift_load")

    conn = connect_to_redshift()
    cursor = conn.cursor()

    for model_name, model_config in resolved_models.items():
        table_name = model_config["table_name"]
        staging_table = model_config["staging_table_name"]
        dataset_name = model_config["dataset"]
        spark_schema = model_config["schema"]

        s3_path = resolved_datasets[dataset_name]["gold"]["path"]["aws"]

        try:
            # -----------------------------
            # 1. Ensure FINAL table exists
            # -----------------------------
            ensure_table_exists(cursor, table_name, spark_schema, model_config)

            # -----------------------------
            # 2. Reset STAGING table
            # -----------------------------
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")

            cursor.execute(f"""
                CREATE TABLE {staging_table}
                (LIKE {table_name});
            """)

            # -----------------------------
            # 3. COPY into STAGING
            # -----------------------------
            copy_query = f"""
                COPY {staging_table}
                FROM '{s3_path}'
                IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
                FORMAT AS PARQUET;
            """

            cursor.execute(copy_query)

            # -----------------------------
            # 4. (Next step) UPSERT
            # -----------------------------
            # Placeholder for now — we’ll implement properly next
            cursor.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {staging_table};
            """)

            conn.commit()

            log_event(logger, "INFO", f"{model_name} loaded successfully")

        except Exception as e:
            conn.rollback()
            log_event(logger, "ERROR", f"{model_name} failed: {e}")

    cursor.close()
    conn.close()

    log_event(logger, "INFO", "completed_redshift_load")