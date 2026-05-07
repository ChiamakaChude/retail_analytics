import psycopg2
import logging
from utils.logging import log_event
from utils.config import TYPE_MAPPING_FOR_REDSHIFT

logger = logging.getLogger(__name__)

def table_exists(cursor, table_name:str, schema:str="public") -> bool:
    """
    Check if a table exists in Redshift.

    Args:
        cursor: Redshift DB cursor
        table_name (str): Name of the table
        schema (str): Schema name (default: public)

    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        log_event(logger, "INFO", f"checking_table_exists: {schema}.{table_name}")

        query = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
            LIMIT 1;
        """

        cursor.execute(query)
        exists = cursor.fetchone() is not None

        log_event(
            logger,
            "INFO",
            f"table_exists_result: {schema}.{table_name} -> {exists}"
        )

        return exists

    except Exception as e:
        log_event(
            logger,
            "ERROR",
            f"error_checking_table_exists: {schema}.{table_name} -> {e}"
        )
        raise

def build_create_table_sql(table_name:str, spark_schema, model_config, schema="public"):
    """
    Generate CREATE TABLE SQL from PySpark StructType schema.

    Args:
        table_name (str): Target table name
        spark_schema (StructType): PySpark schema
        model_config (dict): Model config (dist_key, sort_key)
        schema (str): Redshift schema

    Returns:
        str: CREATE TABLE SQL statement
    """
    try:
        log_event(logger, "INFO", f"building_create_table_sql: {schema}.{table_name}")

        columns_sql = []

        for field in spark_schema.fields:
            col_name = field.name
            spark_type = type(field.dataType).__name__
            redshift_type = TYPE_MAPPING_FOR_REDSHIFT.get(spark_type)
            log_event(logger, "INFO", f"column_type_mapping", column=col_name, spark_type=spark_type, redshift_type=redshift_type)

            nullable = "" if field.nullable else "NOT NULL"

            columns_sql.append(f"{col_name} {redshift_type} {nullable}")

        columns_str = ",\n".join(columns_sql)

        dist_key = model_config.get("dist_key")
        sort_key = model_config.get("sort_key")

        dist_sql = f"DISTKEY({dist_key})" if dist_key else ""
        sort_sql = f"SORTKEY({sort_key})" if sort_key else ""

        create_sql = f"""
            CREATE TABLE {schema}.{table_name} (
                {columns_str}
            )
            {dist_sql}
            {sort_sql};
        """

        log_event(
            logger,
            "INFO",
            f"create_table_sql_built: {schema}.{table_name}"
        )

        return create_sql

    except Exception as e:
        log_event(
            logger,
            "ERROR",
            f"error_building_create_table_sql: {schema}.{table_name} -> {e}"
        )
        raise

def ensure_table_exists(cursor, table_name, spark_schema, model_config, schema="public"):
    """
    Ensure a table exists in Redshift. If not, create it.

    Args:
        cursor: Redshift DB cursor
        table_name (str): Target table
        spark_schema (StructType): Schema definition
        model_config (dict): Model configuration
        schema (str): Redshift schema
    """
    try:
        log_event(logger, "INFO", f"ensuring_table_exists: {schema}.{table_name}")

        if not table_exists(cursor, table_name, schema):
            log_event(
                logger,
                "INFO",
                f"table_not_found_creating: {schema}.{table_name}"
            )

            create_sql = build_create_table_sql(
                table_name,
                spark_schema,
                model_config,
                schema
            )

            cursor.execute(create_sql)

            log_event(
                logger,
                "INFO",
                f"table_created: {schema}.{table_name}"
            )
        else:
            log_event(
                logger,
                "INFO",
                f"table_already_exists: {schema}.{table_name}"
            )

    except Exception as e:
        log_event(
            logger,
            "ERROR",
            f"error_ensuring_table_exists: {schema}.{table_name} -> {e}"
        )
        raise