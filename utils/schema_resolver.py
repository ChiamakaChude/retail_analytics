import logging
import yaml
import os
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from utils.config import TYPE_MAPPING, BASE_DIR, DATASETS_CONFIG_PATH, SCHEMAS_CONFIG_PATH, ENV, AWS_ENV_NAME, LOCAL_ENV_NAME,DATASETS_CONFIG_PATH_S3, SCHEMAS_CONFIG_PATH_S3    
from utils.load_yaml import load_yaml, load_yaml_from_s3
from utils.logging import log_event

logger = logging.getLogger(__name__)


def build_schema(schema_def: dict, schema_name: str="unknown") -> StructType:
    try:
        fields = []

        for col in schema_def.get("columns", []):
            col_name = col.get("name")
            col_type = col.get("type")

            if col_type not in TYPE_MAPPING:
                log_event(
                    logger,
                    "ERROR",
                    "unsupported_schema_type",
                    schema=schema_name,
                    column=col_name,
                    type=col_type
                )
                raise ValueError(
                    f"Unsupported type '{col_type}' in schema '{schema_name}'"
                )

            fields.append(
                StructField(
                    col_name,
                    TYPE_MAPPING[col_type],
                    col.get("nullable", True)
                )
            )

        log_event(
            logger,
            "INFO",
            "schema_built_successfully",
            schema=schema_name,
            column_count=len(fields)
        )

        return StructType(fields)

    except Exception as e:
        log_event(
            logger,
            "ERROR",
            "schema_build_failed",
            schema=schema_name,
            error=str(e)
        )
        raise

def resolve_datasets(datasets_config: dict, schemas_config: dict) -> dict:
    resolved = {}

    datasets = datasets_config.get("datasets", {})

    for name, config in datasets.items():
        try:
            log_event(
                logger,
                "INFO",
                "dataset_resolution_started",
                dataset=name
            )

            schema_name = config.get("schema")
            schema = None

            if schema_name:
                schema_def = schemas_config.get("schemas", {}).get(schema_name)

                if not schema_def:
                    log_event(
                        logger,
                        "ERROR",
                        "schema_not_found",
                        dataset=name,
                        schema=schema_name
                    )
                    raise ValueError(
                        f"Schema '{schema_name}' not found for dataset '{name}'"
                    )

                schema = build_schema(schema_def, schema_name)

            resolved[name] = {
                **config,
                "schema": schema
            }

            log_event(
                logger,
                "INFO",
                "dataset_resolved_successfully",
                dataset=name,
                schema=schema_name
            )

        except Exception as e:
            log_event(
                logger,
                "ERROR",
                "dataset_resolution_failed",
                dataset=name,
                error=str(e)
            )
            raise  # fail fast

    return resolved

def load_dataset_config()-> dict:
    try:

        log_event(logger, "INFO", "config_loading_started")

        if ENV == LOCAL_ENV_NAME:

            log_event(logger, "INFO", "loading_yaml", file="datasets_config")
            datasets_config = load_yaml(DATASETS_CONFIG_PATH)

            log_event(logger, "INFO", "loading_yaml", file="schemas_config")
            schemas_config = load_yaml(SCHEMAS_CONFIG_PATH)

            log_event(logger, "INFO", "config_loaded_successfully")

            resolved = resolve_datasets(datasets_config, schemas_config)

            log_event(
                logger,
                "INFO",
                "dataset_config_resolved",
                dataset_count=len(resolved)
            )

            return resolved
        
        else:
            log_event(logger, "INFO", "loading_yaml_from_s3", file="datasets_config")
            datasets_config = load_yaml_from_s3(DATASETS_CONFIG_PATH_S3)

            log_event(logger, "INFO", "loading_yaml_from_s3", file="schemas_config")
            schemas_config = load_yaml_from_s3(SCHEMAS_CONFIG_PATH_S3)

            log_event(logger, "INFO", "config_loaded_successfully")

            resolved = resolve_datasets(datasets_config, schemas_config)

            log_event(
                logger,
                "INFO",
                "dataset_config_resolved",
                dataset_count=len(resolved)
            )

            return resolved

    except Exception as e:
        log_event(
            logger,
            "ERROR",
            "config_loading_failed",
            error=str(e)
        )
        raise