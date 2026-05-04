import logging
import boto3
from utils.logging import log_event
import yaml
import os
from importlib import resources


from utils.config import BASE_DIR

logger = logging.getLogger(__name__)

def load_yaml(file_name):
    path = os.path.join(BASE_DIR, file_name)

    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if data is None:
            raise ValueError(f"{file_name} is empty")

        log_event(logger, "INFO", "yaml_loaded_successfully", file=file_name)
        return data

    except FileNotFoundError:
        log_event(logger, "ERROR", "yaml_file_not_found", file=file_name)
        raise

    except yaml.YAMLError as e:
        log_event(logger, "ERROR", "yaml_invalid_format", file=file_name, error=str(e))
        raise

    except Exception as e:
        log_event(logger, "ERROR", "yaml_loading_failed", file=file_name, error=str(e))
        raise

def load_yaml_from_s3(s3_path):
    """
    Load YAML config directly from S3.
    """

    try:
        s3 = boto3.client("s3")
        # ----------------------------
        # Parse S3 path
        # ----------------------------
        path = s3_path.replace("s3://", "")
        bucket, key = path.split("/", 1)

        # ----------------------------
        # Fetch file from S3
        # ----------------------------
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        # ----------------------------
        # Parse YAML
        # ----------------------------
        data = yaml.safe_load(content)

        if data is None:
            raise ValueError(f"YAML is empty: {s3_path}")

        return data

    except Exception as e:
        raise RuntimeError(f"Failed to load YAML from S3: {s3_path} | {str(e)}")