import os
import logging
from pathlib import Path
from utils.logging import log_event
from utils.config import ENV, BASE_DIR, LOCAL_ENV_NAME, AWS_ENV_NAME


logger = logging.getLogger(__name__)

def resolve_path(path_config, env=ENV, base_dir=BASE_DIR):
    """
    Resolve the correct dataset path based on environment.

    Args:
        path_config (dict | str): Path configuration from YAML.
            Example:
                {
                    "local": "data/raw/orders/",
                    "aws": "s3://bucket/raw/orders/"
                }
            OR a direct string path.

        env (str): Current environment ("local" or "aws").

        base_dir (str, optional): Base directory for local paths.

    Returns:
        str: Fully resolved path.

    Raises:
        ValueError: If path is missing for the given environment.
    """

    try:
        log_event(logger, "INFO", "resolving_path", env=env)

        # -----------------------------
        # Case 1: Path is environment-based (dict)
        # -----------------------------
        if isinstance(path_config, dict):
            path = path_config.get(env)

            if not path:
                raise ValueError(
                    f"No path defined for environment '{env}'. Available keys: {list(path_config.keys())}"
                )

            # -----------------------------
            # Handle local paths
            # -----------------------------
            if env == LOCAL_ENV_NAME:
                if not base_dir:
                    raise ValueError("base_dir must be provided for local environment")

                abs_path = Path(base_dir) / path

                # Convert to proper URI for Spark
                resolved_path = abs_path.resolve().as_uri()


                log_event(logger, "INFO", "local_path_resolved", path=resolved_path)

                return resolved_path

            # -----------------------------
            # Handle S3 paths 
            # -----------------------------
            log_event(logger, "INFO", "s3_path_resolved", path=path)
            return path

        # -----------------------------
        # Case 2: Path is already a string
        # -----------------------------
        log_event(logger, "INFO", "direct_path_provided", path=path_config)
        return path_config

    except Exception as e:
        log_event(logger, "ERROR", "path_resolution_failed", env=env, path_config=path_config, error=str(e))
        raise