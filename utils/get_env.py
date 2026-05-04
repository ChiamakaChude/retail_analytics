import os   
import logging
from utils.logging import log_event

logger = logging.getLogger(__name__)

def get_env():
    # Try Glue job params first
    try:
        from awsglue.utils import getResolvedOptions
        import sys
        args = getResolvedOptions(sys.argv, ['env'])
        log_event(logger, "INFO", "env_resolved_from_glue", env=args['env'])
        return args['env']
    except Exception:
        # Fall back to .env / os.environ for local/test
        log_event(logger, "INFO", "env_resolved_from_os", env=os.getenv('ENV', 'local'))
        return os.getenv('ENV', 'local')