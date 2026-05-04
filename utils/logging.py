import logging
from datetime import datetime
import json

def configure_logging(level=logging.INFO):
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # only add handler if none exist (avoids duplicate logs)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def log_event(logger, level, event, **kwargs):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event,
        **kwargs
    }

    message = json.dumps(log_entry)

    if level == "INFO":
        logger.info(message)
    elif level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.debug(message)