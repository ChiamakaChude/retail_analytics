import logging
from utils.logging import log_event
from utils.path_resolver import resolve_path

logger = logging.getLogger(__name__)



def write_gold(df, resolved_datasets):
    path = resolve_path(resolved_datasets["gold"]["path"])

    log_event(logger, "INFO", "gold_write_started", path=path)

    df.write \
        .mode("overwrite") \
        .parquet(path)

    log_event(logger, "INFO", "gold_write_completed", path=path)