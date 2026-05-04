# cleans, joins, logic
from utils.logging import configure_logging, log_event


def transform_all(dataframes):
    log_event(configure_logging(), "INFO", "transformation_started")

    try:
        for name, df in dataframes.items():
            dataframes[name] = df.dropDuplicates()
        log_event(configure_logging(), "INFO", "transformation_success")
        return dataframes
    except Exception as e:
        log_event(configure_logging(), "ERROR", "transformation_failed", error=str(e))
        raise