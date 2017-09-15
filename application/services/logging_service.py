import logging
import os
import sys
import ast
import config_service



def init_logging():
    root_logger = logging.getLogger()
    log_format = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    log_level = get_log_level()
    root_logger.setLevel(log_level)

    logging.basicConfig(level=log_level, format=log_format)

    log_formatter = logging.Formatter(log_format)
    file_handler = logging.FileHandler(get_log_file_path())
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)


def show_current_parameter_set():
    config = config_service.get_config()
    logging.info("")
    logging.info("Using parameter set:")
    logging.info("Spatial domain: %s", config.get("dycast", "spatial_domain"))
    logging.info("Temporal domain: %s", config.get("dycast", "temporal_domain"))
    logging.info("Close in space: %s", config.get("dycast", "close_in_space"))
    logging.info("Close in time: %s", config.get("dycast", "close_in_time"))
    logging.info("")



# 'Private' methods

def get_log_level():
    debug_setting = config_service.get_env_variable("DEBUG")
    if ast.literal_eval(debug_setting):
        return logging.DEBUG
    else:
        return logging.INFO

def get_log_file_path():
    config = config_service.get_config()
    return os.path.join(config.get("system", "unix_dycast_path"), config.get("system", "logfile"))
