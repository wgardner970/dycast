import logging
import os
import sys
import config_service



def init_logging():
    root_logger = logging.getLogger()

    # Remove default root log handler, as overwriting it  with below StreamHandler(sys.stdout) was giving trouble
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    log_level = get_log_level()
    root_logger.setLevel(log_level)

    log_formatter = logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')

    file_handler = logging.FileHandler(get_log_file_path())
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)


def get_log_level():
    debug = config_service.get_env_variable("DEBUG")
    if debug:
        return logging.DEBUG
    else:
        return logging.INFO

def get_log_file_path():
    config = config_service.get_config()
    return os.path.join(config.get("system", "unix_dycast_path"), config.get("system", "logfile"))
