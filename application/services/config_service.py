import sys
import os
import ConfigParser
import logging

CONFIG = ConfigParser.SafeConfigParser(os.environ)

def get_env_variable(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        return None

def init_config(config_file_path):
    try:
        logging.debug("Reading config file...")
        CONFIG.read(config_file_path)
        logging.debug("Done reading config file.")
    except Exception:
        logging.exception("Could not read config file: %s", config_file_path)
        sys.exit()

def get_config():
    return CONFIG
