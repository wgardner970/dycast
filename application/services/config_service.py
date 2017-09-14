import sys
import os
import inspect
import ConfigParser
import logging

def get_env_variable(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        return None

def get_config():
    current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    config_file = os.path.join(current_dir, "..", "dycast.config")

    config = ConfigParser.SafeConfigParser(os.environ)

    try:
        logging.debug("Reading config file...")
        config.read(config_file)
        logging.debug("Done reading config file.")
    except Exception, e:
        logging.error("Could not read config file: %s", config_file)
        logging.error(e)
        sys.exit()

    return config