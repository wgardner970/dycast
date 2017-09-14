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
        logging.info("Reading config file...")
        config.read(config_file)
        logging.info("Done reading config file.")
    except:
        logging.info("Could not read config file: {0}".format(config_file))
        sys.exit()

    return config