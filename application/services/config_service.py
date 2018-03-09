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
        logging.debug("Reading config file: %s", config_file_path)
        CONFIG.read(config_file_path)
        logging.debug("Done reading config file.")
    except Exception:
        logging.exception("Could not read config file: %s", config_file_path)
        sys.exit()

def get_config():
    return CONFIG

def get_root_directory():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(current_dir, '..', '..')

def get_import_directory():
    import_directory = CONFIG.get("system", "import_directory")
    root_directory = get_root_directory()
    return os.path.join(root_directory, import_directory)

def get_export_directory():
    export_directory = CONFIG.get("system", "export_directory")
    root_directory = get_root_directory()
    return os.path.join(root_directory, export_directory)