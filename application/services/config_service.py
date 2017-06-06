import sys
import os
import inspect
import ConfigParser
import dycast

def get_env_variable(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        return None

def get_default_config():
    current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    config_file = os.path.join(current_dir, "..", "dycast.config")

    config = ConfigParser.SafeConfigParser(os.environ)

    try:
        print "Reading config file..."
        config.read(config_file)
        print "Done reading config file."
    except:
        print "could not read config file:", config_file
        sys.exit()

    return config