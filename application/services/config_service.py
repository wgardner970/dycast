import sys
import os
import inspect
import ConfigParser
from application import dycast

def get_config():
    test_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    config_file = os.path.join(test_dir, "..", "dycast.config")

    config = ConfigParser.SafeConfigParser()

    try:
        config.read(config_file)
        dycast.read_config(config_file)
    except:
        print "could not read config file:", config_file
        sys.exit()

    return config