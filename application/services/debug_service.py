import os
import logging
import time
from application.services import config_service


def enable_debugger():
    if config_service.get_env_variable("DEBUG") == "True":
        import ptvsd
        secret = config_service.get_env_variable("REMOTE_DEBUG_SECRET")
        
        try:
            ptvsd.enable_attach(secret=secret, address=('0.0.0.0', 3000))
            print "Debugger is ready for attachment."
        except:
            pass


        if config_service.get_env_variable("WAIT_FOR_ATTACH") == "True":
            print "Waiting 10 seconds for debugger to attach..."
            time.sleep(10)
            if ptvsd.is_attached:
                print "Attached debugger"
            else:
                print "Not attached"
