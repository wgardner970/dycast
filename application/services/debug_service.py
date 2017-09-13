import os
import logging
import config_service

def enable_debugger():
    if config_service.get_env_variable("DEBUG") == "True":
        import ptvsd
        secret = config_service.get_env_variable("REMOTE_DEBUG_SECRET")
        ptvsd.enable_attach(secret=secret, address=('0.0.0.0', 3000))

        if config_service.get_env_variable("REMOTE_DEBUG") == "True":
            logging.info("Waiting for debugger to attach...")
            ptvsd.wait_for_attach()
