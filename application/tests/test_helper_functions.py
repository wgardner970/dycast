import os
from application.services import config_service
from application.services import logging_service


def init_test_environment():
    current_dir = os.path.basename(os.path.dirname(os.path.realpath(__file__)))
    config_path = os.path.join(current_dir, '..', 'dycast.config')

    config_service.init_config(config_path)
    logging_service.init_logging()
