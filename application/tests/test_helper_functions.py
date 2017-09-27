import os
from application.services import config_service
from application.services import logging_service
from application.services import debug_service


debug_service.enable_debugger()

current_dir = os.path.basename(os.path.dirname(os.path.realpath(__file__)))

def init_test_environment():
    config_path = os.path.join(current_dir, '..', 'dycast.config')

    config_service.init_config(config_path)
    logging_service.init_logging()


def get_test_data_directory():
    return os.path.join(current_dir, 'test_data')

def get_test_cases_import_files_latlong():
    file_1 = os.path.join(get_test_data_directory(), 'input_cases_latlong1.tsv')
    file_2 = os.path.join(get_test_data_directory(), 'input_cases_latlong2.tsv')
    return [file_1, file_2]

def get_test_cases_import_file_geometry():
    return os.path.join(get_test_data_directory(), 'input_cases_geometry.tsv')
