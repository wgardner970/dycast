# Dycast functions

# dist_margs means "distribution marginals" and is the result of the
# monte carlo simulations.  See Theophilides et al. for more information

import sys
import os
import inspect
import logging
import datetime

from services import debug_service
from services import logging_service
from services import config_service
from services import grid_service
from services import conversion_service
from services import file_service
from services import database_service
from services import import_service
# from services import export_service
from models.enums import enums


APPLICATION_ROOT = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
lib_dir = os.path.join(APPLICATION_ROOT, 'libs')
sys.path.append(lib_dir)
sys.path.append(os.path.join(lib_dir, "psycopg2"))
sys.path.append(os.path.join(lib_dir, "dbfpy"))


debug_service.enable_debugger()


CONFIG = config_service.get_config()


class Dycast(object):

    def __init__(self, **kwargs):
        self.cur = None
        self.conn = None
        self.case_table_name = None

        self.srid_of_cases = None
        self.dead_birds_dir = None
        self.files_to_import = None

        self.risk_file_dir = None

        self.spatial_domain = None
        self.temporal_domain = None
        self.close_in_space = None
        self.close_in_time = None
        self.case_threshold = None

        self.startdate = None
        self.enddate = None

        self.extent_min_x = None
        self.extent_min_y = None
        self.extent_max_x = None
        self.extent_max_y = None
        self.srid_of_extent = None

        self.tmp_daily_case_table = None
        self.tmp_cluster_per_point_selection_table = None

        for (key, value) in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, value)


    def import_cases(self):
        if self.files_to_import:
            logging.info("Loading files: %s", self.files_to_import)
            import_service.load_case_files(self)
        else:
            logging.info("Loading files from import path: %s",
                         self.dead_birds_dir)
            raise NotImplementedError

        logging.info("Done loading cases")

    def listen_for_files(self):
        raise NotImplementedError

    def export_risk(self):
        raise NotImplementedError

    def generate_risk(self):
        raise NotImplementedError




##########################################################################
# Main functions:
##########################################################################

def run_dycast(**kwargs):
    raise NotImplementedError
#     load_case_files()
#     daily_risk()
#     export_risk()


def import_cases(**kwargs):

    dycast_import = Dycast()

    dycast_import.cur, dycast_import.conn = database_service.init_db()
    dycast_import.case_table_name = database_service.get_case_table_name()
    dycast_import.srid_of_cases = kwargs.get('srid_cases')
    dycast_import.dead_birds_dir = kwargs.get(
        'import_directory', CONFIG.get("system", "import_directory"))
    dycast_import.files_to_import = kwargs.get('files')

    dycast_import.import_cases()


def generate_risk(**kwargs):

    dycast_risk = Dycast()

    dycast_risk.cur, dycast_risk.conn = database_service.init_db()

    dycast_risk.spatial_domain = float(kwargs.get('spatial_domain'))
    dycast_risk.temporal_domain = int(kwargs.get('temporal_domain'))
    dycast_risk.close_in_space = float(kwargs.get('close_in_space'))
    dycast_risk.close_in_time = int(kwargs.get('close_in_time'))
    dycast_risk.case_threshold = int(kwargs.get('case_threshold'))

    dycast_risk.startdate = kwargs.get('startdate', datetime.date.today())
    dycast_risk.enddate = kwargs.get('enddate', dycast_risk.startdate)

    dycast_risk.extent_min_x = kwargs.get('extent-min-x')
    dycast_risk.extent_min_y = kwargs.get('extent-min-y')
    dycast_risk.extent_max_x = kwargs.get('extent-max-x')
    dycast_risk.extent_max_y = kwargs.get('extent-max-y')
    dycast_risk.srid_of_extent = kwargs.get('srid-extent')

    dycast_risk.tmp_daily_case_table = database_service.get_tmp_daily_case_table_name()
    dycast_risk.tmp_cluster_per_point_selection_table = database_service.get_tmp_cluster_per_point_table_name()

    dycast_risk.generate_risk()


def export_risk(**kwargs):

    dycast_export = Dycast()

    dycast_export.cur, dycast_export.conn = database_service.init_db()

    dycast_export.risk_file_dir = kwargs.get(
        'export_directory', CONFIG.get("system", "export_directory"))
    dycast_export.startdate = kwargs.get('startdate', datetime.date.today())
    dycast_export.enddate = kwargs.get('enddate', dycast_export.startdate)

    dycast_export.export_risk()


def listen_for_input(**kwargs):
    raise NotImplementedError
