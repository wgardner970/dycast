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


class DycastBase(object):

    def __init__(self, **args):
        self.cur, self.conn = database_service.init_db()

        self.case_table_name = database_service.get_case_table_name()


class DycastImport(DycastBase):

    def __init__(self, **kwargs):
        super(DycastImport, self).__init__(**kwargs)

        self.srid_of_cases = kwargs.get('srid_cases')
        print self.srid_of_cases
        self.dead_birds_dir = kwargs.get(
            'import_directory', CONFIG.get("system", "import_directory"))

        self.files_to_import = kwargs.get('files')

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


class DycastExport(DycastBase):

    def __init__(self, **kwargs):
        super(DycastExport, self).__init__(**kwargs)
        self._risk_file_dir = kwargs.get(
            'export_directory', CONFIG.get("system", "export_directory"))

    def export_risk(self):
        raise NotImplementedError


class DycastRisk(DycastBase):

    def __init__(self, **kwargs):
        super(DycastRisk, self).__init__(**kwargs)
        self.spatial_domain = float(kwargs.get('spatial_domain'))
        self.temporal_domain = int(kwargs.get('temporal_domain'))
        self.close_in_space = float(kwargs.get('close_in_space'))
        self.close_in_time = int(kwargs.get('close_in_time'))
        self.case_threshold = int(kwargs.get('case_threshold'))

        self.startdate = kwargs.get('startdate', datetime.date.today())
        self.enddate = kwargs.get('enddate', self.startdate)

        self.extent_min_x = kwargs.get('extent-min-x')
        self.extent_min_y = kwargs.get('extent-min-y')
        self.extent_max_x = kwargs.get('extent-max-x')
        self.extent_max_y = kwargs.get('extent-max-y')
        self.srid_extent = kwargs.get('srid-extent')

        self.tmp_daily_case_table = database_service.get_tmp_daily_case_table_name()
        self.tmp_cluster_per_point_selection_table = database_service.get_tmp_cluster_per_point_table_name()

    def generate_risk(self):
        raise NotImplementedError


class Dycast(DycastImport, DycastExport):
    def __init__(self, args):
        super(Dycast, self).__init__()


##########################################################################
# Main functions:
##########################################################################

def run_dycast(**kwargs):
    raise NotImplementedError
#     load_case_files()
#     daily_risk()
#     export_risk()


def import_cases(**kwargs):

    dycast_import = DycastImport(**kwargs)
    dycast_import.import_cases()


def generate_risk(**kwargs):

    dycast_risk = DycastRisk(**kwargs)
    dycast_risk.generate_risk()


def export_risk(**kwargs):

    dycast_export = DycastExport(**kwargs)
    dycast_export.export_risk()


def listen_for_input(**kwargs):
    raise NotImplementedError
