# Dycast functions

# dist_margs means "distribution marginals" and is the result of the
# monte carlo simulations.  See Theophilides et al. for more information

import sys
import os
import inspect
import logging

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
        self.dead_birds_dir = kwargs.get('import_directory', CONFIG.get("system", "import_directory"))

        self.files_to_import = kwargs.get('files')

    def import_cases(self):
        if self.files_to_import:
            logging.info("Loading files: %s", self.files_to_import)
            import_service.load_case_files(self)
        else:
            logging.info("Loading files from import path: %s", self.dead_birds_dir)
            raise NotImplementedError

        logging.info("Done loading cases")

    def listen_for_files(self):
        raise NotImplementedError


class DycastExport(DycastBase):

    def __init__(self, args):
        super(DycastExport, self).__init__()
        self._risk_file_dir = args.export_directory or CONFIG.get(
            "system", "export_directory")

    def export_risk(self):
        raise NotImplementedError


class DycastRisk(DycastBase):

    def __init__(self, args):
        super(DycastRisk, self).__init__()
        self._sd = float(args.spatial_domain)
        self._cs = float(args.close_in_space)
        self._ct = int(args.close_in_time)
        self._td = int(args.temporal_domain)
        self._threshold = int(args.case_threshold)

        self._tmp_daily_case_table = database_service.get_tmp_daily_case_table_name()
        self._tmp_cluster_per_point_selection_table = database_service.get_tmp_cluster_per_point_table_name()

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
