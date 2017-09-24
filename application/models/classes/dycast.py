# Dycast functions

# dist_margs means "distribution marginals" and is the result of the
# monte carlo simulations.  See Theophilides et al. for more information

import sys
import os
import inspect
import logging
import datetime

from application.services import import_service
# from services import export_service
from application.models.enums import enums


APPLICATION_ROOT = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
lib_dir = os.path.join(APPLICATION_ROOT, 'libs')
sys.path.append(lib_dir)
sys.path.append(os.path.join(lib_dir, "psycopg2"))
sys.path.append(os.path.join(lib_dir, "dbfpy"))




class Dycast(object):

    def __init__(self, **kwargs):
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
