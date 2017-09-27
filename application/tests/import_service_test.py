import unittest
import psycopg2
from application.services import import_service as import_service_module
from application.services import database_service
from application.tests import test_helper_functions
from application.models.classes import dycast_parameters
from application.models.enums import enums


test_helper_functions.init_test_environment()


class TestImportServiceFunctions(unittest.TestCase):

    def test_load_cases(self):
        import_service = import_service_module.ImportService()

        dycast = dycast_parameters.DycastParameters()

        dycast.srid_of_cases = '3857'
        dycast.files_to_import = test_helper_functions.get_test_cases_import_files_latlong()

        import_service.load_case_files(dycast)

    def test_load_case_correct(self):
        import_service = import_service_module.ImportService()
        cur, conn = database_service.init_db()

        dycast = dycast_parameters.DycastParameters()
        dycast.srid_of_cases = 3857

        line_correct = "1\t03/09/16\t1832445.278\t2118527.399"
        location_type = enums.Location_type.LAT_LONG

        import_service.load_case(dycast, line_correct, location_type, cur, conn)

        with self.assertRaises(psycopg2.DataError) as context:
            line_incorrect_date = "1\t30/09/16\t1832445.278\t2118527.399"            
            import_service.load_case(dycast, line_incorrect_date, location_type, cur, conn)

    def test_load_case_data_error(self):
        import_service = import_service_module.ImportService()
        cur, conn = database_service.init_db()

        dycast = dycast_parameters.DycastParameters()
        dycast.srid_of_cases = 3857

        location_type = enums.Location_type.LAT_LONG

        with self.assertRaises(psycopg2.DataError) as context:
            line_incorrect_date = "1\t30/09/16\t1832445.278\t2118527.399"            
            import_service.load_case(dycast, line_incorrect_date, location_type, cur, conn)
