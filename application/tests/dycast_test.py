import unittest
import datetime
from application.services import risk_service
from application.services import database_service
from application.services import grid_service
from application.services import config_service
from application.tests import test_helper_functions
from application.models.classes import dycast_parameters

class TestDycastFunctions(unittest.TestCase):

    def test_get_vector_count_for_point(self):
        test_helper_functions.init_test_environment()

        cur, conn = database_service.init_db()
        config = config_service.get_config()

        case_table_name = config.get("database", "dead_birds_table_projected")
        tmp_daily_case_table = "tmp_daily_case_selection"
        system_coordinate_system = config.get("dycast", "system_coordinate_system")

        dycast_paramaters = dycast_parameters.DycastParameters()

        dycast_paramaters.srid_of_extent = "29193"
        dycast_paramaters.extent_min_x = 197457.283284349
        dycast_paramaters.extent_min_y = 7639474.3256114
        dycast_paramaters.extent_max_x = 198056.722079
        dycast_paramaters.extent_max_y = 7639344.265401

        dycast_paramaters.temporal_domain = 200

        riskdate = datetime.date(int(2006), int(4), int(25))

        risk_service.setup_tmp_daily_case_table_for_date(dycast_paramaters, case_table_name, tmp_daily_case_table, riskdate, cur, conn)

        gridpoints = grid_service.generate_grid(dycast_paramaters)

        point = gridpoints[0]
        count = risk_service.get_vector_count_for_point(dycast_paramaters, tmp_daily_case_table, point, system_coordinate_system, cur, conn)

        self.assertIsNotNone(count)
