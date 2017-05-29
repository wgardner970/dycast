import os
import sys
import inspect
import unittest
import datetime
from application import dycast
from application.services import grid_service
from application.services import config_service
from application.services import debug_service

class TestDycastFunctions(unittest.TestCase):

    def test_get_vector_count_for_point(self):

        config = config_service.get_default_config()
        dycast.read_config(None, config)
        dycast.init_db()
        system_coordinate_system = config.get("dycast", "system_coordinate_system")
        temporal_domain = int(config.get("dycast", "temporal_domain"))
        tmp_daily_case_table = config.get("database", "tmp_daily_case_table")

        user_coordinate_system = "29193"
        extent_min_x = 197457.283284349
        extent_min_y = 7639474.3256114
        extent_max_x = 198056.722079
        extent_max_y = 7639344.265401
        riskdate = datetime.date(int(2006), int(4), int(25))

        dycast.setup_tmp_daily_case_table_for_date(tmp_daily_case_table, riskdate, temporal_domain)

        gridpoints = grid_service.generate_grid(user_coordinate_system, system_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y)

        point = gridpoints[0]
        count = dycast.get_vector_count_for_point(tmp_daily_case_table, point)

        self.assertIsNotNone(count)
