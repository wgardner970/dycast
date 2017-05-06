import os
import sys
import inspect
import unittest
from application import dycast
from application.services import grid_service
from application.services import config_service
from application.services import debug_service

class TestDycastFunctions(unittest.TestCase):

    def test_get_vector_count_for_point(self):

        config = config_service.get_default_config()
        dycast.read_config(None, config)
        dycast.init_db()

        srid = "29193"
        extent_min_x = 197457.283284349
        extent_min_y = 7639474.3256114
        extent_max_x = 197458.283284349
        extent_max_y = 7639475.3256114

        gridpoints = grid_service.generate_grid(
            srid, extent_min_x, extent_min_y, extent_max_x, extent_max_y)

        case_table = config.get("database", "dead_birds_table_projected")
        point = gridpoints[0]
        projection_from = "29193"
        projection_to = "29193"
        count = dycast.get_vector_count_for_point(
            case_table, point, projection_from, projection_to)

        self.assertIsNotNone(count)
