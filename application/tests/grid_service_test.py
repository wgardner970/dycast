import unittest
from application.services import grid_service
from application.services import config_service

class TestGridService(unittest.TestCase):

    def test_generate_grid(self):
        config = config_service.get_config()
        system_coordinate_system = config.get("dycast", "system_coordinate_system")

        user_coordinate_system = "29193"
        extent_min_x = 197457.283284349
        extent_min_y = 7639474.3256114
        extent_max_x = 198056.722079
        extent_max_y = 7639344.265401

        gridpoints = grid_service.generate_grid(user_coordinate_system, system_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y)
        self.assertIsNotNone(gridpoints)
        self.assertGreaterEqual(len(gridpoints), 1)
