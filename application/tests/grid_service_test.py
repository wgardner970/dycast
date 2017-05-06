import unittest
from application.services import grid_service


class TestGridService(unittest.TestCase):

    def test_generate_grid(self):
        srid = "29193"
        extent_mix_x = 197457.283284349
        extent_max_x = 197957.283284349
        extent_min_y = 7639474.3256114
        extent_max_y = 7639974.3256114

        gridpoints = grid_service.generate_grid(srid, extent_mix_x, extent_min_y, extent_max_x, extent_max_y)
        self.assertIsNotNone(gridpoints)
