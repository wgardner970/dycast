import unittest
from application.services import grid_service


class TestGridService(unittest.TestCase):

    def test_generate_grid(self):
        srid = "29193"
        extentMinX = 197457.283284349
        extentMinY = 7639474.3256114
        extentMaxX = 224257.283284349
        extentMaxY = 7666274.3256114

        gridpoints = grid_service.generate_grid(srid, extentMinX, extentMinY, extentMaxX, extentMaxY)
        self.assertIsNotNone(gridpoints)
