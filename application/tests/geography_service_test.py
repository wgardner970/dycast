import unittest
import datetime

from application.services import risk_service as risk_service_module
from application.services import database_service
from application.services import geography_service
from application.services import grid_service
from application.tests import test_helper_functions


test_helper_functions.init_test_environment()


class TestGeographyServiceFunctions(unittest.TestCase):

    def test_get_close_space_only(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, riskdate)
        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)

        count = geography_service.get_close_space_only(cases_in_cluster_query, dycast_parameters.close_in_space)
        self.assertGreater(count, 0)


    def test_close_time_only(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, riskdate)
        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)

        count = geography_service.get_close_time_only(cases_in_cluster_query, dycast_parameters.close_in_time)
        self.assertGreater(count, 0)

