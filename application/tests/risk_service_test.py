import unittest
import datetime
from application.services import import_service as import_service_module
from application.services import risk_service as risk_service_module
from application.services import database_service
from application.services import grid_service
from application.tests import test_helper_functions


test_helper_functions.init_test_environment()


class TestDycastFunctions(unittest.TestCase):

    def test_get_daily_cases_query(self):
        risk_service = risk_service_module.RiskService()

        session = database_service.get_sqlalchemy_session()

        dycast_paramaters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        daily_cases_query = risk_service.get_daily_cases_query(session, dycast_paramaters, riskdate)
        count = database_service.get_count_for_query(daily_cases_query)

        self.assertGreater(count, 0)


    def test_get_cases_in_cluster_query(self):
        test_helper_functions.init_test_environment()
        risk_service = risk_service_module.RiskService()

        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()

        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, riskdate)

        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)
        vector_count = database_service.get_count_for_query(cases_in_cluster_query)

        self.assertGreater(vector_count, 0)


    def test_generate_risk(self):
        import_service = import_service_module.ImportService()
        risk_service = risk_service_module.RiskService()

        dycast = test_helper_functions.get_dycast_parameters()

        import_service.load_case_files(dycast)
        risk_service.generate_risk(dycast)

        risk_count = test_helper_functions.get_count_from_table("risk")
        self.assertGreater(risk_count, 0)


    def test_get_close_space_and_time(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session,
                                                               dycast_parameters,
                                                               dycast_parameters.startdate)

        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query,
                                                                         dycast_parameters,
                                                                         point)

        count = risk_service.get_close_space_and_time(cases_in_cluster_query,
                                                      dycast_parameters.close_in_space,
                                                      dycast_parameters.close_in_time)
        self.assertGreater(count, 0)


    def test_get_close_space_only(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session,
                                                               dycast_parameters,
                                                               riskdate)

        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query,
                                                                         dycast_parameters,
                                                                         point)

        count = risk_service.get_close_space_only(cases_in_cluster_query,
                                                  dycast_parameters.close_in_space)
        self.assertGreater(count, 0)


    def test_close_time_only(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session,
                                                               dycast_parameters,
                                                               riskdate)

        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query,
                                                                         dycast_parameters,
                                                                         point)

        count = risk_service.get_close_time_only(cases_in_cluster_query,
                                                 dycast_parameters.close_in_time)
        self.assertGreater(count, 0)
