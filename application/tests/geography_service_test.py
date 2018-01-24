import unittest
import datetime

from application.services import risk_service as risk_service_module
from application.services import database_service
from application.services import geography_service
from application.services import grid_service
from application.tests import test_helper_functions


test_helper_functions.init_test_environment()


class TestGeographyServiceFunctions(unittest.TestCase):

    def test_get_close_space_and_time(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, dycast_parameters.startdate)
        cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)

        count = geography_service.get_close_space_and_time(cases_in_cluster_query,
                                                           dycast_parameters.close_in_space,
                                                           dycast_parameters.close_in_time)
        self.assertGreater(count, 0)


    def test_get_close_space_and_time_compare(self):
        risk_service = risk_service_module.RiskService()
        cur, conn = database_service.init_psycopg_db()
        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()

        gridpoints = grid_service.generate_grid(dycast_parameters)
        for point in gridpoints:
            daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, dycast_parameters.startdate)
            cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)
            count_new = geography_service.get_close_space_and_time(cases_in_cluster_query,
                                                                   dycast_parameters.close_in_space,
                                                                   dycast_parameters.close_in_time)

            risk_service.setup_tmp_daily_case_table_for_date(dycast_parameters, dycast_parameters.startdate, cur, conn)
            risk_service.insert_cases_in_cluster_table(dycast_parameters, point, cur, conn)
            count_old = geography_service.get_close_space_and_time_old(dycast_parameters.close_in_space,
                                                                       dycast_parameters.close_in_time,
                                                                       cur)[0]

            self.assertEqual(count_new, count_old)


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



    def test_get_close_space_only_old(self):

        cur, conn = database_service.init_psycopg_db()
        risk_service = risk_service_module.RiskService()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        risk_service.setup_tmp_daily_case_table_for_date(dycast_parameters, riskdate, cur, conn)
        risk_service.insert_cases_in_cluster_table(dycast_parameters, point, cur, conn)

        count = geography_service.get_close_space_only_old(dycast_parameters.close_in_space, cur)
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


    def test_get_close_time_only_old(self):

        cur, conn = database_service.init_psycopg_db()
        risk_service = risk_service_module.RiskService()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        risk_service.setup_tmp_daily_case_table_for_date(dycast_parameters, riskdate, cur, conn)
        risk_service.insert_cases_in_cluster_table(dycast_parameters, point, cur, conn)

        count = geography_service.get_close_time_only_old(dycast_parameters.close_in_time, cur)
        self.assertGreater(count, 0)

    def test_get_close_space_only_compare(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()
        cur, conn = database_service.init_psycopg_db()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)

        for point in gridpoints:
            daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, riskdate)
            cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)
            count_new = geography_service.get_close_space_only(cases_in_cluster_query, dycast_parameters.close_in_space)

            risk_service.setup_tmp_daily_case_table_for_date(dycast_parameters, riskdate, cur, conn)
            risk_service.insert_cases_in_cluster_table(dycast_parameters, point, cur, conn)
            count_old = geography_service.get_close_space_only_old(dycast_parameters.close_in_space, cur)

            self.assertEqual(count_new, count_old[0])


    def test_get_close_time_only_compare(self):

        risk_service = risk_service_module.RiskService()
        session = database_service.get_sqlalchemy_session()
        cur, conn = database_service.init_psycopg_db()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = grid_service.generate_grid(dycast_parameters)
        for point in gridpoints:

            daily_cases_query = risk_service.get_daily_cases_query(session, dycast_parameters, riskdate)
            cases_in_cluster_query = risk_service.get_cases_in_cluster_query(daily_cases_query, dycast_parameters, point)
            count_new = geography_service.get_close_time_only(cases_in_cluster_query, dycast_parameters.close_in_time)

            risk_service.setup_tmp_daily_case_table_for_date(dycast_parameters, riskdate, cur, conn)
            risk_service.insert_cases_in_cluster_table(dycast_parameters, point, cur, conn)
            count_old = geography_service.get_close_time_only_old(dycast_parameters.close_in_time, cur)

            self.assertEqual(count_new, count_old[0])
