import datetime
import unittest

from application.models.models import Case, Risk
from application.services import database_service
from application.services import geography_service
from application.services import import_service as import_service_module
from application.services import risk_service as risk_service_module
from application.tests import comparative_test_service as comparative_test_service_module
from application.tests import test_helper_functions


class TestRiskServiceFunctions(unittest.TestCase):

    def test_get_clusters_per_point_query(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters(large_dataset=False)
        risk_service = risk_service_module.RiskService(dycast_parameters)
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)

        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))
        gridpoints = geography_service.generate_grid(dycast_parameters)

        clusters_per_point = risk_service.get_clusters_per_point_query(session, gridpoints, riskdate).all()

        daily_cases_query = comparative_test_service.get_daily_cases_query(session, riskdate)

        for cluster in clusters_per_point:
            point = geography_service.get_shape_from_wkb(cluster.point)
            point_wkt_element = geography_service.get_wktelement_from_wkt(point.to_wkt())

            cases_in_cluster_query = comparative_test_service.get_cases_in_cluster_query(daily_cases_query,
                                                                                         point_wkt_element)

            vector_count_new = len(cluster.case_array)
            vector_count_old = database_service.get_count_for_query(cases_in_cluster_query)

            self.assertEqual(vector_count_new, vector_count_old)



    def test_get_daily_cases_query(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)

        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))

        daily_cases_query = comparative_test_service.get_daily_cases_query(session, riskdate)
        count = database_service.get_count_for_query(daily_cases_query)

        self.assertGreater(count, 0)

    def test_get_cases_in_cluster_query(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)

        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = geography_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = comparative_test_service.get_daily_cases_query(session, riskdate)

        cases_in_cluster_query = comparative_test_service.get_cases_in_cluster_query(daily_cases_query, point)
        vector_count = database_service.get_count_for_query(cases_in_cluster_query)

        self.assertGreater(vector_count, 0)

    def test_generate_risk(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)

        import_service = import_service_module.ImportService()
        import_service.load_case_files(dycast_parameters)

        risk_service.generate_risk()

        risk_count = test_helper_functions.get_count_from_table("risk")
        self.assertGreater(risk_count, 0)


    def test_insert_risk(self):

        session = database_service.get_sqlalchemy_session()

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)

        gridpoints = geography_service.generate_grid(dycast_parameters)
        point = gridpoints[0]


        risk = Risk(risk_date=datetime.date(int(2016), int(3), int(25)),
                    number_of_cases=5,
                    lat=point.x,
                    long=point.y,
                    close_pairs=3,
                    close_space=2,
                    close_time=1,
                    cumulative_probability=0.032)

        risk_service.insert_risk(session, risk)
        session.commit()

        session.query(Risk.risk_date).filter(Risk.risk_date == risk.risk_date,
                                             Risk.lat == risk.lat,
                                             Risk.long == risk.long) \
            .one()

    def test_get_close_space_and_time(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = geography_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = comparative_test_service.get_daily_cases_query(session,
                                                                           riskdate)

        cases_in_cluster_query = comparative_test_service.get_cases_in_cluster_query(daily_cases_query,
                                                                                     point)

        self.assertGreater(count, 0)
        count = comparative_test_service.get_close_space_and_time(cases_in_cluster_query)


    def test_get_close_space_only(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()
    def test_get_close_space_only_old(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = geography_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = comparative_test_service.get_daily_cases_query(session,
                                                                           riskdate)

        cases_in_cluster_query = comparative_test_service.get_cases_in_cluster_query(daily_cases_query,
                                                                                     point)

        count = comparative_test_service.get_close_space_only_old(cases_in_cluster_query)
        self.assertGreater(count, 0)

    def test_close_time_only(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        comparative_test_service = comparative_test_service_module.ComparativeTestService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        riskdate = datetime.date(int(2016), int(3), int(25))

        gridpoints = geography_service.generate_grid(dycast_parameters)
        point = gridpoints[0]

        daily_cases_query = comparative_test_service.get_daily_cases_query(session,
                                                                           riskdate)

        cases_in_cluster_query = comparative_test_service.get_cases_in_cluster_query(daily_cases_query,
                                                                                     point)

        count = comparative_test_service.get_close_time_only(cases_in_cluster_query)
        self.assertGreater(count, 0)

    def test_get_exact_match_distribution_margin(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        number_of_cases = 2
        close_in_space_and_time = 1
        close_in_space = 1
        close_in_time = 1

        cumulative_probability = risk_service.get_exact_match_cumulative_probability(session,
                                                                                     number_of_cases,
                                                                                     close_in_space_and_time,
                                                                                     close_in_space,
                                                                                     close_in_time)

        self.assertGreater(cumulative_probability, 0)

    def test_get_nearest_close_in_time_distribution_margin(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        number_of_cases = 2
        close_in_space_and_time = 1
        close_in_time = 1

        nearest_close_in_time = risk_service.get_nearest_close_in_time_distribution_margin(session,
                                                                                           number_of_cases,
                                                                                           close_in_space_and_time,
                                                                                           close_in_time)

        self.assertGreater(nearest_close_in_time, 0)

    def test_get_cumulative_probability_by_nearest_close_in_time(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        number_of_cases = 2
        close_in_space_and_time = 1
        close_in_space = 1
        nearest_close_in_time = 1

        cumulative_probability = risk_service.get_cumulative_probability_by_nearest_close_in_time(session,
                                                                                                  number_of_cases,
                                                                                                  close_in_space_and_time,
                                                                                                  nearest_close_in_time,
                                                                                                  close_in_space)

        self.assertGreater(cumulative_probability, 0)

    def test_get_cumulative_probability(self):

        dycast_parameters = test_helper_functions.get_dycast_parameters()
        risk_service = risk_service_module.RiskService(dycast_parameters)
        session = database_service.get_sqlalchemy_session()

        number_of_cases = 2
        close_in_space_and_time = 1
        close_in_space = 1
        close_in_time = 1

        cumulative_probability = risk_service.get_cumulative_probability(session,
                                                                         number_of_cases,
                                                                         close_in_space_and_time,
                                                                         close_in_time,
                                                                         close_in_space)

        self.assertGreater(cumulative_probability, 0)
