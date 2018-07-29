import datetime

from sqlalchemy import bindparam, func
from sqlalchemy.ext import baked
from sqlalchemy.sql.expression import literal

from application.models.models import Case, DistributionMargin
from application.services import config_service
from application.services import database_service

CONFIG = config_service.get_config()
bakery = baked.bakery()



class ComparativeTestService(object):
    """
    Temporary service to test new queries against old ones
    
    Arguments:
        object {DycastParameters} -- instance of DycastParameters class
    """


    def __init__(self, dycast_parameters):
        self.system_coordinate_system = CONFIG.get(
            "dycast", "system_coordinate_system")
        self.dycast_parameters = dycast_parameters


    def get_close_space_and_time_query(self, session):
        case_query = session.query(Case.id)
        subquery = session.query(Case).subquery()

        return case_query.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id) \
            .subquery()


    def get_daily_cases_query(self, session, riskdate):
        days_prev = self.dycast_parameters.temporal_domain
        enddate = riskdate
        startdate = riskdate - datetime.timedelta(days=(days_prev))

        return session.query(Case).filter(
            Case.report_date >= startdate,
            Case.report_date <= enddate
        )


    def get_cases_in_cluster_query(self, daily_cases_query, point):
        # wkt_point = geography_service.get_point_from_lat_long(point.y, point.x, self.system_coordinate_system)
        return daily_cases_query.filter(func.ST_DWithin(Case.location, point, self.dycast_parameters.spatial_domain))




    def get_close_space_and_time_baked(self, session, cluster):
        baked_query = bakery(lambda session: session.query(func.count(Case.id).label('count_cst)')))
        baked_query += lambda q: q.filter(Case.id.op('IN')(bindparam('case_id_array')))
        baked_query += self.get_cst_subquery_baked
        baked_query += lambda q: q.group_by(Case.id)

        return baked_query(session).params(case_id_array=tuple(cluster.case_id_array)).count()


    def get_cst_subquery_baked(self, main_query):
        subquery = main_query.session.query(Case) \
                .filter(Case.id.op('IN')(bindparam('case_id_array'))) \
                .subquery()
        return main_query.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id)


    def get_close_space_and_time_new(self, session, cluster):
        cases_in_cluster = session.query(Case).filter(Case.id.in_(cluster.case_id_array))
        subquery = cases_in_cluster.subquery()

        close_in_space_and_time = cases_in_cluster.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id)

        return database_service.get_count_for_query(close_in_space_and_time)


    def get_close_space_and_time(self, cases_in_cluster_query):
        subquery = cases_in_cluster_query.subquery()
        query = cases_in_cluster_query.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id)

        return database_service.get_count_for_query(query)

    def get_close_space_only_old(self, cases_in_cluster_query):
        subquery = cases_in_cluster_query.subquery()
        query = cases_in_cluster_query.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    Case.id < subquery.c.id)

        return database_service.get_count_for_query(query)


    def get_close_time_only(self, cases_in_cluster_query):
        subquery = cases_in_cluster_query.subquery()
        query = cases_in_cluster_query.join(subquery, literal(True)) \
            .filter(func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id)
        return database_service.get_count_for_query(query)



    # Probability
    def get_cumulative_probability(self, session, number_of_cases, close_in_space_and_time, close_in_space, close_in_time):
        exact_match = self.get_exact_match_cumulative_probability(session,
                                                                  number_of_cases,
                                                                  close_in_space_and_time,
                                                                  close_in_space,
                                                                  close_in_time)

        if exact_match:
            return exact_match
        else:
            nearest_close_in_time = self.get_nearest_close_in_time_distribution_margin(session,
                                                                                       number_of_cases,
                                                                                       close_in_space_and_time,
                                                                                       close_in_time)
            if nearest_close_in_time:
                cumulative_probability = self.get_cumulative_probability_by_nearest_close_in_time(session,
                                                                                                  number_of_cases,
                                                                                                  close_in_space_and_time,
                                                                                                  nearest_close_in_time,
                                                                                                  close_in_space)
                return cumulative_probability or 0.001
            else:
                return 0.0001


    def get_exact_match_cumulative_probability(self, session,
                                               number_of_cases,
                                               close_in_space_and_time,
                                               close_in_space,
                                               close_in_time):

        return session.query(DistributionMargin.cumulative_probability) \
            .filter(
                DistributionMargin.number_of_cases == number_of_cases,
                DistributionMargin.close_in_space_and_time == close_in_space_and_time,
                DistributionMargin.close_space == close_in_space,
                DistributionMargin.close_time == close_in_time) \
            .scalar()


    def get_nearest_close_in_time_distribution_margin(self, session,
                                                      number_of_cases,
                                                      close_in_space_and_time,
                                                      close_in_time):

        return session.query(DistributionMargin.close_time) \
            .filter(
                DistributionMargin.number_of_cases == number_of_cases,
                DistributionMargin.close_in_space_and_time >= close_in_space_and_time,
                DistributionMargin.close_time >= close_in_time) \
            .order_by(DistributionMargin.close_time) \
            .first()


    def get_cumulative_probability_by_nearest_close_in_time(self, session,
                                                            number_of_cases,
                                                            close_in_space_and_time,
                                                            nearest_close_in_time,
                                                            close_in_space):

        return session.query(DistributionMargin.cumulative_probability) \
            .filter(
                DistributionMargin.number_of_cases == number_of_cases,
                DistributionMargin.close_in_space_and_time >= close_in_space_and_time,
                DistributionMargin.close_time == nearest_close_in_time,
                DistributionMargin.close_space >= close_in_space) \
            .order_by(DistributionMargin.close_space) \
            .first()
