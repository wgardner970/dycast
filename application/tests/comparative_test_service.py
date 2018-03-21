import datetime
import time
import logging

from sqlalchemy import bindparam, func, select
from sqlalchemy.sql.expression import literal
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext import baked

from application.services import config_service
from application.services import logging_service
from application.services import database_service

from application.models.models import Case, DistributionMargin, Risk
from application.models.classes.cluster import Cluster


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