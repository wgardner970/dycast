import datetime
import logging
import time

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.sql.expression import literal

from application.models.classes.cluster import Cluster
from application.models.models import Case, DistributionMargin, Risk
from application.services import config_service
from application.services import database_service
from application.services import geography_service
from application.services import logging_service

CONFIG = config_service.get_config()


class RiskService(object):

    def __init__(self, dycast_parameters):
        self.system_coordinate_system = CONFIG.get(
            "dycast", "system_coordinate_system")
        self.dycast_parameters = dycast_parameters


    def generate_risk(self):

        session = database_service.get_sqlalchemy_session()
        logging_service.display_current_parameter_set(self.dycast_parameters)

        case_threshold = self.dycast_parameters.case_threshold

        gridpoints = geography_service.generate_grid(self.dycast_parameters)

        day = self.dycast_parameters.startdate
        delta = datetime.timedelta(days=1)

        while day <= self.dycast_parameters.enddate:
            start_time = time.time()
            logging.info("Starting daily_risk for %s", day)
            points_above_threshold = 0

            clusters_per_point_query = self.get_clusters_per_point_query(session, gridpoints, day)
            clusters_per_point = self.get_clusters_per_point_from_query(clusters_per_point_query)


            for cluster in clusters_per_point:
                vector_count = len(cluster.cases)
                if vector_count >= case_threshold:
                    points_above_threshold += 1
                    risk = Risk(risk_date=day,
                                number_of_cases=vector_count,
                                lat=cluster.point.y,
                                long=cluster.point.x)

                    self.insert_risk(session, risk)

            session.commit()


            logging.info(
                "Finished daily_risk for %s: done %s points", day, len(gridpoints))
            logging.info("Total points above threshold of %s: %s",
                            case_threshold, points_above_threshold)
            logging.info("Time elapsed: %.0f seconds",
                            time.time() - start_time)

            day += delta

        try:
            session.commit()
        except SQLAlchemyError, e:
            session.rollback()
            logging.exception("There was a problem committing the risk data session")
            logging.exception(e)
            raise
        finally:
            session.close()

    def insert_risk(self, session, risk):
        try:
            session.add(risk)
            session.commit()
        except IntegrityError, e:
            logging.warning("Risk already exists in database for this date '%s' and location '%s - %s', skipping...",
                            risk.risk_date, risk.lat, risk.long)
            session.rollback()
        except SQLAlchemyError, e:
            logging.exception("There was a problem inserting risk")
            logging.exception(e)
            session.rollback()
            raise

    def get_clusters_per_point_query(self, session, gridpoints, riskdate):
        days_prev = self.dycast_parameters.temporal_domain
        enddate = riskdate
        startdate = riskdate - datetime.timedelta(days=(days_prev))

        points_query = self.get_points_query_from_gridpoints(gridpoints)

        return session.query(func.array_agg(
                                func.json_build_object(
                                    Case.id,
                                    func.ST_AsText(Case.location)
                                )).label('case_array'),
                             points_query.c.point.geom.label('point')) \
                        .join(points_query, literal(True)) \
                        .filter(Case.report_date >= startdate,
                                Case.report_date <= enddate,
                                func.ST_DWithin(Case.location,
                                                points_query.c.point.geom,
                                                self.dycast_parameters.spatial_domain)) \
                        .group_by(points_query.c.point.geom)


    def get_clusters_per_point_from_query(self, cluster_per_point_query):
        """
        Because get_clusters_per_point_query() aggregates cases by point in a json format,
        here we create a proper collection of classes with it, in order to speed up further
        iterations over these clusters
        :param cluster_per_point_query:
        :return: array of Cluster objects
        """
        rows = cluster_per_point_query.all()
        cluster_per_point = []

        for row in rows:
            cluster = Cluster()
            cluster.point = geography_service.get_shape_from_sqlalch_element(row.point)
            cluster.cases = []

            for case_json in row.case_array:
                for case_id, case_location in case_json.iteritems():
                    case = Case()
                    case.id = case_id
                    case.location = geography_service.get_shape_from_literal_wkt(case_location)
                    cluster.cases.append(case)

            cluster_per_point.append(cluster)

        return cluster_per_point


    def get_points_query_from_gridpoints(self, gridpoints):
        return select([
                func.ST_DumpPoints(
                func.ST_Collect(array(gridpoints))) \
            .label('point')]) \
            .alias('point_query')


    def get_close_space_only(self, cluster_per_point):
        for cluster in cluster_per_point:
            cluster.close_in_space = 0
            for case in cluster.cases:
                for nearby_case in cluster.cases:
                    if case.id > nearby_case.id:
                        if geography_service.is_within_distance(case.location,
                                                                nearby_case.location,
                                                                self.dycast_parameters.close_in_space):
                            cluster.close_in_space += 1


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
