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
                vector_count = cluster.get_case_count()
                if vector_count >= case_threshold:
                    points_above_threshold += 1
                    self.get_close_space_and_time_for_cluster(cluster)
                    risk = Risk(risk_date=day,
                                number_of_cases=vector_count,
                                lat=cluster.point.y,
                                long=cluster.point.x,
                                close_pairs=cluster.close_space_and_time,
                                close_space=cluster.close_in_space,
                                close_time=cluster.close_in_time)

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
                "case_id",
                Case.id,
                "report_date",
                Case.report_date,
                "location",
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
        clusters_per_point = []

        for row in rows:
            cluster = Cluster()
            cluster.point = geography_service.get_shape_from_sqlalch_element(row.point)
            cluster.cases = []

            for case_json in row.case_array:
                case = Case()

                case.id = case_json['case_id']
                case.report_date = datetime.datetime.strptime(case_json['report_date'], "%Y-%m-%d").date()
                case.location = geography_service.get_shape_from_literal_wkt(case_json['location'])

                cluster.cases.append(case)

            clusters_per_point.append(cluster)

        return clusters_per_point

    def get_points_query_from_gridpoints(self, gridpoints):
        return select([
            func.ST_DumpPoints(
                func.ST_Collect(array(gridpoints)))
                .label('point')]) \
            .alias('point_query')

    def enrich_clusters_per_point_with_close_space_and_time(self, clusters_per_point):
        for cluster in clusters_per_point:
            self.get_close_space_and_time_for_cluster(cluster)

    def get_close_space_and_time_for_cluster(self, cluster):

        cluster.close_in_space = 0
        cluster.close_in_time = 0
        cluster.close_space_and_time = 0

        for case in cluster.cases:
            for nearby_case in cluster.cases:
                if case.id > nearby_case.id:
                    is_close_in_time = False
                    is_close_in_space = False
                    if geography_service.is_within_distance(case.location,
                                                            nearby_case.location,
                                                            self.dycast_parameters.close_in_space):
                        is_close_in_space = True
                        cluster.close_in_space += 1
                    if abs((case.report_date - nearby_case.report_date).days) <= self.dycast_parameters.close_in_time:
                        is_close_in_time = True
                        cluster.close_in_time += 1
                    if is_close_in_space & is_close_in_time:
                        cluster.close_space_and_time += 1

    def get_distribution_margins_for_clusters(self, session, clusters_per_point):

        for cluster in clusters_per_point:
            exact_match_subquery = session.query(DistributionMargin.cumulative_probability) \
                .filter(
                DistributionMargin.number_of_cases == cluster.get_case_count(),
                DistributionMargin.close_in_space_and_time == cluster.close_space_and_time,
                DistributionMargin.close_space == cluster.close_in_space,
                DistributionMargin.close_time == cluster.close_in_time) \
                .as_scalar()

            nearest_close_time_subquery = session.query(DistributionMargin.close_time) \
                .filter(
                DistributionMargin.number_of_cases == cluster.get_case_count(),
                DistributionMargin.close_in_space_and_time >= cluster.close_space_and_time,
                DistributionMargin.close_time >= cluster.close_in_time) \
                .order_by(DistributionMargin.close_time) \
                .limit(1)

            res1 = nearest_close_time_subquery.first()

            probability_by_nearest_close_time_subquery = session.query(DistributionMargin.cumulative_probability) \
                .filter(
                DistributionMargin.number_of_cases == cluster.get_case_count(),
                DistributionMargin.close_in_space_and_time >= cluster.close_space_and_time,
                DistributionMargin.close_space >= cluster.close_in_space,
                DistributionMargin.close_time == nearest_close_time_subquery) \
                .order_by(DistributionMargin.close_space) \
                .limit(1)

            res2 = probability_by_nearest_close_time_subquery.first()

            result = session.query(exact_match_subquery.label('exact_match'),
                                   sqlalchemy_case([(nearest_close_time_subquery is None, '0.0001'),
                                                    (probability_by_nearest_close_time_subquery is None,
                                                     '0.001')],
                                                   else_=probability_by_nearest_close_time_subquery.first())
                                   .label('by_nearest_close_time')) \
                .all()

            print result
