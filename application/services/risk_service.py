import datetime
import time
import logging
import sys
import psycopg2

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.sql.expression import literal

from application.services import config_service
from application.services import logging_service
from application.services import database_service
from application.services import geography_service

from application.models.models import Case, DistributionMargin, Risk


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

            daily_cases_query = self.get_daily_cases_query(session, day)
            daily_case_count = database_service.get_count_for_query(daily_cases_query)

            if daily_case_count >= case_threshold:
                start_time = time.time()
                logging.info("Starting daily_risk for %s", day)
                points_above_threshold = 0

                for point in gridpoints:
                    cases_in_cluster_query = self.get_cases_in_cluster_query(daily_cases_query, point)
                    vector_count = database_service.get_count_for_query(cases_in_cluster_query)
                    if vector_count >= case_threshold:
                        points_above_threshold += 1
                        risk = Risk(risk_date=day,
                                    number_of_cases=vector_count,
                                    lat=point.x,
                                    long=point.y)

                        risk.close_pairs = self.get_close_space_and_time(cases_in_cluster_query)
                        risk.close_space = self.get_close_space_only(cases_in_cluster_query)
                        risk.close_time = self.get_close_time_only(cases_in_cluster_query)

                        risk.cumulative_probability = self.get_cumulative_probability(session,
                                                                                      risk.number_of_cases,
                                                                                      risk.close_pairs,
                                                                                      risk.close_space,
                                                                                      risk.close_time)
                        self.insert_risk(session, risk)

                logging.info(
                    "Finished daily_risk for %s: done %s points", day, len(gridpoints))
                logging.info("Total points above threshold of %s: %s",
                             case_threshold, points_above_threshold)
                logging.info("Time elapsed: %.0f seconds",
                             time.time() - start_time)
            else:
                logging.info("Amount of cases for %s lower than threshold %s: %s, skipping.",
                             day, case_threshold, daily_case_count)

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
            session.flush()
        except IntegrityError, e:
            logging.warning("Risk already exists in database for this date '%s' and location '%s - %s', skipping...",
                            risk.risk_date, risk.lat, risk.long)
            session.rollback()
        except SQLAlchemyError, e:
            logging.exception("There was a problem inserting risk")
            logging.exception(e)
            session.rollback()
            raise


    def get_daily_cases_query(self, session, riskdate):
        days_prev = self.dycast_parameters.temporal_domain
        enddate = riskdate
        startdate = riskdate - datetime.timedelta(days=(days_prev))

        return session.query(Case).filter(
            Case.report_date >= startdate,
            Case.report_date <= enddate
        )


    def get_cases_in_cluster_query(self, daily_cases_query, point):
        wkt_point = geography_service.get_point_from_lat_long(point.y, point.x, self.system_coordinate_system)

        return daily_cases_query.filter(func.ST_DWithin(Case.location, wkt_point, self.dycast_parameters.spatial_domain))


    def cst_cs_ct_wrapper(self, cur, conn):
        close_in_space = self.dycast_parameters.close_in_space
        close_in_time = self.dycast_parameters.close_in_time

        querystring = "SELECT * FROM cst_cs_ct(%s, %s)"
        try:
            cur.execute(querystring, (close_in_space, close_in_time))
        except Exception:
            conn.rollback()
            logging.exception("Can't select cst_cs_ct function")
            logging.info("Rolling back and exiting...")
            sys.exit()
        return cur.fetchall()

    def nmcm_wrapper(self, number_of_cases, close_pairs, close_space, close_time, cur, conn):
        querystring = "SELECT * FROM nmcm(%s, %s, %s, %s)"
        try:
            cur.execute(querystring, (number_of_cases, close_pairs,
                                      close_space, close_time))
        except Exception:
            conn.rollback()
            logging.exception("Can't select nmcm function")
            logging.info("Rolling back and exiting...")
            sys.exit()
        return cur.fetchall()


    def get_close_space_and_time(self, cases_in_cluster_query):
        subquery = cases_in_cluster_query.subquery()
        query = cases_in_cluster_query.join(subquery, literal(True)) \
            .filter(func.ST_DWithin(Case.location, subquery.c.location, self.dycast_parameters.close_in_space),
                    func.abs(Case.report_date - subquery.c.report_date) <= self.dycast_parameters.close_in_time,
                    Case.id < subquery.c.id)

        return database_service.get_count_for_query(query)


    def get_close_space_only(self, cases_in_cluster_query):
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

        return session.query(DistributionMargin) \
            .filter(
                DistributionMargin.number_of_cases == number_of_cases,
                DistributionMargin.close_in_space_and_time >= close_in_space_and_time,
                DistributionMargin.close_time == nearest_close_in_time,
                DistributionMargin.close_space >= close_in_space) \
            .order_by(DistributionMargin.close_space) \
            .first()
