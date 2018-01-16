import datetime
import time
import logging
import sys
import psycopg2

from application.services import grid_service
from application.services import config_service
from application.services import logging_service
from application.services import database_service


CONFIG = config_service.get_config()


class RiskService(object):

    def __init__(self, **kwargs):
        self.system_coordinate_system = CONFIG.get(
            "dycast", "system_coordinate_system")
        self.case_table_name = CONFIG.get(
            "database", "dead_birds_table_projected")
        self.tmp_daily_case_table = CONFIG.get(
            "database", "tmp_daily_case_table")
        self.tmp_cluster_per_point_selection_table = CONFIG.get(
            "database", "tmp_cluster_per_point_selection_table")

    def generate_risk(self, dycast_parameters):

        logging_service.display_current_parameter_set(dycast_parameters)

        case_threshold = dycast_parameters.case_threshold
        cur, conn = database_service.init_db()

        gridpoints = grid_service.generate_grid(dycast_parameters)

        day = dycast_parameters.startdate
        delta = datetime.timedelta(days=1)

        while day <= dycast_parameters.enddate:

            self.setup_tmp_daily_case_table_for_date(dycast_parameters, day, cur, conn)
            daily_case_count = self.get_daily_case_count(day, cur, conn)

            if daily_case_count >= case_threshold:
                start_time = time.time()
                logging.info("Starting daily_risk for %s", day)
                points_above_threshold = 0

                for point in gridpoints:
                    vector_count = self.get_vector_count_for_point(dycast_parameters, point, cur, conn)
                    if vector_count >= case_threshold:
                        points_above_threshold += 1
                        self.insert_cases_in_cluster_table(
                            dycast_parameters, point, cur, conn)
                        results = self.cst_cs_ct_wrapper(
                            dycast_parameters, cur, conn)
                        close_pairs = results[0][0]
                        close_space = results[1][0] - close_pairs
                        close_time = results[2][0] - close_pairs
                        result2 = self.nmcm_wrapper(
                            vector_count, close_pairs, close_space, close_time, cur, conn)
                        self.insert_result(day, point.x, point.y, vector_count, close_pairs,
                                           close_time, close_space, result2[0][0], cur, conn)

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

    def setup_tmp_daily_case_table_for_date(self, dycast_parameters, riskdate, cur, conn):
        days_prev = dycast_parameters.temporal_domain
        enddate = riskdate
        startdate = riskdate - datetime.timedelta(days=(days_prev))
        querystring = "TRUNCATE " + self.tmp_daily_case_table + "; INSERT INTO " + self.tmp_daily_case_table + \
            " SELECT * from " + self.case_table_name + \
            " where report_date >= %s and report_date <= %s"
        try:
            cur.execute(querystring, (startdate, enddate))
        except Exception:
            conn.rollback()
            logging.exception(
                "Something went wrong when setting up tmp_daily_case_selection table: " + str(riskdate))
            raise
        conn.commit()

    def get_daily_case_count(self,  riskdate, cur, conn):
        query = "SELECT COUNT(*) FROM {0}".format(self.tmp_daily_case_table)
        try:
            cur.execute(query)
        except Exception:
            conn.rollback()
            logging.exception(
                "Something went wrong when getting count from tmp_daily_case_selection table: " + str(riskdate))
            raise
        result_count = cur.fetchone()
        return result_count[0]

    def get_vector_count_for_point(self, dycast_parameters, point, cur, conn):
        querystring = "SELECT count(*) from \"" + self.tmp_daily_case_table + \
            "\" a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s"
        try:
            cur.execute(querystring, (point.x, point.y,
                                      self.system_coordinate_system, dycast_parameters.spatial_domain))
        except Exception:
            conn.rollback()
            logging.exception("Can't select vector count, exiting...")
            sys.exit()
        new_row = cur.fetchone()
        return new_row[0]

    def insert_cases_in_cluster_table(self, dycast_parameters, point, cur, conn):
        querystring = "TRUNCATE " + self.tmp_cluster_per_point_selection_table + "; INSERT INTO " + self.tmp_cluster_per_point_selection_table + \
            " SELECT * from " + self.tmp_daily_case_table + \
            " a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s"
        try:
            cur.execute(querystring, (point.x, point.y,
                                      self.system_coordinate_system, dycast_parameters.spatial_domain))
        except Exception:
            conn.rollback()
            logging.exception("Something went wrong at point: " + str(point))
            logging.info("Rolling back and exiting...")
            sys.exit(1)
        conn.commit()

    def cst_cs_ct_wrapper(self, dycast_parameters, cur, conn):
        close_in_space = dycast_parameters.close_in_space
        close_in_time = dycast_parameters.close_in_time

        querystring = "SELECT * FROM cst_cs_ct(%s, %s)"
        try:
            cur.execute(querystring, (close_in_space, close_in_time))
        except Exception:
            conn.rollback()
            logging.exception("Can't select cst_cs_ct function")
            logging.info("Rolling back and exiting...")
            sys.exit()
        return cur.fetchall()

    def nmcm_wrapper(self, num_birds, close_pairs, close_space, close_time, cur, conn):
        querystring = "SELECT * FROM nmcm(%s, %s, %s, %s)"
        try:
            cur.execute(querystring, (num_birds, close_pairs,
                                      close_space, close_time))
        except Exception:
            conn.rollback()
            logging.exception("Can't select nmcm function")
            logging.info("Rolling back and exiting...")
            sys.exit()
        return cur.fetchall()

    def insert_result(self, riskdate, latitude, longitude, number_of_cases, close_pairs, close_time, close_space, nmcm, cur, conn):
        querystring = "INSERT INTO risk (risk_date, lat, long, num_birds, close_pairs, close_space, close_time, nmcm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            # Be careful of the ordering of space and time in the db vs the txt file
            cur.execute(querystring, (riskdate, latitude, longitude,
                                      number_of_cases, close_pairs, close_space, close_time, nmcm))
        except psycopg2.IntegrityError:
            conn.rollback()
            logging.warning(
                "Risk already exists in database for this date '%s' and location '%s - %s', skipping...", riskdate, latitude, longitude)
        except Exception:
            conn.rollback()
            logging.exception("Couldn't insert risk")
            logging.info("Rolling back and exiting...")
            sys.exit(1)
        else:
            conn.commit()
