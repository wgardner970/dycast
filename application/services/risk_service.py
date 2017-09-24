import datetime
import time
import logging
import sys

from application.services import grid_service
from application.services import config_service
from application.services import logging_service
from application.services import database_service


CONFIG = config_service.get_config()


##########################################################################
# functions for generating risk:
##########################################################################

def generate_risk(dycast_parameters):

    case_threshold = dycast_parameters.case_threshold
    cur, conn = database_service.init_db()
    
    system_coordinate_system = CONFIG.get("dycast", "system_coordinate_system")
    case_table_name = CONFIG.get("database", "dead_birds_table_projected")
    tmp_daily_case_table = CONFIG.get("database", "tmp_daily_case_table")
    tmp_cluster_per_point_selection_table = CONFIG.get("database", "tmp_cluster_per_point_selection_table")
    
    logging_service.show_current_parameter_set()
    
    gridpoints = grid_service.generate_grid(dycast_parameters)

    day = dycast_parameters.startdate
    delta = datetime.timedelta(days=1)

    while day <= dycast_parameters.enddate:
    
        setup_tmp_daily_case_table_for_date(dycast_parameters, case_table_name, tmp_daily_case_table, day, cur, conn)
        daily_case_count = get_daily_case_count(tmp_daily_case_table, day, cur, conn)

        if daily_case_count >= case_threshold:
            st = time.time()
            logging.info("Starting daily_risk for {0}".format(day))
            points_above_threshold = 0

            for point in gridpoints:
                vector_count = get_vector_count_for_point(dycast_parameters, tmp_daily_case_table, point, system_coordinate_system, cur, conn)
                if vector_count >= case_threshold:
                    points_above_threshold += 1
                    insert_cases_in_cluster_table(dycast_parameters, tmp_cluster_per_point_selection_table, tmp_daily_case_table, point, system_coordinate_system, cur, conn)
                    results = cst_cs_ct_wrapper(dycast_parameters, cur, conn)
                    close_pairs = results[0][0]
                    close_space = results[1][0] - close_pairs
                    close_time = results[2][0] - close_pairs
                    result2 = nmcm_wrapper(vector_count, close_pairs, close_space, close_time, cur, conn)
                    insert_result(day, point.x, point.y, vector_count, close_pairs, close_time, close_space, result2[0][0], cur, conn)

            logging.info("Finished daily_risk for {0}: done {1} points".format(day, len(gridpoints)))
            logging.info("Total points above threshold of {0}: {1}".format(case_threshold, points_above_threshold))
            logging.info("Time elapsed: {:.0f} seconds".format(time.time() - st))
        else:
            logging.info("Amount of cases for {0} lower than threshold {1}: {2}, skipping.".format(day, case_threshold, daily_case_count))

        day += delta


def setup_tmp_daily_case_table_for_date(dycast_parameters, case_table_name, tmp_daily_case_table_name, riskdate, cur, conn):
    days_prev = dycast_parameters.temporal_domain
    enddate = riskdate
    startdate = riskdate - datetime.timedelta(days=(days_prev))
    querystring = "TRUNCATE " + tmp_daily_case_table_name + "; INSERT INTO " + tmp_daily_case_table_name + " SELECT * from " + case_table_name + " where report_date >= %s and report_date <= %s"
    try:
        cur.execute(querystring, (startdate, enddate))
    except Exception as e:
        conn.rollback()
        logging.error("Something went wrong when setting up tmp_daily_case_selection table: " + str(riskdate))
        logging.error(e)
        raise
    conn.commit()

def get_daily_case_count(tmp_daily_case_table_name, riskdate, cur, conn):
    query = "SELECT COUNT(*) FROM {0}".format(tmp_daily_case_table_name)
    try:
        cur.execute(query)
    except Exception as e:
        conn.rollback()
        logging.error("Something went wrong when getting count from tmp_daily_case_selection table: " + str(riskdate))
        logging.error(e)
        raise
    result_count = cur.fetchone()
    return result_count[0]

def get_vector_count_for_point(dycast_parameters, table_name, point, system_coordinate_system, cur, conn):
    sd = dycast_parameters.spatial_domain
    querystring = "SELECT count(*) from \"" + table_name + "\" a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s" 
    try:
        cur.execute(querystring, (point.x, point.y, system_coordinate_system, sd))
    except Exception as e:
        conn.rollback()
        logging.error("Can't select vector count")
        logging.error(e)
        sys.exit()
    new_row = cur.fetchone()
    return new_row[0]

def insert_cases_in_cluster_table(dycast_parameters, tmp_cluster_table_name, tmp_daily_case_table_name, point, system_coordinate_system, cur, conn):
    sd = dycast_parameters.spatial_domain
    querystring = "TRUNCATE " + tmp_cluster_table_name + "; INSERT INTO " + tmp_cluster_table_name + " SELECT * from " + tmp_daily_case_table_name + " a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s"
    try:
        cur.execute(querystring, (point.x, point.y, system_coordinate_system, sd))
    except Exception, inst:
        conn.rollback()
        logging.error("Something went wrong at point: " + str(point))
        logging.error(inst)
        sys.exit(1)
    conn.commit()

def cst_cs_ct_wrapper(dycast_parameters, cur, conn):
    close_in_space = dycast_parameters.close_in_space
    close_in_time = dycast_parameters.close_in_time

    querystring = "SELECT * FROM cst_cs_ct(%s, %s)"
    try:
        cur.execute(querystring, (close_in_space, close_in_time))
    except Exception, inst:
        conn.rollback()
        logging.error("can't select cst_cs_ct function")
        logging.error(inst)
        sys.exit()
    return cur.fetchall()

def nmcm_wrapper(num_birds, close_pairs, close_space, close_time, cur, conn):
    querystring = "SELECT * FROM nmcm(%s, %s, %s, %s)"
    try:
        cur.execute(querystring, (num_birds, close_pairs, close_space, close_time))
    except Exception, inst:
        conn.rollback()
        logging.error("can't select nmcm function")
        logging.error(inst)
        sys.exit()
    return cur.fetchall()

def insert_result(riskdate, latitude, longitude, num_birds, close_pairs, close_time, close_space, nmcm, cur, conn):
    querystring = "INSERT INTO risk (risk_date, lat, long, num_birds, close_pairs, close_space, close_time, nmcm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        # Be careful of the ordering of space and time in the db vs the txt file
        cur.execute(querystring, (riskdate, latitude, longitude, num_birds, close_pairs, close_space, close_time, nmcm))
    except Exception, inst:
        conn.rollback()
        logging.error("couldn't insert effects_poly risk")
        logging.error(inst)
        return 0
    conn.commit()




##########################################################################
##########################################################################