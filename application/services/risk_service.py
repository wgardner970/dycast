import logging_service
import datetime
import time

from application.services import grid_service


CONFIG = config_service.get_config()


##########################################################################
# functions for generating risk:
##########################################################################

def generate_risk(dycast):
    tmp_daily_case_table = CONFIG.get("database", "tmp_daily_case_table")
    tmp_cluster_per_point_selection_table = CONFIG.get("database", "tmp_cluster_per_point_selection_table")
    
    logging_service.show_current_parameter_set()
    
    gridpoints = grid_service.generate_grid(dycast.user_coordinate_system, dycast.extent_min_x, dycast.extent_min_y, dycast.extent_max_x, dycast.extent_max_y)

    day = dycast.startdate
    delta = datetime.timedelta(days=1)

    while day <= dycast.enddate:

        setup_tmp_daily_case_table_for_date(tmp_daily_case_table, day, td)
        daily_case_count = get_daily_case_count(tmp_daily_case_table)

        if daily_case_count >= threshold:
            st = time.time()
            logging.info("Starting daily_risk for {0}".format(day))
            points_above_threshold = 0

            for point in gridpoints:
                vector_count = get_vector_count_for_point(tmp_daily_case_table, point)
                if vector_count >= threshold:
                    points_above_threshold += 1
                    insert_cases_in_cluster_table(tmp_cluster_per_point_selection_table, tmp_daily_case_table, point)
                    results = cst_cs_ct_wrapper()
                    close_pairs = results[0][0]
                    close_space = results[1][0] - close_pairs
                    close_time = results[2][0] - close_pairs
                    result2 = nmcm_wrapper(vector_count, close_pairs, close_space, close_time)
                    insert_result(day, point.x, point.y, vector_count, close_pairs, close_time, close_space, result2[0][0])

            logging.info("Finished daily_risk for {0}: done {1} points".format(day, len(gridpoints)))
            logging.info("Total points above threshold of {0}: {1}".format(threshold, points_above_threshold))
            logging.info("Time elapsed: {:.0f} seconds".format(time.time() - st))
        else:
            logging.info("Amount of cases for {0} lower than threshold {1}: {2}, skipping.".format(day, threshold, daily_case_count))

        day += delta


def setup_tmp_daily_case_table_for_date(tmp_daily_case_table_name, riskdate, days_prev):
    enddate = riskdate
    startdate = riskdate - datetime.timedelta(days=(days_prev))
    querystring = "TRUNCATE " + tmp_daily_case_table_name + "; INSERT INTO " + tmp_daily_case_table_name + " SELECT * from " + dead_birds_table_projected + " where report_date >= %s and report_date <= %s"
    try:
        cur.execute(querystring, (startdate, enddate))
    except Exception as e:
        conn.rollback()
        logging.error("Something went wrong when setting up tmp_daily_case_selection table: " + str(riskdate))
        logging.error(e)
        raise
    conn.commit()

def get_daily_case_count(tmp_daily_case_table_name):
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

def get_vector_count_for_point(table_name, point):
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

def insert_cases_in_cluster_table(tmp_cluster_table_name, tmp_daily_case_table_name, point):
    querystring = "TRUNCATE " + tmp_cluster_table_name + "; INSERT INTO " + tmp_cluster_table_name + " SELECT * from " + tmp_daily_case_table_name + " a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s"
    try:
        cur.execute(querystring, (point.x, point.y, system_coordinate_system, sd))
    except Exception, inst:
        conn.rollback()
        logging.error("Something went wrong at point: " + str(point))
        logging.error(inst)
        sys.exit(1)
    conn.commit()

def cst_cs_ct_wrapper():
    querystring = "SELECT * FROM cst_cs_ct(%s, %s)"
    try:
        cur.execute(querystring, (cs, ct))
    except Exception, inst:
        conn.rollback()
        logging.error("can't select cst_cs_ct function")
        logging.error(inst)
        sys.exit()
    return cur.fetchall()

def nmcm_wrapper(num_birds, close_pairs, close_space, close_time):
    querystring = "SELECT * FROM nmcm(%s, %s, %s, %s)"
    try:
        cur.execute(querystring, (num_birds, close_pairs, close_space, close_time))
    except Exception, inst:
        conn.rollback()
        logging.error("can't select nmcm function")
        logging.error(inst)
        sys.exit()
    return cur.fetchall()

def insert_result(riskdate, latitude, longitude, num_birds, close_pairs, close_time, close_space, nmcm):
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