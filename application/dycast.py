# Dycast functions

# dist_margs means "distribution marginals" and is the result of the
# monte carlo simulations.  See Theophilides et al. for more information

import sys
import os
import inspect
import shutil
import datetime
import time
import fileinput
import ConfigParser
import logging
from services import debug_service
from services import logging_service
from services import config_service
from services import grid_service
from services import conversion_service
from services import file_service
from models.enums import enums

debug_service.enable_debugger()

APPLICATION_ROOT = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
lib_dir = os.path.join(APPLICATION_ROOT, 'libs')
sys.path.append(lib_dir)
sys.path.append(os.path.join(lib_dir, "psycopg2"))
sys.path.append(os.path.join(lib_dir, "dbfpy"))

try:
    import dbf
except ImportError:
    logging.error("Couldn't import dbf library in path:", sys.path)
    sys.exit()

try:
    import psycopg2
except ImportError:
    logging.error("Couldn't import psycopg2 library in path:", sys.path)
    sys.exit()


CONFIG = config_service.get_config()

conn = 0
cur = 0

# TODO: find a more appropriate way to initialize these
sd = 1
cs = 1
ct = 1
td = 1
threshold = 1

# postgresql database connection information and table names
dsn = "x"
dead_birds_table_unprojected = "x"
dead_birds_table_projected = "x"




def read_config(filename, config_object=None):
    # All dycast objects must be initialized from a config file, therefore,
    # all dycast applications must include the name of a config file, or they
    # must use the default, which is dycast.config in the current directory

    config = config_object

    global dbname
    global user
    global password
    global host
    global dsn
    global dead_birds_filename
    global dead_birds_dir
    global risk_file_dir
    global lib_dir
    global dead_birds_table_unprojected
    global dead_birds_table_projected
    global tmp_daily_case_table
    global tmp_cluster_per_point_selection_table
    global sd
    global td
    global cs
    global ct
    global threshold
    global system_coordinate_system

    if not config:
        config = ConfigParser.SafeConfigParser(os.environ)
        config.read(filename)

    dbname = config.get("database", "dbname")
    user = config.get("database", "user")
    password = config.get("database", "password")
    host = config.get("database", "host")
    port = config.get("database", "port")
    dsn = "dbname='" + dbname + "' user='" + user + \
        "' password='" + password + "' host='" + host + \
        "' port='" + port + "'"

    if sys.platform == 'win32':
        dead_birds_dir = config.get(
            "system", "windows_dycast_path") + config.get("system", "dead_birds_subdir")
        risk_file_dir = config.get(
            "system", "windows_dycast_path") + config.get("system", "risk_file_subdir")
    else:
        dead_birds_dir = config.get(
            "system", "unix_dycast_path") + config.get("system", "dead_birds_subdir")
        risk_file_dir = config.get(
            "system", "unix_dycast_path") + config.get("system", "risk_file_subdir")

    dead_birds_table_unprojected = config.get(
        "database", "dead_birds_table_unprojected")
    dead_birds_table_projected = config.get(
        "database", "dead_birds_table_projected")
    tmp_daily_case_table = config.get("database", "tmp_daily_case_table")
    tmp_cluster_per_point_selection_table = config.get(
        "database", "tmp_cluster_per_point_selection_table")

    sd = float(config.get("dycast", "spatial_domain"))
    cs = float(config.get("dycast", "close_in_space"))
    ct = int(config.get("dycast", "close_in_time"))
    td = int(config.get("dycast", "temporal_domain"))
    threshold = int(config.get("dycast", "bird_threshold"))

    system_coordinate_system = config.get("dycast", "system_coordinate_system")


def init_db(config=None):
    global cur, conn
    try:
        conn = psycopg2.connect(dsn)
    except Exception, inst:
        logging.error("Unable to connect to database")
        logging.error(inst)
        sys.exit()
    cur = conn.cursor()


##########################################################################
# functions for loading data:
##########################################################################


def load_case(line, location_type, user_coordinate_system):
    if location_type not in (enums.Location_type.LAT_LONG, enums.Location_type.GEOMETRY):
        logging.error("Wrong value for 'location_type', exiting...")
        sys.exit(1)

    if location_type == enums.Location_type.LAT_LONG:
        try:
            (case_id, report_date_string, lon, lat, species) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count()
        querystring = "INSERT INTO " + dead_birds_table_projected + " VALUES (%s, %s, %s, ST_Transform(ST_GeomFromText('POINT(" + lon + " " + lat + ")', " + user_coordinate_system + "), CAST (%s AS integer)))"

    else:
        try:
            (case_id, report_date_string, geometry, species) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count()
        querystring = "INSERT INTO " + dead_birds_table_projected + " VALUES (%s, %s, %s, ST_Transform(Geometry('" + geometry + "'), CAST (%s AS integer)))"

    try:
        cur.execute(querystring, (case_id, report_date_string, species, system_coordinate_system))
    except Exception, inst:
        conn.rollback()
        if str(inst).startswith("duplicate key"):
            logging.debug("Couldn't insert duplicate case key %s, skipping...", case_id)
            return -1
        else:
            logging.warning("Couldn't insert case record")
            logging.warning(inst)
            return 0
    conn.commit()
    return case_id

def fail_on_incorrect_count(location_type, line):
    logging.error("Incorrect number of fields for 'location_type' %s: %s, exiting...", (location_type, line.rstrip()))
    sys.exit(1)    


##########################################################################
# functions for exporting results:
##########################################################################

def export_risk(startdate, enddate, format = "dbf", path = None):
    # Quick and dirty solution
    if (format != "dbf" and format != "txt"):
        logging.error("Incorrect export format: %s", format)
        return 1

    # dates are objects, not strings
    startdate_string = conversion_service.get_string_from_date_object(startdate)
    enddate_string = conversion_service.get_string_from_date_object(enddate)

    if path == None:
        path = risk_file_dir + "/tmp/"
        using_tmp = True
    else:
        using_tmp = False

    filename = "risk" + startdate_string + "--" + enddate_string + "." + format
    filepath = os.path.join(path, filename)

    if format == "txt":
        txt_out = init_txt_out(filepath)
    else:   # dbf
        dbf_out = init_dbf_out(filepath)

    logging.info("Exporting risk for: %s - %s", startdate_string, enddate_string)
    query = "SELECT risk_date, lat, long, num_birds, close_pairs, close_space, close_time, nmcm FROM risk WHERE risk_date >= %s AND risk_date <= %s"

    try:
        cur.execute(query, (startdate, enddate))
    except Exception, e:
        conn.rollback()
        logging.error(e)
        raise

    rows = cur.fetchall()

    if format == "txt":
        write_rows_to_txt(filepath, rows)
    else: # dbf
        write_rows_to_dbf(dbf_out, rows)
        dbf_close()
        
    if using_tmp:
        outbox_tmp_to_new(risk_file_dir, filename)    # Move finished file to "new"


def init_txt_out(filepath):
    dirname = os.path.dirname(filepath)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filepath, "w") as text_file:
        text_file.write("risk_date\tlat\tlong\tnumber_of_cases\tclose_pairs\tclose_time\tclose_space\tp_value\n")

def init_dbf_out(filename):
    dbfn = dbf.Dbf(filename, new=True)
    dbfn.addField(
        ("LAT",'N',8,0),
        ("LONG",'N',8,0),
        ("COUNTY",'N',3,0),
        ("RISK",'N',1,0),
        ("DISP_VAL",'N',8,6),
        ("RISK_DATE",'D',8)
    )

    # TODO: make this an object
    return dbfn

def write_rows_to_txt(filepath, rows):
    with open(filepath, "a") as text_file:
        for row in rows:
            [date, lat, long, num_birds, close_pairs, close_space, close_time, monte_carlo_p_value] = row
            text_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (date, lat, long, num_birds, close_pairs, close_time, close_space, monte_carlo_p_value))

def write_rows_to_dbf(dbfn, rows):
    for row in rows:
        [date, lat, long, num_birds, close_pairs, close_space, close_time, monte_carlo_p_value] = row
        rec = dbfn.newRecord()
        
        rec['LAT'] = lat
        rec['LONG'] = long
        if nmcm > 0:
            rec['RISK'] = 1
        else:
            rec['RISK'] = 0
        rec['DISP_VAL'] = nmcm
        rec['DATE'] = (int(date.strftime("%Y")), int(date.strftime("%m")), int(date.strftime("%d")))

        rec.store()

def dbf_close(dbfn):
    dbfn.close()

##########################################################################
# functions for uploading and downloading files:
##########################################################################

# The outbox functions are based on the Maildir directory structure.
# The use of /tmp/ allows to spend a lot of time writing to the file,
# if necessary, and then atomically move it to a /new/ directory, where
# other scripts can find it.  In this way, the /new/ directory will never
# include incomplete files that are still being written.

def outbox_tmp_to_new(outboxpath, filename):
    shutil.move(outboxpath + "/tmp/" + filename, outboxpath + "/new/" + filename)

def outbox_new_to_cur(outboxpath, filename):
    shutil.move(outboxpath + "/new/" + filename, outboxpath + "/cur/" + filename)

def backup_birds():
    (stripped_file, ext) = os.path.splitext(dead_birds_filename)
    new_file = stripped_file + "_" + datetime.date.today().strftime("%Y-%m-%d") + ".tsv"
    shutil.copyfile(dead_birds_dir + dead_birds_filename, dead_birds_dir + new_file)

def load_case_file(user_coordinate_system, filename = None):
    if filename == None:
        filename = dead_birds_dir + os.sep + dead_birds_filename
    lines_read = 0
    lines_processed = 0
    lines_loaded = 0
    lines_skipped = 0
    location_type = ""

    try:
        input_file = file_service.read_file(filename)
    except Exception, e:
        logging.error(e)
        sys.exit(1)

    for line_number, line in enumerate(input_file):
        if line_number == 0:
            header_count = line.count("\t") + 1
            if header_count == 5:
                location_type = enums.Location_type.LAT_LONG
            elif header_count == 4:
                location_type = enums.Location_type.GEOMETRY
            else:
                logging.error("Incorrect column count: %s, exiting...", header_count)
                sys.exit(1)
        else:
            lines_read += 1
            result = 0
            result = load_case(line, location_type, user_coordinate_system)

            # If result is a bird ID or -1 (meaning duplicate) then:
            if result:
                lines_processed += 1
                if result == -1:
                    lines_skipped += 1
                else:
                    lines_loaded += 1
            else:
                logging.error("No result after loading case: ")
                logging.error(line)

    logging.info("Case load complete: %s", filename)
    logging.info("Processed %s of %s lines, %s loaded, %s duplicate IDs skipped", lines_processed, lines_read, lines_loaded, lines_skipped)
    return lines_read, lines_processed, lines_loaded, lines_skipped


def upload_new_risk(outboxpath = None):
    if outboxpath == None:
        outboxpath = risk_file_dir
    newdir = outboxpath + "/new/"
    for file in os.listdir(newdir):
         if ((os.path.splitext(file))[1] == '.dbf'):
            logging.debug("uploading %s and moving it from new/ to cur/", file)
            upload_risk(newdir, file)
            outbox_new_to_cur(outboxpath, file)


##########################################################################
# functions for generating risk:
##########################################################################

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

def get_vector_count_for_point(bird_tab, point):
    querystring = "SELECT count(*) from \"" + bird_tab + "\" a where st_distance(a.location,ST_GeomFromText('POINT(%s %s)',%s)) < %s" 
    try:
        cur.execute(querystring, (point.x, point.y, system_coordinate_system, sd))
    except Exception as e:
        conn.rollback()
        logging.error("can't select bird count")
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


def daily_risk(startdate, enddate, user_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y):
    logging_service.show_current_parameter_set()
    
    gridpoints = grid_service.generate_grid(user_coordinate_system, system_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y)

    day = startdate
    delta = datetime.timedelta(days=1)

    while day <= enddate:

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
    
##########################################################################
##########################################################################
