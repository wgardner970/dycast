import shutil
import sys
import os
import logging
from time import gmtime, strftime
import conversion_service
import config_service

try:
    import dbf
except ImportError:
    logging.error("Couldn't import dbf library in path: %s", sys.path)
    sys.exit()

CONFIG = config_service.get_config()


##########################################################################
# functions for exporting results:
##########################################################################

def export_risk(startdate, enddate, format = "dbf", path = None, export_prefix = None):
    # Quick and dirty solution
    if (format != "dbf" and format != "txt"):
        logging.error("Incorrect export format: %s", format)
        return 1

    if path == None:
        path = export_location = CONFIG.get("system", "export_directory")

    # dates are objects, not strings
    startdate_string = conversion_service.get_string_from_date_object(startdate)
    enddate_string = conversion_service.get_string_from_date_object(enddate)

    export_time = strftime("%Y-%m-%d__%H-%M-%S", gmtime())
    filename = export_time + "_risk" + startdate_string + "--" + enddate_string + "." + format
    if export_prefix:
        filename = export_prefix + filename
    filepath = os.path.join(path, filename)


    logging.info("Exporting risk for: %s - %s", startdate_string, enddate_string)
    query = "SELECT risk_date, lat, long, num_birds, close_pairs, close_space, close_time, nmcm FROM risk WHERE risk_date >= %s AND risk_date <= %s"

    try:
        cur.execute(query, (startdate, enddate))
    except Exception, e:
        conn.rollback()
        logging.error(e)
        raise

    if cur.rowcount == 0:
        logging.info("No risk found for the provided dates: %s - %s", startdate_string, enddate_string)
        logging.info("Exiting...")
        sys.exit(0)

    rows = cur.fetchall()

    table_content = file_service.TableContent()

    if format == "txt":
        header = get_header_as_string()
        table_content.set_header(header)

        body = get_rows_as_string(rows)
        table_content.set_body(body)
    else: # dbf
        dbf_out = init_dbf_out(filepath)
        write_rows_to_dbf(dbf_out, rows)
        dbf_close(dbf_out)


    file_service.save_file(table_content.get_content(), filepath)


def get_header_as_string():
    return "risk_date\tlat\tlong\tnumber_of_cases\tclose_pairs\tclose_time\tclose_space\tp_value"    

def get_rows_as_string(rows):
    string = ""
    for row in rows:
        [date, lat, long, num_birds, close_pairs, close_space, close_time, monte_carlo_p_value] = row
        string = string + "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format(date, lat, long, num_birds, close_pairs, close_time, close_space, monte_carlo_p_value)
    return string


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
    