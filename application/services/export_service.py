import sys
import os
import logging
from time import strftime
from application.services import conversion_service
from application.services import config_service
from application.services import database_service
from application.services import file_service


CONFIG = config_service.get_config()


##########################################################################
# functions for exporting results:
##########################################################################

def export_risk(dycast_parameters):
    startdate = dycast_parameters.startdate
    enddate = dycast_parameters.enddate
    export_directory = dycast_parameters.export_directory
    export_prefix = dycast_parameters.export_prefix
    export_format = dycast_parameters.export_format

    cur, conn = database_service.init_db()

    # Quick and dirty solution
    if export_format != "tsv" and export_format != "csv":
        logging.error("Incorrect export format: %s", export_format)
        return 1
    else:
        separator = get_separator(export_format)

    if export_directory is None:
        export_directory = CONFIG.get("system", "export_directory")

    # dates are objects, not strings
    startdate_string = conversion_service.get_string_from_date_object(startdate)
    enddate_string = conversion_service.get_string_from_date_object(enddate)

    export_time = strftime("%Y-%m-%d__%H-%M-%S")
    filename = "exported_{0}__risk_{1}--{2}.{3}".format(export_time, startdate_string, enddate_string, export_format)
    if export_prefix:
        filename = export_prefix + filename
    filepath = os.path.join(export_directory, filename)


    logging.info("Exporting risk for: %s - %s", startdate_string, enddate_string)
    query = "SELECT risk_date, lat, long, num_birds, close_pairs, close_space, close_time, nmcm FROM risk WHERE risk_date >= %s AND risk_date <= %s"

    try:
        cur.execute(query, (startdate, enddate))
    except Exception:
        conn.rollback()
        logging.exception("Failed to select risk data")
        raise

    if cur.rowcount == 0:
        logging.info("No risk found for the provided dates: %s - %s", startdate_string, enddate_string)
        logging.info("Exiting...")
        sys.exit(0)

    rows = cur.fetchall()

    table_content = file_service.TableContent()

    header = get_header_as_string(separator)
    table_content.set_header(header)

    body = get_rows_as_string(rows, separator)
    table_content.set_body(body)


    file_service.save_file(table_content.get_content(), filepath)


def get_header_as_string(separator):
    return "risk_date{0}lat{0}long{0}number_of_cases{0}close_pairs{0}close_time{0}close_space{0}p_value".format(separator)  

def get_rows_as_string(rows, separator):
    string = ""
    for row in rows:
        [date, lat, lon, num_birds, close_pairs, close_space, close_time, monte_carlo_p_value] = row
        string = string + "{0}{8}{1}{8}{2}{8}{3}{8}{4}{8}{5}{8}{6}{8}{7}\n".format(date, lat, lon, num_birds, close_pairs, close_time, close_space, monte_carlo_p_value, separator)
    return string

def get_separator(file_format):
    if file_format == "tsv":
        return "\t"
    elif file_format == "csv":
        return ","
    else:
        raise ValueError("Invalid file format requested")
