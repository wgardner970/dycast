import sys
import os
import logging
from time import gmtime, strftime
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
    if export_format != "txt":
        logging.error("Incorrect export format: %s", export_format)
        return 1

    if export_directory is None:
        export_directory = CONFIG.get("system", "export_directory")

    # dates are objects, not strings
    startdate_string = conversion_service.get_string_from_date_object(startdate)
    enddate_string = conversion_service.get_string_from_date_object(enddate)

    export_time = strftime("%Y-%m-%d__%H-%M-%S", gmtime())
    filename = export_time + "_risk" + startdate_string + "--" + enddate_string + "." + export_format
    if export_prefix:
        filename = export_prefix + filename
    filepath = os.path.join(export_directory, filename)


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

    if export_format == "txt":
        header = get_header_as_string()
        table_content.set_header(header)

        body = get_rows_as_string(rows)
        table_content.set_body(body)


    file_service.save_file(table_content.get_content(), filepath)


def get_header_as_string():
    return "risk_date\tlat\tlong\tnumber_of_cases\tclose_pairs\tclose_time\tclose_space\tp_value"    

def get_rows_as_string(rows):
    string = ""
    for row in rows:
        [date, lat, lon, num_birds, close_pairs, close_space, close_time, monte_carlo_p_value] = row
        string = string + "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format(date, lat, lon, num_birds, close_pairs, close_time, close_space, monte_carlo_p_value)
    return string
