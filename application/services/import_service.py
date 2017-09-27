import logging
import sys
from application.services import config_service
from application.services import file_service
from application.services import database_service
from application.models.enums import enums


CONFIG = config_service.get_config()


##########################################################################
# functions for loading data:
##########################################################################


def load_case_files(dycast_parameters):
    logging.info("Loading files: %s", dycast_parameters.files_to_import)
    system_coordinate_system = CONFIG.get("dycast", "system_coordinate_system")
    cur, conn = database_service.init_db()

    for filepath in dycast_parameters.files_to_import:
        try:
            logging.info("Loading file: %s", filepath)
            load_case_file(dycast_parameters, filepath, system_coordinate_system, cur, conn)
        except Exception:
            logging.exception("Could not load file: %s", filepath)
            sys.exit(1)


def load_case_file(dycast_parameters, filename, system_coordinate_system, cur, conn):
    lines_read = 0
    lines_processed = 0
    lines_loaded = 0
    lines_skipped = 0
    location_type = ""

    try:
        input_file = file_service.read_file(filename)
    except Exception:
        logging.exception("Could not read file: %s", filename)
        sys.exit(1)

    for line_number, line in enumerate(input_file):
        line = remove_trailing_newline(line)
        if line_number == 0:
            header_count = line.count("\t") + 1
            if header_count == 4:
                location_type = enums.Location_type.LAT_LONG
            elif header_count == 3:
                location_type = enums.Location_type.GEOMETRY
            else:
                logging.error(
                    "Incorrect column count: %s, exiting...", header_count)
                sys.exit(1)
            logging.info("Loading cases as location type: %s",
                         enums.Location_type(location_type).name)
        else:
            lines_read += 1
            result = 0
            try:
                result = load_case(dycast_parameters, line, location_type, system_coordinate_system, cur, conn)
            except Exception:
                raise

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
    logging.info("Processed %s of %s lines, %s loaded, %s duplicate IDs skipped",
                 lines_processed, lines_read, lines_loaded, lines_skipped)
    return lines_read, lines_processed, lines_loaded, lines_skipped


def load_case(dycast_parameters, line, location_type, system_coordinate_system, cur, conn):
    dead_birds_table_projected = CONFIG.get("database", "dead_birds_table_projected")
    user_coordinate_system = str(dycast_parameters.srid_of_cases)

    if location_type not in (enums.Location_type.LAT_LONG, enums.Location_type.GEOMETRY):
        logging.error("Wrong value for 'location_type', exiting...")
        sys.exit(1)

    if location_type == enums.Location_type.LAT_LONG:
        if not user_coordinate_system:
            raise ValueError(
                "Parameter 'user_coordinate_system' cannot be undefined when loading cases with lat/long locations")
        try:
            (case_id, report_date_string, lon, lat) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count(location_type, line)

        querystring = "INSERT INTO " + dead_birds_table_projected + \
            " VALUES (%s, %s, ST_Transform(ST_GeomFromText('POINT(" + lon + " " + \
                      lat + ")', " + user_coordinate_system + \
            "), CAST (%s AS integer)))"

    else:
        try:
            (case_id, report_date_string, geometry) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count(location_type, line)
        querystring = "INSERT INTO " + dead_birds_table_projected + \
            " VALUES (%s, %s, ST_Transform(Geometry('" + \
                      geometry + "'), CAST (%s AS integer)))"

    try:
        cur.execute(querystring, (case_id, report_date_string, system_coordinate_system))
    except Exception, e:
        conn.rollback()
        if str(e).startswith("duplicate key"):
            logging.debug(
                "Couldn't insert duplicate case key %s, skipping...", case_id)
            return -1
        else:
            raise
    conn.commit()
    return case_id


def fail_on_incorrect_count(location_type, line):
    logging.error("Incorrect number of fields for 'location_type': %s",
                  enums.Location_type(location_type).name)
    logging.error(line.rstrip())
    sys.exit(1)

def remove_trailing_newline(line):
    return line.strip()