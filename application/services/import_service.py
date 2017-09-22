import logging
import sys
import config_service
from application.models.enums import enums

import file_service

CONFIG = config_service.get_config()
SYSTEM_COORDINATE_SYSTEM = CONFIG.get("dycast", "system_coordinate_system")


##########################################################################
# functions for loading data:
##########################################################################


def load_case_files(dycast_import):
    for filepath in dycast_import.files_to_import:
        try:
            logging.info("Loading file: %s", filepath)
            load_case_file(filepath, dycast_import)
        except Exception, e:
            logging.error("Could not load file: %s", filepath)
            logging.error(e)
            logging.error("Continuing...")


def load_case_file(filename, dycast_import):
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
                logging.error(
                    "Incorrect column count: %s, exiting...", header_count)
                sys.exit(1)
        else:
            lines_read += 1
            result = 0
            try:
                result = load_case(line, location_type, dycast_import)
            except Exception, e:
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


def load_case(line, location_type, dycast_import):
    dead_birds_table_projected = dycast_import.case_table_name
    user_coordinate_system = dycast_import.srid_of_cases
    cur = dycast_import.cur
    conn = dycast_import.conn

    if location_type not in (enums.Location_type.LAT_LONG, enums.Location_type.GEOMETRY):
        logging.error("Wrong value for 'location_type', exiting...")
        sys.exit(1)

    if location_type == enums.Location_type.LAT_LONG:
        if not user_coordinate_system:
            raise ValueError(
                "Parameter 'user_coordinate_system' cannot be undefined when loading cases with lat/long locations")
        try:
            (case_id, report_date_string, lon, lat, species) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count(location_type, line)
        querystring = "INSERT INTO " + dead_birds_table_projected + \
            " VALUES (%s, %s, %s, ST_Transform(ST_GeomFromText('POINT(" + lon + " " + \
                      lat + ")', " + user_coordinate_system + \
            "), CAST (%s AS integer)))"

    else:
        try:
            (case_id, report_date_string, geometry, species) = line.split("\t")
        except ValueError:
            fail_on_incorrect_count(location_type, line)
        querystring = "INSERT INTO " + dead_birds_table_projected + \
            " VALUES (%s, %s, %s, ST_Transform(Geometry('" + \
                      geometry + "'), CAST (%s AS integer)))"

    try:
        cur.execute(querystring, (case_id, report_date_string,
                                  species, SYSTEM_COORDINATE_SYSTEM))
    except Exception, inst:
        conn.rollback()
        if str(inst).startswith("duplicate key"):
            logging.debug(
                "Couldn't insert duplicate case key %s, skipping...", case_id)
            return -1
        else:
            logging.warning("Couldn't insert case record")
            logging.warning(inst)
            return 0
    conn.commit()
    return case_id


def fail_on_incorrect_count(location_type, line):
    logging.error("Incorrect number of fields for 'location_type' %s: %s, exiting...",
                  location_type, line.rstrip())
    sys.exit(1)
