import logging
import sys
try:
    import psycopg2
except ImportError:
    logging.error("Couldn't import psycopg2 library in path: %s", sys.path)
    sys.exit()
from application.services import config_service

CONFIG = config_service.get_config()


def init_psycopg_db():
    dsn = get_dsn()
    try:
        conn = psycopg2.connect(dsn)
    except Exception:
        logging.exception("Unable to connect to database")
        sys.exit()
    cur = conn.cursor()
    return cur, conn


def get_dsn():
    db_instance_name = CONFIG.get("database", "db_instance_name")
    user = CONFIG.get("database", "user")
    password = CONFIG.get("database", "password")
    host = CONFIG.get("database", "host")
    port = CONFIG.get("database", "port")

    return "dbname='" + db_instance_name + "' user='" + user + \
        "' password='" + password + "' host='" + host + \
        "' port='" + port + "'"
