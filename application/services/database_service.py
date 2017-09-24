import logging
import sys
try:
    import psycopg2
except ImportError:
    logging.error("Couldn't import psycopg2 library in path:", sys.path)
    sys.exit()
from application.services import config_service

CONFIG = config_service.get_config()


def init_db(config=None):
    dsn = get_dsn()
    try:
        conn = psycopg2.connect(dsn)
    except Exception, inst:
        logging.error("Unable to connect to database")
        logging.error(inst)
        sys.exit()
    cur = conn.cursor()
    return cur, conn


def get_dsn():
    dbname = CONFIG.get("database", "dbname")
    user = CONFIG.get("database", "user")
    password = CONFIG.get("database", "password")
    host = CONFIG.get("database", "host")
    port = CONFIG.get("database", "port")

    return "dbname='" + dbname + "' user='" + user + \
        "' password='" + password + "' host='" + host + \
        "' port='" + port + "'"
