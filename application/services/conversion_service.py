import logging
from datetime import datetime

def get_string_from_date_object(date):
    try:
        return date.strftime('%Y-%m-%d')
    except Exception:
        logging.exception("Could not convert date to string: %s", date)
        raise

def get_date_object_from_string(string):
    try:
        return datetime.strptime(string, '%Y-%m-%d').date()
    except Exception:
        logging.exception("Could not convert string to date: %s", string)
