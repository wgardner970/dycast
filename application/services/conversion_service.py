import logging

def get_string_from_date_object(date):
    try:
        return date.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(e)
        logging.error("Couldn't convert date to string: %s", date)
        raise