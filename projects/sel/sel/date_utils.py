import re
import time
import datetime
from dateutil.relativedelta import relativedelta
from .utils import InvalidClientInput


DATE_FORMAT = {
    "year" :   "%Y",
    "month":   "%Y-%m",
    "day":     "%Y-%m-%d",
    "hour":    "%Y-%m-%d %H",
    "minute":  "%Y-%m-%d %H:%M",
    "second":  "%Y-%m-%d %H:%M:%S",
}

ELASTIC_TO_DATETIME_FORMAT_TABLE = [
    {"from": "yyyy", "to": r"%Y"},
    {"from": "MM",   "to": r"%m"},
    {"from": "dd",   "to": r"%d"},
    {"from": "HH",   "to": r"%H"},
    {"from": "mm",   "to": r"%M"},
    {"from": "ss",   "to": r"%S"}
]

ELASTIC_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm||yyyy-MM-dd HH||yyyy-MM-dd||yyyy-MM||yyyy"


def elastic_format_to_datetime_format(str_format):
    """
    Convert Elastic date format to datetime format
    """
    for element in ELASTIC_TO_DATETIME_FORMAT_TABLE:
        str_format = re.sub(element["from"], element["to"], str_format)
    return str_format

INTERVAL_SHORTCUT = {
    "y": "year",
    "q": "quarter",
    "M": "month",
    "w": "week",
    "d": "day",
    "h": "hour",
    "m": "minute",
    "s": "second",
}
def shortcurt_to_interval(interval):
    interval = re.sub(r"^[0-9]*", "", interval)
    return INTERVAL_SHORTCUT.get(interval, interval)

INTERVAL_DATE_FORMAT = {
    "year" :   DATE_FORMAT["year"],
    "quarter": DATE_FORMAT["month"],
    "month":   DATE_FORMAT["month"],
    "week":    DATE_FORMAT["day"],
    "day":     DATE_FORMAT["day"],
    "hour":    DATE_FORMAT["hour"],
    "minute":  DATE_FORMAT["minute"],
    "second":  DATE_FORMAT["second"],
}
def date_format_from_interval(interval):
    """
    Avoid to display too low date element if not necessary.
    Such aggreg: date interval month, does not display day element
    """
    interval = shortcurt_to_interval(interval)
    return INTERVAL_DATE_FORMAT[interval]


DATE_DELTA = {
    "year" :   relativedelta(years=+1),
    "quarter": relativedelta(months=+3),
    "month":   relativedelta(months=+1),
    "week":    relativedelta(days=+7),
    "day":     relativedelta(days=+1),
    "hour":    relativedelta(hours=+1),
    "minute":  relativedelta(minutes=+1),
    "second":  relativedelta(seconds=+1),
}
def interval_to_delta_time(interval):
    """ Get delta time to the end of bucket period based on interval """
    interval = shortcurt_to_interval(interval)
    return DATE_DELTA[interval] - relativedelta(microseconds=+1000)


LAST_ELM_TABLE = [
    {"value": r"%S", "delta": lambda v: relativedelta(seconds=v)},
    {"value": r"%M", "delta": lambda v: relativedelta(minutes=v)},
    {"value": r"%H", "delta": lambda v: relativedelta(hours=v)},
    {"value": r"%d", "delta": lambda v: relativedelta(days=v)},
    {"value": r"%m", "delta": lambda v: relativedelta(months=v)},
    {"value": r"%Y", "delta": lambda v: relativedelta(years=v)},
]
def date_add_to_last_element(date, value, date_format):
    """
    Add <value> to the last date element
    Ex. 2018-01-15 will add <value> to the day, such 2018-01-16
    Ex. 2018-01 will add <value> to the month, such 2018-02
    """
    for element in LAST_ELM_TABLE:
        if element["value"] in date_format:
            return date + element["delta"](value)

    raise InvalidClientInput(f"Invalid date format: '{date}'")

def str_date_to_datetime(str_date):
    for date_format in DATE_FORMAT.values():
        try:
            return datetime.datetime.strptime(str_date, date_format), date_format
        except ValueError:
            pass

    raise InvalidClientInput(f"Invalid date format: {str_date}")


def month_to_datetime(date):
    return datetime.datetime.strptime(date, "%Y-%m")

def to_string(d):
    return d.strftime("%Y-%m-%d")

def first_day_of_month(d):
    return d.replace(day=1)

def first_day_of_next_month(d):
    # this will never fail
    next_month = d.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=(next_month.day - 1))

def add_months(d, months):
    for i in range(0, months):
        d = first_day_of_next_month(d)
    return d

def remove_months(d, months):
    for i in range(0, months):
        d = first_day_of_month(first_day_of_month(d) - datetime.timedelta(days=1))
    return d

def last_day_of_month(d):
    return add_months(d, 1) - datetime.timedelta(1)

def current_month():
    return datetime.datetime.now().strftime("%Y-%m")

def to_timestamp(dt):
    """ datetime to timestamp """
    return time.mktime(dt.timetuple())

def from_timestamp(ts):
    """ datetime from timestamp """
    return datetime.datetime.fromtimestamp(ts)
