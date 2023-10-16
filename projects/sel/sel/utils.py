# External deps
import functools
import traceback
import logging


class InternalServerError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class InvalidClientInput(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class NotFound(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


def set_if_exists(source, dest, keys):
    if source is not None:
        for key in keys:
            if key in source and source[key] is not None:
                dest[key] = source[key]
    return dest


def build_group(operator, items):
    items = [i for i in items if i]
    if len(items) == 1:
        return items[0]
    if len(items) > 1:
        return {"operator": operator, "items": items}
    return None


def get_lastest_sub_data(data):
    while "sub" in data:
        data = data["sub"]
    return data


def _detailor(exc):
    message = None

    if hasattr(exc, "info") \
       and exc.info \
       and isinstance(exc.info, dict) \
       and "error" in exc.info \
       and "root_cause" in exc.info["error"] \
       and exc.info["error"]["root_cause"] \
       and "reason" in exc.info["error"]["root_cause"][0]:
        message = exc.info["error"]["root_cause"][0]["reason"]

    logging.error(str(exc))
    logging.error(traceback.format_exc())

    if message is not None:
        return Exception(message + "\n"+ str(exc))

    return exc


def elastic_exception_detailor(handler):
    """ The decorator, take function """

    @functools.wraps(handler)
    def handler_wrapper(*args, **kwargs):
        """ Take function parameters """
        try:
            return handler(*args, **kwargs)
        except Exception as exc:
            raise _detailor(exc)

    return handler_wrapper
