from fastapi import FastAPI, HTTPException
from sel.utils import InternalServerError, InvalidClientInput, NotFound
from sel.schema_reader import SchemaError
import functools


def rewriter(route_handler):
    """ Rewriter any exception to HTTPException """

    @functools.wraps(route_handler)
    def wrapper(*args, **kwargs):
        """ Take parameters from fastapi """

        try:
            return route_handler(*args, **kwargs)
        except Exception as exc:
            raise to_httpexception(exc)

    return wrapper


def to_httpexception(exc):
    if isinstance(exc, InternalServerError):
        return HTTPException(status_code=500, detail=exc.message)
    elif isinstance(exc, InvalidClientInput):
        return HTTPException(status_code=400, detail=exc.message)
    elif isinstance(exc, NotFound):
        return HTTPException(status_code=404, detail=exc.message)
    elif isinstance(exc, SchemaError):
        return HTTPException(status_code=400, detail=exc.message)
    else:
        return HTTPException(status_code=500, detail=str(exc))
