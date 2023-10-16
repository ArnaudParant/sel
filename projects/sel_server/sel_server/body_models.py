"""
Input body models definitions
"""
from typing import List, Union

# pylint: disable=no-name-in-module
from pydantic import BaseModel


class QueryString(BaseModel):
    query: str = None
    meta: dict = None
    extended: dict = None


class QueryDict(BaseModel):
    query: dict = None
    aggregations: dict = None
    sort: list = None
    meta: dict = None
    extended: dict = None


Query = Union[QueryDict, QueryString]


class DownloadAggreg(BaseModel):
    base_aggregation: dict
    query: Union[dict, str] = None
    aggregations: dict = None
    extended: dict = None


class Scroll(BaseModel):
    query: Union[dict, str] = None
    sort: list = None
    meta: dict = None
    extended: dict = None
    cash_time: str
    scroll_id: str = None


class DeleteQuery(BaseModel):
    query: Union[dict, str] = None
    ids: List[str] = None
    undelete: bool = False


class ReallyDeleteQuery(BaseModel):
    query: Union[dict, str] = None
    ids: List[str] = None


class Subfields(BaseModel):
    fields: List[str]
    no_empty: bool = True


class QueryGenerator(BaseModel):
    query: Union[dict, str]
    aggregations: dict = None
    sort: list = None
    meta: dict = None
    extended: dict = None
    index_schema: dict = None
