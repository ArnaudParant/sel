"""
Response models definitions
"""
from typing import List, Union

# pylint: disable=no-name-in-module
from pydantic import BaseModel


class Hello(BaseModel):
    name: str
    message: str
    version: str


class Scroll(BaseModel):
    documents: List[dict]
    scroll_id: str


class Success(BaseModel):
    success: bool


class DownloadAggreg(BaseModel):
    buckets: List[dict]


class Search(BaseModel):
    results: dict
    warnings: List[str]


class GetOneDocument(BaseModel):
    document: dict


class DeleteDocuments(BaseModel):
    action: str
    count: int


class ReallyDeleteDocuments(BaseModel):
    count: int


class ListIndex(BaseModel):
    indexes: List[dict]


class Schema(BaseModel):
    index_schema: dict


class ListFields(BaseModel):
    fields: List[dict]


class SearchField(BaseModel):
    fields: List[dict]


class Subfields(BaseModel):
    fields: List[dict]


class Generator(BaseModel):
    warns: List[str]
    elastic_query: dict
    internal_query: dict
    query_data: dict
