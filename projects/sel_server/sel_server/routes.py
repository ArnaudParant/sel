import logging
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html

from sel import utils
from . import starter, response_models, body_models, exceptions


app = FastAPI(
    title="SEL Server",
    description="Simple Elastic Language Server, make ElasticSearch query easier",
    swagger="2.0",
    version="2.4.1",
    root_path=None,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.getLogger("urllib3").setLevel(logging.WARNING)


@app.get('/', response_model=response_models.Hello)
@exceptions.rewriter
def hello():
    return {
        "name": app.title,
        "message": "Hey! Listen. Check /docs for documentation",
        "version": app.version,
    }


@app.post('/scroll/{index}', response_model=response_models.Scroll)
@exceptions.rewriter
def scroll(index: str, query: body_models.Scroll = None):
    sel = starter.get_api()
    query = query.__dict__ if query else None
    return sel.scroll(index, query, query.pop("cash_time"), scroll_id=query.pop("scroll_id", None))


@app.delete('/clear-scroll/{scroll_id}', response_model=response_models.Success)
@exceptions.rewriter
def clear_scroll(scroll_id: str):
    sel = starter.get_api()
    sel.clear_scroll(scroll_id)
    return {"success": True}


@app.post('/download-aggreg/{index}', response_model=response_models.DownloadAggreg)
@exceptions.rewriter
def download_aggreg(index: str, query: body_models.DownloadAggreg):
    sel = starter.get_api()
    query = query.__dict__
    base_aggreg = query.pop("base_aggregation")
    lines = sel.download_aggreg(index, base_aggreg, query)
    return {"buckets": list(lines)}


@app.post('/search/{index}', response_model=response_models.Search)
@exceptions.rewriter
def search(index: str, query: body_models.Query = None):
    sel = starter.get_api()
    return sel.search(index, query.__dict__ if query else None)


@app.get('/document/{index}/{doc_id}', response_model=response_models.GetOneDocument)
@exceptions.rewriter
def get_one_document(index: str, doc_id: str):
    sel = starter.get_api()
    return {"document": sel.get_one_document(index, doc_id)}


@app.delete('/delete-documents/{index}', response_model=response_models.DeleteDocuments)
@exceptions.rewriter
def delete_documents(index: str, query: body_models.DeleteQuery):
    sel = starter.get_api()
    query = query.__dict__
    undelete = query.pop("undelete", False)
    return sel.delete_documents(index, query, undelete=undelete)


@app.delete(
    '/unsafe/really-delete-documents/{index}',
    response_model=response_models.ReallyDeleteDocuments
)
@exceptions.rewriter
def UNSAFE_really_delete_documents(index: str, query: body_models.ReallyDeleteQuery):
    sel = starter.get_api()
    count = sel.really_delete_documents(index, query.__dict__)
    return {"count": count}


@app.get('/list-index/{index}', response_model=response_models.ListIndex)
@app.get('/list-index', response_model=response_models.ListIndex)
@exceptions.rewriter
def list_index(index: str = None):
    sel = starter.get_api()
    return {"indexes": sel.list_index(index=index)}


@app.get('/schema/{index}', response_model=response_models.Schema)
@exceptions.rewriter
def schema(index: str):
    sel = starter.get_api()
    return {"index_schema": sel.get_schema(index)}


@app.get('/list-field/{index}', response_model=response_models.ListFields)
@exceptions.rewriter
def list_fields(index: str):
    sel = starter.get_api()
    return {"fields": sel.list_fields(index)}


@app.get('/search-field/{index}', response_model=response_models.SearchField)
@exceptions.rewriter
def search_field(index: str, field: str):
    sel = starter.get_api()
    return {"fields": sel.search_field(index, field)}


@app.post('/subfields/{index}', response_model=response_models.Subfields)
@exceptions.rewriter
def schema_subfield(index, subfields: body_models.Subfields):
    sel = starter.get_api()
    res = sel.subfields(index, subfields.fields, no_empty=subfields.no_empty)
    return {"fields": list(res)}


@app.post('/generator/{index}', response_model=response_models.Generator)
@app.post('/generator', response_model=response_models.Generator)
@exceptions.rewriter
def generator(query: body_models.QueryGenerator, index: str = None):
    sel = starter.get_api()
    query = query.__dict__
    schema = query.pop("index_schema", None)
    return sel.generate_query(query, schema=schema, index=index)


@app.get("/docs", include_in_schema=False)
@exceptions.rewriter
def docs():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI"
    )
