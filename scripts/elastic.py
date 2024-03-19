#!/usr/bin/python3

import json
import argparse
import logging
import time
from itertools import islice
from elasticsearch import Elasticsearch
from elasticsearch.client import _normalize_hosts


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    parser.add_argument("schema_filepath")
    parser.add_argument("index_name")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--hosts", nargs='+')
    parser.add_argument("--http-auth")
    return parser.parse_args()


def create_index(filepath, schema_filepath, index, overwrite=False, hosts=None, http_auth=None):
    elastic = elastic_connect(hosts=hosts, http_auth=http_auth)

    with open(filepath) as fd:
        data = loads_ndjson(fd)

        if overwrite is True and elastic.indices.exists(index=index):
            _delete_index(elastic, index)

        _create_index(elastic, index, schema_filepath)
        insert(elastic, index, data)


def _delete_index(elastic, index):
    res = elastic.indices.delete(index=index, ignore=[400, 404, 503], request_timeout=60)
    if "acknowledged" not in res:
        raise Exception(f"Failed to delete index: {index}")


def loads_ndjson(fd):
    for line in fd:
        yield json.loads(line)


def _document_wrapper(index, documents, id_getter, operation):
    for doc in documents:

        wrapper = {"action": {operation: {
            "_index": index,
            "_id": id_getter(doc)
        }}}

        if operation != "delete":
            wrapper["source"] = doc

        yield wrapper


def _sender(elastic, bulk, operation):
    body = [s for e in bulk for s in [e["action"], e.get("source")] if s is not None]
    res = elastic.bulk(body=body, refresh=True)

    failure = [i[operation] for i in res["items"] if "error" in i[operation]]
    if failure:
        raise Exception(str(failure))


def _manager(elastic, documents, size, operation):

    while True:
        bulk = list(islice(documents, size))
        if not bulk:
            break

        _sender(elastic, bulk, operation)


def bulk(elastic, index, documents, id_getter, bulk_size=100, operation="index"):
    docs = _document_wrapper(index, documents, id_getter, operation)
    _manager(elastic, docs, bulk_size, operation)


def insert(elastic, index, data):
    logging.info("Start insertion ...")
    id_getter = lambda d: d["id"]
    bulk(elastic, index, data, id_getter)
    logging.info("Done")


def _create_index(elastic, index, schema_filepath):
    schema = load_schema(schema_filepath)
    res = elastic.indices.create(index=index, mappings=schema, request_timeout=60)
    if "acknowledged" not in res:
        logging.error("Index creation response:\n{res}")
        raise Exception("Failed to create index: {index_name}")


def load_schema(filepath):
    with open(filepath, "r") as fd:
        return json.load(fd)


def elastic_connect(hosts=None, http_auth=None):
    """ Create new elastic connection """
    es_hosts = hosts if hosts else ["http://localhost"]
    kwargs = {
        "hosts": _normalize_hosts(es_hosts),
        "http_auth": http_auth,
        "retry_on_timeout": True,
        "timeout": 30
    }

    return Elasticsearch(**kwargs)


if __name__ == "__main__":
    args = options()
    create_index(
        args.filepath, args.schema_filepath, args.index_name,
        overwrite=args.overwrite, hosts=args.hosts, http_auth=args.http_auth
    )
