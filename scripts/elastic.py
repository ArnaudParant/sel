import os
import json
import argparse
import logging
import time
from elasticsearch import Elasticsearch
from elasticsearch.client import _normalize_hosts

from sel import upload


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    parser.add_argument("schema_filepath")
    parser.add_argument("index_name")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def create_index(filepath, schema_filepath, index, overwrite=False):
    elastic = elastic_connect()

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


def insert(elastic, index, data):
    logging.info("Start insertion ...")
    id_getter = lambda d: d["id"]
    upload.bulk(elastic, index, "document", data, id_getter)
    logging.info("Done")


def _create_index(elastic, index, schema_filepath):
    schema = load_schema(schema_filepath)
    res = elastic.indices.create(index=index, body=schema, request_timeout=60)
    if "acknowledged" not in res:
        logging.error("Index creation response:\n{res}")
        raise Exception("Failed to create index: {index_name}")


def load_schema(filepath):
    with open(filepath, "r") as fd:
        return json.load(fd)


def elastic_connect():
    """ Create new elastic connection """
    es_hosts = os.environ["ES_HOST"].split(",")
    kwargs = {
        "hosts": _normalize_hosts(es_hosts),
        "retry_on_timeout": True,
        "timeout": 30,
        "sniff_on_start": True,
        "sniff_on_connection_fail": True,
        "sniff_timeout": 10,
        "sniffer_timeout": 60,
    }

    return Elasticsearch(**kwargs)


if __name__ == "__main__":
    args = options()
    create_index(args.filepath, args.schema_filepath, args.index_name, overwrite=args.overwrite)
