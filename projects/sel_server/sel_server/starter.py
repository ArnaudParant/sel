import os
import logging
from elasticsearch import Elasticsearch
from elasticsearch.client import _normalize_hosts

from . import config

from sel.sel import SEL


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

CONF = config.read()
CONNECTION = None

def get_api():
    """ Create new instant of the api """
    global CONNECTION

    if CONNECTION is None:
        CONNECTION = elastic_connect()

    log_level = logging.INFO
    if os.environ.get("LOGLEVEL").upper() == "DEBUG":
        log_level = logging.DEBUG

    return SEL(CONNECTION, conf=CONF, log_level=log_level)
