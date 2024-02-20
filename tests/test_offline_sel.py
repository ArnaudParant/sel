import json
import pytest
import logging

from sel.sel import SEL


@pytest.fixture(scope="session")
def osel():
    return SEL(None, log_level=logging.DEBUG)


def load_schema():
    with open("scripts/schema.json", "r") as fd:
        return json.load(fd)["mappings"]


class TestOfflineSEL():

    def test_generator(self, osel):
        query = {"query": "label = bag"}
        res = osel.generate_query(query, schema=load_schema())
        assert res == {
            'warns': [],
            'elastic_query': {'query': {'bool': {'must': [{'nested': {'path': 'media.label', 'query': {'term': {'media.label.name': 'bag'}}}}], 'must_not': [{'term': {'deleted': True}}]}}, 'sort': [{'deleted': {'order': 'desc', 'nested_filter': {'bool': {'must_not': [{'term': {'deleted': True}}]}}}}, {'media.label.score': {'order': 'desc', 'nested_path': 'media.label', 'nested_filter': {'term': {'media.label.name': 'bag'}}}}]},
            'internal_query': {'query': {'operator': 'and', 'items': [{'field': '.deleted', 'comparator': '!=', 'value': True}, {'field': 'label', 'comparator': '=', 'value': 'bag'}]}},
            'query_data': {}
        }
