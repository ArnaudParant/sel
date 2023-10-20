"""
TestRoutes is only made to tests routes, it not check api returns
"""
import pytest
import json

from scripts import elastic


TEST_INDEX_FILE = "/tests/data/sample_2017.json"
TEST_INDEX = "test_index"


class TestRoutes:

    @pytest.fixture(scope="function", autouse=True)
    def init(self):
        elastic.create_index(TEST_INDEX_FILE, TEST_INDEX, overwrite=True)


    def test_hello(self, api):
        res = api.get("/")
        assert res.json()["message"] == "Hey! Listen. Check /docs for documentation"


    def test_scroll(self, api):
        query = {"cash_time": "1m", "meta": {"size": 1000}}
        res = api.post(f"/scroll/{TEST_INDEX}", json=query)
        assert len(res.json()["documents"]) == 100, res.text

        api.delete(f"/clear-scroll/{res.json()['scroll_id']}")


    def test_download_aggreg(self, api):
        query = {
            "aggregations": {"my_aggreg": {"field": "label"}},
            "base_aggregation": {"field": "date", "interval": "week"}
        }
        res = api.post(f"/download-aggreg/{TEST_INDEX}", json=query)
        assert len(res.json()["buckets"]) > 0, res.text


    def test_search(self, api):
        res = api.post(f"/search/{TEST_INDEX}")
        assert len(res.json()["results"]["hits"]["hits"]) > 0, res.text


    def test_get_one_document(self, api):
        res = api.get(f"/document/{TEST_INDEX}/1434484792463866663")
        assert res.json()["document"] is not None, res.text


    def test_delete_documents(self, api):
        query = {"ids": ["1434484792463866663"]}
        res = api.delete(f"/delete-documents/{TEST_INDEX}", json=query)
        assert res.json()["count"] == 1, res.text


    def test_unsafe_really_delete_documents(self, api):
        query = {"ids": ["1434484792463866663"]}
        res = api.delete(f"/unsafe/really-delete-documents/{TEST_INDEX}", json=query)
        assert res.json()["count"] == 1, res.text


    def test_list_index(self, api):
        res = api.get(f"/list-index")
        assert len(res.json()["indexes"]) == 1, res.text


    def test_index_schema(self, api):
        res = api.get(f"/schema/{TEST_INDEX}")
        assert res.json()["index_schema"] is not None, res.text


    def test_list_field(self, api):
        res = api.get(f"/list-field/{TEST_INDEX}")
        assert len(res.json()["fields"]) > 0, res.text


    def test_search_field(self, api):
        res = api.get(f"/search-field/{TEST_INDEX}", params={"field": "id"})
        assert len(res.json()["fields"]) > 0, res.text


    def test_subfields(self, api):
        data = {"fields": ["label"]}
        res = api.post(f"/subfields/{TEST_INDEX}", json=data)
        assert len(res.json()["fields"]) > 0, res.text


    def test_generator(self, api):
        data = {"query": {"field": ".id", "value": "toto"}}
        res = api.post(f"/generator/{TEST_INDEX}", json=data)
        assert res.json()['elastic_query']['query']['bool']['must'] == [{'term': {'id': 'toto'}}], res.text
