import pytest
import json
import os

from scripts import elastic

from sel import utils
import test_utils


TEST_INDEX_FILE = "/tests/data/sample_2017.json"
TEST_SCHEMA_FILE = "/scripts/schema.json"
TEST_INDEX = "test_index"
ES_HOSTS = os.environ["ES_HOSTS"].split(",")


class TestSEL:

    @pytest.fixture(scope="function", autouse=True)
    def init(self):
        elastic.create_index(
            TEST_INDEX_FILE, TEST_SCHEMA_FILE, TEST_INDEX, hosts=ES_HOSTS, overwrite=True
        )


    def __cleaner(self, obj):
        if "_score" in obj:
            del obj["_score"]
        return obj

    @pytest.mark.parametrize(["query"], [
        [{}],
        [{"meta": {"size": 100}}],
        [{"meta": {"size": 5}}],
    ])
    def test_scroll(self, sel, query):
        with open(TEST_INDEX_FILE, "r") as f:
            expected_lines = {d["id"]: d for d in load_ndjson(f)}
            documents = []
            scroll_id = None

            while True:
                res = sel.scroll(TEST_INDEX, query, "1m", scroll_id=scroll_id)
                documents += res["documents"]
                scroll_id = res["scroll_id"]

                if not len(res["documents"]):
                    break

            sel.clear_scroll(res["scroll_id"])

            found = {}
            for line in documents:
                j = self.__cleaner(line)
                found[j["id"]] = j

            for j2 in expected_lines.values():
                j = found.get(j2["id"])
                j2["_index"] = TEST_INDEX
                assert test_utils.dict_equals(j, j2), f"Got: {j}\nExpected: {j2}"
            size = len(found)
            file_size = len(expected_lines)
            assert size == file_size, f"Download line {size} != {file_size}"

    @pytest.mark.parametrize(["query"], [
        [{"aggregations": {"labels": {"field": "label"}}}],
        [{"aggregations": {"ids": {"field": ".id"}}}],
    ])
    def test_download_aggreg(self, sel, query):

        def sort_aggreg(aggreg):
            aggreg = sorted(aggreg, key=lambda o: o["key"])
            return sorted(aggreg, key=lambda o: o["doc_count"], reverse=True)

        aggreg_key = list(query["aggregations"].keys())[0]
        query["aggregations"][aggreg_key]["size"] = 9999
        base_aggreg = {"field": "date", "interval": "week"}

        res = sel.search(TEST_INDEX, query)
        expected = utils.get_lastest_sub_data(res["results"]["aggregations"][aggreg_key])["buckets"]
        expected = sort_aggreg(expected)

        buckets = sel.download_aggreg(TEST_INDEX, base_aggreg, query)

        aggregated = {}
        for line in buckets:
            if line["key"] not in aggregated:
                aggregated[line["key"]] = line
            else:
                aggregated[line["key"]]["doc_count"] += line["doc_count"]

        aggregated = sort_aggreg(aggregated.values())

        assert aggregated == expected, f"Got: {aggregated}\nExpected: {expected}"


    @pytest.mark.parametrize(["fields", "expected_subfields"], [
        [["label"], [{'field': 'label', "subfields": ['color', 'texture']}]]
    ])
    def test_subfield(self, sel, fields, expected_subfields):
        subfields = list(sel.subfields(TEST_INDEX, fields))
        assert subfields == expected_subfields, \
            f"Expected: {expected_subfields}\nGot: {subfields}"


    def test_delete_documents(self, sel):
        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 100

        query = {"ids": ["1434484792463866663"]}
        res = sel.delete_documents(TEST_INDEX, query)
        assert res == {"count": 1, "action": "delete"}

        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 99

        res = sel.search(TEST_INDEX, {"query": "deleted = true", "meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 1

        res = sel.delete_documents(TEST_INDEX, query)
        assert res == {"count": 1, "action": "delete"}

        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 99

        res = sel.delete_documents(TEST_INDEX, query, undelete=True)
        assert res == {"count": 1, "action": "undelete"}

        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 100


    def test_really_delete_documents(self, sel):
        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 100

        query = {"ids": ["1434484792463866663"]}
        count = sel.really_delete_documents(TEST_INDEX, query)
        assert count == 1

        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 99

        res = sel.search(TEST_INDEX, {"query": "deleted = true", "meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 0

        count = sel.really_delete_documents(TEST_INDEX, query)
        assert count == 0

        res = sel.search(TEST_INDEX, {"meta": {"size": 0}})
        assert res["results"]["hits"]["total"] == 99


def load_ndjson(fd):
    for line in fd:
        yield json.loads(line)
