import pytest
import os

from scripts import elastic

from sel import utils
from sel.schema_reader import SchemaError

import test_utils


TEST_INDEX_FILE = "/tests/data/sample_2017.json"
TEST_SCHEMA_FILE = "/tests/data/sample_2017_schema.json"
TEST_INDEX = "test_index"
ES_HOSTS = os.environ["ES_HOSTS"].split(",")
ES_AUTH = os.environ["ES_AUTH"]


class TestQueryGenerator:

    @pytest.fixture(scope="session", autouse=True)
    def init(self):
        elastic.create_index(
            TEST_INDEX_FILE, TEST_SCHEMA_FILE, TEST_INDEX, hosts=ES_HOSTS, http_auth=ES_AUTH,
            overwrite=True
        )


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["author = toto", 0],
        [{"field": "author", "value": ["toto"]}, None],
        [{"field": "author", "value": {"toto": "tata"}}, None],
        ["author = 000d891f63f14ce29f151553736b35e5", 94],
        ["label = person", 98],
        ["label != person", 2],
        ["not label = person", 2],
        ["label = 'person'", 98],
        ['label = "person"', 98],
        ['label in [person, toto]', 98],
        [{"field": 'label', "comparator": "in", "value": ["person", "toto"]}, 98],
        [{"field": 'label', "comparator": "in", "value": "person"}, None],
        [{"field": 'label', "comparator": "in", "value": {"person": "toto"}}, None],
        ['label nin [person, toto]', 2],
        ['label not in [person, toto]', 2],
        ['deleted in [True, False]', 0],
        ['label.score in [1, 2]', 97],
        ['label.score range (>=0.2, <=0.4)', 94],
        ['0.2 <= label.score <= 0.4', 94],
        [{"field": 'label', "comparator": "nin", "value": ["person", "toto"]}, 2],
        ['label prefix per', 98],
        [{"field": 'label', "comparator": "prefix", "value": 'per'}, 98],
        ['label nprefix per', 2],
    ])
    def test_simple_filter(self, sel, query, expected_total):
        try:
            res = sel.search(TEST_INDEX, {"query": query})
            assert res["results"]["hits"]["total"]["value"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label.exists = true", 99],
        ["label.exists != true", 1],
        ["not label.exists = true", 1],
        ["label.exists = false", 1],
        ["media_size > 0", 100],
        ["media_size = 0", 0],
        ['deleted.exists = true or deleted.exists = false', 100],
    ])
    def test_function(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"]["value"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'

    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label = person and label = outdoor", 1],
        ["label = person or label = indoor", 99],
        ["not (label = person or label = indoor)", 1],
        ["label = person or label = indoor and label = outdoor", 98],
        ["label = person or (label = indoor and label = outdoor)", 98],
        ["(label = person or label = indoor) and label = outdoor", 1],
    ])
    def test_combined_filter(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"]["value"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'

    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label = person where .score > 0.97", 93],
        ["label = person where .score >= 0.97", 93],
        ["label = person where .score < 0.97", 5],
        ["label = person where .score <= 0.97", 5],
        ["label = person where .score = 1", 2],
        ["label = person where (.score >= 0.97 and .score < 0.99)", 2],
        ["label = skirt where label.attribute = wrap", 0],
    ])
    def test_where_filter(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"]["value"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label where (label = person and .score > 0.97)", 93],
        ["label where (label = person and .score >= 0.97 and .score < 0.99)", 2],
        ["label where (label = skirt and label.attribute = wrap)", 0],
        [".id where (label = skirt and label.attribute = wrap)", None],
        ["label where (label where (label = person and .score > 0.97))", 93],
        ["media where (label where (label = person and .score > 0.97))", 93],
        ["media where (label where (label = bag and label.color where (color = red)))", 2],
        ["label = bag and media where (label = bag and label where (label = bag and label.color where (color = red)))", 2],
    ])
    def test_context(self, sel, query, expected_total):
        try:
            res = sel.search(TEST_INDEX, {"query": query})
            assert res["results"]["hits"]["total"]["value"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["'2017'", 52],
        ["not '2017'", 48],
        ["content ~ '20*'", 53],
        ["label.entity ~ '*25fffddfa61c8bc'", 69],
        ["label.entity !~ '*25fffddfa61c8bc'", 31],
        ["label ~ '*a*'", 98],
        ["label ~ '*oo*'", 70],
    ])
    def test_query_string_filter(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"]["value"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'

    @pytest.mark.parametrize(["query", "expected_total"], [
        ["date = 2017", 88],
        ["date > 2017", 0],
        ['date >= 2017', 88],
        ["date < 2017", 12],
        ['date < 2018', 100],
        ['date <= 2018', 100],
        ['date > 2017-06', 5],
        ['date >= 2017-06', 6],
        ['date >= 2017-06-19', 5],
        ['date >= "2017-06-19 17"', 5],
        ['date > "2017-06-19 17:56"', 5],
        ['date > "2017-06-19 17:56:00"', 5],
        ['date range (>= 2017, < 2018)', 88],
        ['2017 <= date < 2018', 88],
        ['date range (>= 2018, < 2019)', 0],
        ['date nrange (>= 2017, < 2018)', 12],
        ['not 2017 <= date < 2018', 12],
        [{"field": 'date', "comparator": "range", "value": "2017"}, None],
        [{"field": 'date', "comparator": "range", "value": ["2017", "2018"]}, None],
        [{"field": 'date', "comparator": "range", "value": ("2017", "2018")}, None],
        [{"field": 'date', "comparator": "range", "value": {">=": "2017","<": "2018"}}, 88],
    ])
    def test_date_filter(self, sel, query, expected_total):
        try:
            res = sel.search(TEST_INDEX, {"query": query})
            assert res["results"]["hits"]["total"]["value"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]["value"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_ok"], [
        [".media.label != person", True],
        [".label != person", False],
        [".content ~ Sunday", True],
        ["content ~ Sunday", True],
        [".date > 2017", True],
        [".source = 309c34318a71a94c9050f668831e1b50", True],
        ["media.label = person where .score > 0.95", True],
        [".media.label = person where .score > 0.95", True],
        [".media.label = person where label.score > 0.95", True],
        ["label = person where .score > 0.95", True],
        ["label = person where score > 0.95", False],
        ["label = person where label.score > 0.95", True],
        ["label = person where .label.score > 0.95", False],
        ["label = person where .media.label.score > 0.95", True],
        ["label = person where media.label.score > 0.95", True],
        ["label.model = foo", True],
    ])
    def test_path_finder(self, sel, query, expected_ok):
        data = {"query": query, "meta": {}}
        try:
            res = sel.search(TEST_INDEX, data)
            assert expected_ok is True, res.text

        except SchemaError:
            assert expected_ok is False, res.text

    @pytest.mark.parametrize(["query", "expected_ids"], [
        ["sort: .id asc",
         [u'1222459402943827225', u'1253754434252136810', u'1323578809726851806', u'1376904579438345642', u'1383595488075795516', u'1399053902707634383', u'1399059638099514176', u'1403562315994952320', u'1403564829163166350', u'1403565260421537480']],

        ["sort: label asc mode sum",
         ['1253754434252136810', '1476404500146632956', '1323578809726851806', '1423561936200686637', '1435879786500245290', '1524573366823246767', '1505515499667544384', '1383595488075795516', '1490181160960369114', '1434484792463866663']],

        ["sort: label.texture under label where label = bag",
         ['1440258192704582204', '1323578809726851806', '1403565260421537480', '1434815988875981994']]

    ])
    def test_sort_order(self, sel, query, expected_ids):
        res = sel.search(TEST_INDEX, {"query": query})
        max_size = len(expected_ids)
        found = [d["_source"]['id'] for d in res["results"]["hits"]["hits"]][:max_size]
        assert test_utils.list_equals(found, expected_ids), \
            f"Got documents order: {found}\nExpected: {expected_ids}"

    def __no_filter(label):
        return True

    def __average(scores):
        return sum(scores) / len(scores)

    @pytest.mark.parametrize(["query", "filter_score", "score_func", "reverse"], [
        # By default use .score desc
        ["sort: label mode avg", __no_filter, __average, True],
        ["sort: label.score mode avg", __no_filter, __average, True],
        ["sort: label desc mode avg", __no_filter, __average, True],

        ["sort: label mode sum", __no_filter, sum, True],
        ["sort: label mode min", __no_filter, min, True],
        ["sort: label mode max", __no_filter, max, True],

        ["sort: label asc mode avg", __no_filter, __average, False],
        ["sort: label mode avg where label = person", lambda l: l["name"] == "person", __average, True],
        ["sort: label mode avg where (label = person or label = indoor)", lambda l: l["name"] == "person" or l["name"] == "indoor", __average, True],

        # implicite sort
        ["label = person", lambda l: l["name"] == "person", max, True],
        ["label != person", lambda l: l["name"] != "person", max, True],
        ["label = dress where .score <= 0.98", lambda l: l["name"] == "dress" and l["score"] <= 0.98, max, True],
    ])
    def test_sort_score_order(self, sel, query, filter_score, score_func, reverse):
        res = sel.search(TEST_INDEX, {"query": query})
        found = [d["_source"]["id"] for d in res["results"]["hits"]["hits"]]

        def aux(hit):
            scores = []
            for media in hit["media"]:
                for label in media["label"]:
                    if filter_score(label):
                        scores.append(label["score"])
            score = score_func(scores) if scores else None
            return {"id": hit["id"], "score": score}

        extract = [aux(h["_source"]) for h in res["results"]["hits"]["hits"]]
        expected = sorted(extract, key=lambda h: h["score"] if h["score"] is not None else 0, reverse=reverse)
        expected = [e["id"] for e in expected]
        scores = [e["score"] for e in extract]
        assert test_utils.list_equals(found, expected), \
            f"Got documents order: {found}\nScore order: {scores}\nExpected: {expected}\n"



    @pytest.mark.parametrize(["aggreg_key", "query", "expected_values"], [
        ["aggreg_0", "aggreg: label",
         [u'face', u'dress', u'shoes', u'person', u'fullbody', u'indoor', u'wholeimage', u'pants', u'top', u'coat', u'day', u'bag', u'skirt', u'event', u'runway', u'shorts', u'crowd', u'socks', u'body', u'hat']],

        # MUST have the same results
        ["aggreg_0", "aggreg: label where label != dress",
         [u'face', u'shoes', u'person', u'fullbody', u'indoor', u'wholeimage', u'pants', u'top', u'coat', u'day', u'bag', u'skirt', u'event', u'runway', u'shorts', u'crowd', u'socks', u'body', u'hat', u'outdoor']],
        ["aggreg_0", "aggreg: label under label where label != dress",
         [u'face', u'shoes', u'person', u'fullbody', u'indoor', u'wholeimage', u'pants', u'top', u'coat', u'day', u'bag', u'skirt', u'event', u'runway', u'shorts', u'crowd', u'socks', u'body', u'hat', u'outdoor']],

        ["aggreg_0", "aggreg: label where (label != dress and label != bag)",
         [u'face', u'shoes', u'person', u'fullbody', u'indoor', u'wholeimage', u'pants', u'top', u'coat', u'day', u'skirt', u'event', u'runway', u'shorts', u'crowd', u'socks', u'body', u'hat', u'outdoor', u'portrait']],

        ["toto", "aggreg toto: label size 2", ['face', 'dress']],
        ["mysource", "aggreg mysource: source where (follower > 1000)", ['ffe8560492ef96f860b965341d0c9698']],
        ["person_source", "aggreg person_source: source where (label = person)", ["ffe8560492ef96f860b965341d0c9698"]],
        ["tata", "aggreg tata: source where (date > 2017 and label = person where .score > 0.5)", []],

        # MUST have the same results
        ["aggreg_0", "label != dress aggreg: label",
         ['face', 'shoes', 'top', 'bag', 'pants', 'person', 'coat', 'fullbody', 'indoor', 'shorts', 'wholeimage', 'day', 'crowd', 'event', 'product', 'runway', 'skirt']],

        ["aggreg_0", "aggreg: label under . where label != dress",
         ['face', 'shoes', 'top', 'bag', 'pants', 'person', 'coat', 'fullbody', 'indoor', 'shorts', 'wholeimage', 'day', 'crowd', 'event', 'product', 'runway', 'skirt']],
    ])
    def test_aggreg(self, sel, aggreg_key, query, expected_values):
        res = sel.search(TEST_INDEX, {"query": query})
        buckets = utils.get_lastest_sub_data(res["results"]["aggregations"][aggreg_key])["buckets"]
        values = [d["key"] for d in buckets]
        assert test_utils.list_equals(values, expected_values), \
            f"Got values: {values}\nExpected: {expected_values}"


    @pytest.mark.parametrize(["aggreg_key", "query", "expected_graph"], [
        ["aggreg_0", "aggreg: label graph pie", "pie"],
        ["aggreg_0", "aggreg: label graph toto", "toto"],
        ["aggreg_0", "aggreg: label", None],
    ])
    def test_aggreg_graph(self, sel, aggreg_key, query, expected_graph):
        res = sel.search(TEST_INDEX, {"query": query})
        graph = res["results"]["aggregations"][aggreg_key].get("graph")

        assert expected_graph == graph, f"Got values: {graph}\nExpected: {expected_graph}"


    @pytest.mark.parametrize(["query", "expected"], [
        ["aggreg: date interval month",
         [{u'key_as_string': u'2016-04', u'key': 1459468800000, u'doc_count': 1}, {u'key_as_string': u'2016-05', u'key': 1462060800000, u'doc_count': 1}, {u'key_as_string': u'2016-06', u'key': 1464739200000, u'doc_count': 0}, {u'key_as_string': u'2016-07', u'key': 1467331200000, u'doc_count': 0}, {u'key_as_string': u'2016-08', u'key': 1470009600000, u'doc_count': 1}, {u'key_as_string': u'2016-09', u'key': 1472688000000, u'doc_count': 0}, {u'key_as_string': u'2016-10', u'key': 1475280000000, u'doc_count': 0}, {u'key_as_string': u'2016-11', u'key': 1477958400000, u'doc_count': 2}, {u'key_as_string': u'2016-12', u'key': 1480550400000, u'doc_count': 7}, {u'key_as_string': u'2017-01', u'key': 1483228800000, u'doc_count': 24}, {u'key_as_string': u'2017-02', u'key': 1485907200000, u'doc_count': 37}, {u'key_as_string': u'2017-03', u'key': 1488326400000, u'doc_count': 8}, {u'key_as_string': u'2017-04', u'key': 1491004800000, u'doc_count': 3}, {u'key_as_string': u'2017-05', u'key': 1493596800000, u'doc_count': 10}, {u'key_as_string': u'2017-06', u'key': 1496275200000, u'doc_count': 1}, {u'key_as_string': u'2017-07', u'key': 1498867200000, u'doc_count': 1}, {u'key_as_string': u'2017-08', u'key': 1501545600000, u'doc_count': 3}, {u'key_as_string': u'2017-09', u'key': 1504224000000, u'doc_count': 1}]],

        ["aggreg: date interval month subaggreg by (sum: like)",
         [{'key_as_string': '2016-04', 'key': 1459468800000, 'doc_count': 1, 'by': {'value': 80.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-05', 'key': 1462060800000, 'doc_count': 1, 'by': {'value': 2830.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-06', 'key': 1464739200000, 'doc_count': 0, 'by': {'value': 0.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-07', 'key': 1467331200000, 'doc_count': 0, 'by': {'value': 0.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-08', 'key': 1470009600000, 'doc_count': 1, 'by': {'value': 70.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-09', 'key': 1472688000000, 'doc_count': 0, 'by': {'value': 0.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-10', 'key': 1475280000000, 'doc_count': 0, 'by': {'value': 0.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-11', 'key': 1477958400000, 'doc_count': 2, 'by': {'value': 660.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2016-12', 'key': 1480550400000, 'doc_count': 7, 'by': {'value': 200.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-01', 'key': 1483228800000, 'doc_count': 24, 'by': {'value': 730.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-02', 'key': 1485907200000, 'doc_count': 37, 'by': {'value': 1050.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-03', 'key': 1488326400000, 'doc_count': 8, 'by': {'value': 720.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-04', 'key': 1491004800000, 'doc_count': 3, 'by': {'value': 250.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-05', 'key': 1493596800000, 'doc_count': 10, 'by': {'value': 470.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-06', 'key': 1496275200000, 'doc_count': 1, 'by': {'value': 10.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-07', 'key': 1498867200000, 'doc_count': 1, 'by': {'value': 90.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-08', 'key': 1501545600000, 'doc_count': 3, 'by': {'value': 110.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}, {'key_as_string': '2017-09', 'key': 1504224000000, 'doc_count': 1, 'by': {'value': 20.0, 'aggreg_type': 'sum', 'field': '.like', 'query_field': 'like'}}]],

        ["aggreg: date interval month subaggreg by (count: label)",
         [{'key_as_string': '2016-04', 'key': 1459468800000, 'doc_count': 1, 'by': {'doc_count': 9, 'sub': {'value': 9}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-05', 'key': 1462060800000, 'doc_count': 1, 'by': {'doc_count': 2, 'sub': {'value': 2}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-06', 'key': 1464739200000, 'doc_count': 0, 'by': {'doc_count': 0, 'sub': {'value': 0}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-07', 'key': 1467331200000, 'doc_count': 0, 'by': {'doc_count': 0, 'sub': {'value': 0}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-08', 'key': 1470009600000, 'doc_count': 1, 'by': {'doc_count': 6, 'sub': {'value': 6}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-09', 'key': 1472688000000, 'doc_count': 0, 'by': {'doc_count': 0, 'sub': {'value': 0}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-10', 'key': 1475280000000, 'doc_count': 0, 'by': {'doc_count': 0, 'sub': {'value': 0}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-11', 'key': 1477958400000, 'doc_count': 2, 'by': {'doc_count': 8, 'sub': {'value': 8}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2016-12', 'key': 1480550400000, 'doc_count': 7, 'by': {'doc_count': 82, 'sub': {'value': 82}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-01', 'key': 1483228800000, 'doc_count': 24, 'by': {'doc_count': 266, 'sub': {'value': 266}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-02', 'key': 1485907200000, 'doc_count': 37, 'by': {'doc_count': 512, 'sub': {'value': 512}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-03', 'key': 1488326400000, 'doc_count': 8, 'by': {'doc_count': 94, 'sub': {'value': 94}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-04', 'key': 1491004800000, 'doc_count': 3, 'by': {'doc_count': 31, 'sub': {'value': 31}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-05', 'key': 1493596800000, 'doc_count': 10, 'by': {'doc_count': 111, 'sub': {'value': 111}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-06', 'key': 1496275200000, 'doc_count': 1, 'by': {'doc_count': 12, 'sub': {'value': 12}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-07', 'key': 1498867200000, 'doc_count': 1, 'by': {'doc_count': 13, 'sub': {'value': 13}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-08', 'key': 1501545600000, 'doc_count': 3, 'by': {'doc_count': 32, 'sub': {'value': 32}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}, {'key_as_string': '2017-09', 'key': 1504224000000, 'doc_count': 1, 'by': {'doc_count': 14, 'sub': {'value': 14}, 'aggreg_type': 'count', 'field': '.media.label.name', 'query_field': 'label'}}]],

        ["histogram: date where (date >= 2016 and date <= 2017) interval month",
         [{"key_as_string": "2016-04", "key": 1459468800000, "doc_count": 1}, {"key_as_string": "2016-05", "key": 1462060800000, "doc_count": 1}, {"key_as_string": "2016-06", "key": 1464739200000, "doc_count": 0}, {"key_as_string": "2016-07", "key": 1467331200000, "doc_count": 0}, {"key_as_string": "2016-08", "key": 1470009600000, "doc_count": 1}, {"key_as_string": "2016-09", "key": 1472688000000, "doc_count": 0}, {"key_as_string": "2016-10", "key": 1475280000000, "doc_count": 0}, {"key_as_string": "2016-11", "key": 1477958400000, "doc_count": 2}, {"key_as_string": "2016-12", "key": 1480550400000, "doc_count": 7}, {"key_as_string": "2017-01", "key": 1483228800000, "doc_count": 24}, {"key_as_string": "2017-02", "key": 1485907200000, "doc_count": 37}, {"key_as_string": "2017-03", "key": 1488326400000, "doc_count": 8}, {"key_as_string": "2017-04", "key": 1491004800000, "doc_count": 3}, {"key_as_string": "2017-05", "key": 1493596800000, "doc_count": 10}, {"key_as_string": "2017-06", "key": 1496275200000, "doc_count": 1}, {"key_as_string": "2017-07", "key": 1498867200000, "doc_count": 1}, {"key_as_string": "2017-08", "key": 1501545600000, "doc_count": 3}, {"key_as_string": "2017-09", "key": 1504224000000, "doc_count": 1}]],

        ["histogram: date where (date >= 2017 and date <= 2018) interval month",
         [{u'key_as_string': u'2017-01', u'key': 1483228800000, u'doc_count': 24}, {u'key_as_string': u'2017-02', u'key': 1485907200000, u'doc_count': 37}, {u'key_as_string': u'2017-03', u'key': 1488326400000, u'doc_count': 8}, {u'key_as_string': u'2017-04', u'key': 1491004800000, u'doc_count': 3}, {u'key_as_string': u'2017-05', u'key': 1493596800000, u'doc_count': 10}, {u'key_as_string': u'2017-06', u'key': 1496275200000, u'doc_count': 1}, {u'key_as_string': u'2017-07', u'key': 1498867200000, u'doc_count': 1}, {u'key_as_string': u'2017-08', u'key': 1501545600000, u'doc_count': 3}, {u'key_as_string': u'2017-09', u'key': 1504224000000, u'doc_count': 1}]],

        ["histogram: date where (date >= 2017 and date <= 2017) interval week size 100",
         [{"key_as_string": "2017-01-09", "key": 1483920000000, "doc_count": 4}, {"key_as_string": "2017-01-16", "key": 1484524800000, "doc_count": 2}, {"key_as_string": "2017-01-23", "key": 1485129600000, "doc_count": 17}, {"key_as_string": "2017-01-30", "key": 1485734400000, "doc_count": 3}, {"key_as_string": "2017-02-06", "key": 1486339200000, "doc_count": 21}, {"key_as_string": "2017-02-13", "key": 1486944000000, "doc_count": 8}, {"key_as_string": "2017-02-20", "key": 1487548800000, "doc_count": 1}, {"key_as_string": "2017-02-27", "key": 1488153600000, "doc_count": 10}, {"key_as_string": "2017-03-06", "key": 1488758400000, "doc_count": 0}, {"key_as_string": "2017-03-13", "key": 1489363200000, "doc_count": 1}, {"key_as_string": "2017-03-20", "key": 1489968000000, "doc_count": 1}, {"key_as_string": "2017-03-27", "key": 1490572800000, "doc_count": 1}, {"key_as_string": "2017-04-03", "key": 1491177600000, "doc_count": 1}, {"key_as_string": "2017-04-10", "key": 1491782400000, "doc_count": 1}, {"key_as_string": "2017-04-17", "key": 1492387200000, "doc_count": 0}, {"key_as_string": "2017-04-24", "key": 1492992000000, "doc_count": 1}, {"key_as_string": "2017-05-01", "key": 1493596800000, "doc_count": 5}, {"key_as_string": "2017-05-08", "key": 1494201600000, "doc_count": 0}, {"key_as_string": "2017-05-15", "key": 1494806400000, "doc_count": 0}, {"key_as_string": "2017-05-22", "key": 1495411200000, "doc_count": 5}, {"key_as_string": "2017-05-29", "key": 1496016000000, "doc_count": 0}, {"key_as_string": "2017-06-05", "key": 1496620800000, "doc_count": 1}, {"key_as_string": "2017-06-12", "key": 1497225600000, "doc_count": 0}, {"key_as_string": "2017-06-19", "key": 1497830400000, "doc_count": 0}, {"key_as_string": "2017-06-26", "key": 1498435200000, "doc_count": 0}, {"key_as_string": "2017-07-03", "key": 1499040000000, "doc_count": 1}, {"key_as_string": "2017-07-10", "key": 1499644800000, "doc_count": 0}, {"key_as_string": "2017-07-17", "key": 1500249600000, "doc_count": 0}, {"key_as_string": "2017-07-24", "key": 1500854400000, "doc_count": 0}, {"key_as_string": "2017-07-31", "key": 1501459200000, "doc_count": 0}, {"key_as_string": "2017-08-07", "key": 1502064000000, "doc_count": 0}, {"key_as_string": "2017-08-14", "key": 1502668800000, "doc_count": 0}, {"key_as_string": "2017-08-21", "key": 1503273600000, "doc_count": 3}, {"key_as_string": "2017-08-28", "key": 1503878400000, "doc_count": 0}, {"key_as_string": "2017-09-04", "key": 1504483200000, "doc_count": 0}, {"key_as_string": "2017-09-11", "key": 1505088000000, "doc_count": 0}, {"key_as_string": "2017-09-18", "key": 1505692800000, "doc_count": 1}]],

    ])
    def test_histogram_buckets(self, sel, query, expected):
        res = sel.search(TEST_INDEX, {"query": query})
        values = utils.get_lastest_sub_data(res["results"]["aggregations"]["aggreg_0"])["buckets"]
        assert test_utils.list_equals(values, expected), \
            f"Got values: {values}\nExpected: {expected}"


    @pytest.mark.parametrize(["query", "expected"], [
        ["aggreg: label",
         [('face', 266, []), ('dress', 203, []), ('shoes', 162, []), ('person', 98, []), ('fullbody', 90, []), ('indoor', 69, []), ('wholeimage', 53, []), ('pants', 50, []), ('top', 49, []), ('coat', 27, []), ('day', 27, []), ('bag', 20, []), ('skirt', 20, []), ('event', 19, []), ('runway', 17, []), ('shorts', 8, []), ('crowd', 6, []), ('socks', 2, []), ('body', 1, []), ('hat', 1, [])]
        ],

        ["aggreg: label subaggreg by (distinct: author.name)",
         [('face', 266, [('by', 97, 5)]), ('dress', 203, [('by', 89, 3)]), ('shoes', 162, [('by', 77, 1)]), ('person', 98, [('by', 98, 5)]), ('fullbody', 90, [('by', 90, 5)]), ('indoor', 69, [('by', 69, 3)]), ('wholeimage', 53, [('by', 53, 3)]), ('pants', 50, [('by', 36, 4)]), ('top', 49, [('by', 36, 1)]), ('coat', 27, [('by', 21, 3)]), ('day', 27, [('by', 27, 2)]), ('bag', 20, [('by', 19, 3)]), ('skirt', 20, [('by', 18, 1)]), ('event', 19, [('by', 19, 3)]), ('runway', 17, [('by', 17, 3)]), ('shorts', 8, [('by', 8, 2)]), ('crowd', 6, [('by', 6, 1)]), ('socks', 2, [('by', 1, 1)]), ('body', 1, [('by', 1, 1)]), ('hat', 1, [('by', 1, 1)])]
        ],

        ["aggreg: label subaggreg by (count: label.texture)",
         [('face', 266, [('by', None, 0)]), ('dress', 203, [('by', None, 219)]), ('shoes', 162, [('by', None, 12)]), ('person', 98, [('by', None, 0)]), ('fullbody', 90, [('by', None, 0)]), ('indoor', 69, [('by', None, 0)]), ('wholeimage', 53, [('by', None, 0)]), ('pants', 50, [('by', None, 34)]), ('top', 49, [('by', None, 31)]), ('coat', 27, [('by', None, 20)]), ('day', 27, [('by', None, 0)]), ('bag', 20, [('by', None, 7)]), ('skirt', 20, [('by', None, 23)]), ('event', 19, [('by', None, 0)]), ('runway', 17, [('by', None, 0)]), ('shorts', 8, [('by', None, 4)]), ('crowd', 6, [('by', None, 0)]), ('socks', 2, [('by', None, 0)]), ('body', 1, [('by', None, 0)]), ('hat', 1, [('by', None, 0)])]
        ],

        ["aggreg: label subaggreg by (sum: like)",
         [('face', 266, [('by', 97, 3960.0)]), ('dress', 203, [('by', 89, 3660.0)]), ('shoes', 162, [('by', 77, 2340.0)]), ('person', 98, [('by', 98, 3990.0)]), ('fullbody', 90, [('by', 90, 3720.0)]), ('indoor', 69, [('by', 69, 5370.0)]), ('wholeimage', 53, [('by', 53, 2130.0)]), ('pants', 50, [('by', 36, 1170.0)]), ('top', 49, [('by', 36, 1440.0)]), ('coat', 27, [('by', 21, 480.0)]), ('day', 27, [('by', 27, 1100.0)]), ('bag', 20, [('by', 19, 720.0)]), ('skirt', 20, [('by', 18, 840.0)]), ('event', 19, [('by', 19, 920.0)]), ('runway', 17, [('by', 17, 850.0)]), ('shorts', 8, [('by', 8, 260.0)]), ('crowd', 6, [('by', 6, 120.0)]), ('socks', 2, [('by', 1, 20.0)]), ('body', 1, [('by', 1, 20.0)]), ('hat', 1, [('by', 1, 120.0)])]
        ],

        ["aggreg: label subaggreg color (aggreg: color size 1) subaggreg texture (aggreg: texture size 1)",
         [('face', 266, [('color', 0, []), ('texture', None, [])]), ('dress', 203, [('color', 429, [('black', 88, [])]), ('texture', None, [('printed', 52, [])])]), ('shoes', 162, [('color', 0, []), ('texture', None, [('leather', 3, [])])]), ('person', 98, [('color', 0, []), ('texture', None, [])]), ('fullbody', 90, [('color', 0, []), ('texture', None, [])]), ('indoor', 69, [('color', 0, []), ('texture', None, [])]), ('wholeimage', 53, [('color', 0, []), ('texture', None, [])]), ('pants', 50, [('color', 94, [('black', 34, [])]), ('texture', None, [('denim', 8, [])])]), ('top', 49, [('color', 107, [('brown', 21, [])]), ('texture', None, [('printed', 9, [])])]), ('coat', 27, [('color', 51, [('black', 21, [])]), ('texture', None, [('leather', 8, [])])]), ('day', 27, [('color', 0, []), ('texture', None, [])]), ('bag', 20, [('color', 49, [('black', 13, [])]), ('texture', None, [('quilted', 3, [])])]), ('skirt', 20, [('color', 53, [('brown', 11, [])]), ('texture', None, [('printed', 7, [])])]), ('event', 19, [('color', 0, []), ('texture', None, [])]), ('runway', 17, [('color', 0, []), ('texture', None, [])]), ('shorts', 8, [('color', 16, [('brown', 6, [])]), ('texture', None, [('denim', 1, [])])]), ('crowd', 6, [('color', 0, []), ('texture', None, [])]), ('socks', 2, [('color', 0, []), ('texture', None, [])]), ('body', 1, [('color', 5, [('beige', 1, [])]), ('texture', None, [])]), ('hat', 1, [('color', 0, []), ('texture', None, [])])]
        ]

    ])
    def test_subaggreg(self, sel, query, expected):
        res = sel.search(TEST_INDEX, {"query": query})
        values = test_utils.subaggreg_formator(res["results"]["aggregations"]["aggreg_0"])
        assert test_utils.list_equals(values, expected), \
            f"Got values: {values}\nExpected: {expected}"


    @pytest.mark.parametrize(["query", "expected_dic"], [
        ["count: label", 1192],
        ["count: label where .score < 0.95", 378],
        ["distinct: label", 24],
        ["distinct: label where .score < 0.95", 20],
        ["sum: label.score", 1059.782954722643],
        ["sum: label.score where label = bag", 18.79399996995926],
        ["min: label.score", 0.2828427255153656],
        ["min: label.score where .score < 0.9", 0.2828427255153656],
        ["max: label.score", 1.0],
        ["max: label.score where .score < 0.9", 0.8989999890327454],
        ["average: label.score", 0.8890796600022172],
        ["average: label.score where .score < 0.9", 0.5961104912559191],
        # ["stats: label.score", {"count": 1192, "min": 0.2828427255153656, "sum_of_squares": 994.6157139719759, "max": 1.0, "sum": 1059.782954722643, "std_deviation": 0.20963423906199122, "variance": 0.043946514187100084, "std_deviation_bounds": {"upper": 1.3083481381261997, "lower": 0.46981118187823473}, "avg": 0.8890796600022172}],
        # ["stats: label.score where .score < 0.9", {"count": 300, "min": 0.2828427255153656, "sum_of_squares": 124.13903544623123, "max": 0.8989999890327454, "sum": 178.83314737677574, "std_deviation": 0.24176241857533926, "variance": 0.05844906703539754, "std_deviation_bounds": {"upper": 1.0796353284065976, "lower": 0.11258565410524057}, "avg": 0.5961104912559191}]
    ])
    def test_special_aggreg(self, sel, query, expected_dic):
        res = sel.search(TEST_INDEX, {"query": query})
        dic = utils.get_lastest_sub_data(res["results"]["aggregations"]["aggreg_0"])["value"]
        assert dic == expected_dic, f"Got values: {dic}\nExpected: {expected_dic}"

    @pytest.mark.parametrize(["query", "expected_values"], [
        ["aggreg: date interval week size 80",
         ['2016-04-04', '2016-04-11', '2016-04-18', '2016-04-25', '2016-05-02', '2016-05-09', '2016-05-16', '2016-05-23', '2016-05-30', '2016-06-06', '2016-06-13', '2016-06-20', '2016-06-27', '2016-07-04', '2016-07-11', '2016-07-18', '2016-07-25', '2016-08-01', '2016-08-08', '2016-08-15', '2016-08-22', '2016-08-29', '2016-09-05', '2016-09-12', '2016-09-19', '2016-09-26', '2016-10-03', '2016-10-10', '2016-10-17', '2016-10-24', '2016-10-31', '2016-11-07', '2016-11-14', '2016-11-21', '2016-11-28', '2016-12-05', '2016-12-12', '2016-12-19', '2016-12-26', '2017-01-02', '2017-01-09', '2017-01-16', '2017-01-23', '2017-01-30', '2017-02-06', '2017-02-13', '2017-02-20', '2017-02-27', '2017-03-06', '2017-03-13', '2017-03-20', '2017-03-27', '2017-04-03', '2017-04-10', '2017-04-17', '2017-04-24', '2017-05-01', '2017-05-08', '2017-05-15', '2017-05-22', '2017-05-29', '2017-06-05', '2017-06-12', '2017-06-19', '2017-06-26', '2017-07-03', '2017-07-10', '2017-07-17', '2017-07-24', '2017-07-31', '2017-08-07', '2017-08-14', '2017-08-21', '2017-08-28', '2017-09-04', '2017-09-11', '2017-09-18']],

        ["aggreg: date interval month size 20",
         ['2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11', '2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09']],

        ["aggreg: date interval month size 2",
         ['2016-04', '2016-05']],

        ["aggreg: date interval year",
         ['2016', '2017']],

        ["histogram: date interval week size 80",
         ['2016-04-04', '2016-04-11', '2016-04-18', '2016-04-25', '2016-05-02', '2016-05-09', '2016-05-16', '2016-05-23', '2016-05-30', '2016-06-06', '2016-06-13', '2016-06-20', '2016-06-27', '2016-07-04', '2016-07-11', '2016-07-18', '2016-07-25', '2016-08-01', '2016-08-08', '2016-08-15', '2016-08-22', '2016-08-29', '2016-09-05', '2016-09-12', '2016-09-19', '2016-09-26', '2016-10-03', '2016-10-10', '2016-10-17', '2016-10-24', '2016-10-31', '2016-11-07', '2016-11-14', '2016-11-21', '2016-11-28', '2016-12-05', '2016-12-12', '2016-12-19', '2016-12-26', '2017-01-02', '2017-01-09', '2017-01-16', '2017-01-23', '2017-01-30', '2017-02-06', '2017-02-13', '2017-02-20', '2017-02-27', '2017-03-06', '2017-03-13', '2017-03-20', '2017-03-27', '2017-04-03', '2017-04-10', '2017-04-17', '2017-04-24', '2017-05-01', '2017-05-08', '2017-05-15', '2017-05-22', '2017-05-29', '2017-06-05', '2017-06-12', '2017-06-19', '2017-06-26', '2017-07-03', '2017-07-10', '2017-07-17', '2017-07-24', '2017-07-31', '2017-08-07', '2017-08-14', '2017-08-21', '2017-08-28', '2017-09-04', '2017-09-11', '2017-09-18']],

        ["histogram: date interval month size 20",
         ['2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11', '2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09']],

        ["histogram: date interval day size 2",
         ['2016-04-06', '2016-04-07']],
    ])
    def test_histogram_key_return(self, sel, query, expected_values):
        res = sel.search(TEST_INDEX, {"query": query})
        buckets = utils.get_lastest_sub_data(res["results"]["aggregations"]["aggreg_0"])["buckets"]
        values = [d["key_as_string"] for d in buckets]
        assert test_utils.list_equals(values, expected_values), \
            f"Got values: {values}\nExpected: {expected_values}"
