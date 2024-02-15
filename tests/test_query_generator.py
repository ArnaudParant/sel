import pytest

from scripts import elastic

from sel import utils
import test_utils


TEST_INDEX_FILE = "/tests/data/sample_2017.json"
TEST_SCHEMA_FILE = "/scripts/schema.json"
TEST_INDEX = "test_index"


class TestQueryGenerator:

    @pytest.fixture(scope="session", autouse=True)
    def init(self):
        elastic.create_index(TEST_INDEX_FILE, TEST_SCHEMA_FILE, TEST_INDEX, overwrite=True)


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
        ['label.score prefix 1', 97],
    ])
    def test_simple_filter(self, sel, query, expected_total):
        try:
            res = sel.search(TEST_INDEX, {"query": query})
            assert res["results"]["hits"]["total"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label.exists = true", 99],
        ["label.exists != true", 1],
        ["not label.exists = true", 1],
        ["label.exists = false", 1],
        ["label.missing = true", 0],
        ["label.missing != true", 100],
        ["label.missing = false", 100],
        ["media_size > 0", 100],
        ["media_size = 0", 0],
        ['deleted.exists = true or deleted.exists = false', 100],
    ])
    def test_function(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'

    @pytest.mark.parametrize(["query", "expected_total"], [
        ["label.entity ~ '*25fffddfa61c8bc'", 69],
        ["label.entity !~ '*25fffddfa61c8bc'", 31],
        ["label ~ '*a*'", 98],
        ["label ~ '*oo*'", 70],
    ])
    def test_regex_filter(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'

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
        assert res["results"]["hits"]["total"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'

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
        assert res["results"]["hits"]["total"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'


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
            assert res["results"]["hits"]["total"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_total"], [
        ["'2017'", 52],
        ["not '2017'", 48],
        ["content ~ '20*'", 53],
    ])
    def test_query_string_filter(self, sel, query, expected_total):
        res = sel.search(TEST_INDEX, {"query": query})
        assert res["results"]["hits"]["total"] == expected_total, \
            f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'

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
            assert res["results"]["hits"]["total"] == expected_total, \
                f'Bad document count: {res["results"]["hits"]["total"]}, expected: {expected_total}'
        except Exception as exc:
            assert expected_total is None, str(exc)


    @pytest.mark.parametrize(["query", "expected_ok"], [
        [".media.label != person", True],
        [".label != person", False],
        [".content ~ Sunday", True],
        [".content ~ Sunday", False],
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
        except:
            assert expected_ok is False, res.text

    @pytest.mark.parametrize(["query", "expected_ids"], [
        ["sort: .id asc",
         [u'1222459402943827225', u'1253754434252136810', u'1323578809726851806', u'1376904579438345642', u'1383595488075795516', u'1399053902707634383', u'1399059638099514176', u'1403562315994952320', u'1403564829163166350', u'1403565260421537480']],

        ["sort: label asc mode sum",
         ['1253754434252136810', '1476404500146632956', '1323578809726851806', '1423561936200686637', '1435879786500245290', '1524573366823246767', '1505515499667544384', '1383595488075795516', '1490181160960369114', '1434484792463866663']],

        ["sort: label.texture under label where label = bag",
         ['1440258192704582204', '1323578809726851806', '1403565260421537480', '1434815988875981994', '1222459402943827225']]

    ])
    def test_sort_order(self, sel, query, expected_ids):
        res = sel.search(TEST_INDEX, {"query": query})
        max_size = len(expected_ids)
        found = [d["_source"]['id'] for d in res["results"]["hits"]["hits"]][:max_size]
        assert test_utils.list_equals(found, expected_ids), \
            f"Got documents order: {found}\nExpected: {expected_ids}"

    def __no_filter(label):
        return True

    @pytest.mark.parametrize(["query", "filter_score", "reverse"], [
        # By default use .score desc
        ["sort: label", __no_filter, True],
        ["sort: label.score", __no_filter, True],
        ["sort: label desc", __no_filter, True],

        ["sort: label asc", __no_filter, False],
        ["sort: label where label = person", lambda l: l["name"] == "person", True],
        ["sort: label where (label = person or label = indoor)", lambda l: l["name"] == "person" or l["name"] == "indoor", True],

        # implicite sort
        ["label = person", lambda l: l["name"] == "person", True],
        ["label != person", lambda l: l["name"] != "person", True],
        ["label = dress where .score <= 0.98", lambda l: l["name"] == "dress" and l["score"] <= 0.98, True],
    ])
    def test_sort_score_order(self, sel, query, filter_score, reverse):
        res = sel.search(TEST_INDEX, {"query": query})
        found = [d["_source"]["id"] for d in res["results"]["hits"]["hits"]]

        def aux(hit):
            scores = []
            for media in hit["media"]:
                for label in media["label"]:
                    if filter_score(label):
                        scores.append(label["score"])
            score = (sum(scores) / len(scores)) if len(scores) > 0 else None
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
         [{u'key_as_string': u'2016-04', u'key': 1459468800000, u'doc_count': 89.0}, {u'key_as_string': u'2016-05', u'key': 1462060800000, u'doc_count': 2830.0}, {u'key_as_string': u'2016-06', u'key': 1464739200000, u'doc_count': 0.0}, {u'key_as_string': u'2016-07', u'key': 1467331200000, u'doc_count': 0.0}, {u'key_as_string': u'2016-08', u'key': 1470009600000, u'doc_count': 76.0}, {u'key_as_string': u'2016-09', u'key': 1472688000000, u'doc_count': 0.0}, {u'key_as_string': u'2016-10', u'key': 1475280000000, u'doc_count': 0.0}, {u'key_as_string': u'2016-11', u'key': 1477958400000, u'doc_count': 660.0}, {u'key_as_string': u'2016-12', u'key': 1480550400000, u'doc_count': 247.0}, {u'key_as_string': u'2017-01', u'key': 1483228800000, u'doc_count': 842.0}, {u'key_as_string': u'2017-02', u'key': 1485907200000, u'doc_count': 1212.0}, {u'key_as_string': u'2017-03', u'key': 1488326400000, u'doc_count': 753.0}, {u'key_as_string': u'2017-04', u'key': 1491004800000, u'doc_count': 276.0}, {u'key_as_string': u'2017-05', u'key': 1493596800000, u'doc_count': 513.0}, {u'key_as_string': u'2017-06', u'key': 1496275200000, u'doc_count': 18.0}, {u'key_as_string': u'2017-07', u'key': 1498867200000, u'doc_count': 99.0}, {u'key_as_string': u'2017-08', u'key': 1501545600000, u'doc_count': 128.0}, {u'key_as_string': u'2017-09', u'key': 1504224000000, u'doc_count': 24.0}]],

        ["aggreg: date interval month subaggreg by (count: label)",
         [{'key_as_string': '2016-04', 'key': 1459468800000, 'doc_count': 9}, {'key_as_string': '2016-05', 'key': 1462060800000, 'doc_count': 2}, {'key_as_string': '2016-06', 'key': 1464739200000, 'doc_count': 0}, {'key_as_string': '2016-07', 'key': 1467331200000, 'doc_count': 0}, {'key_as_string': '2016-08', 'key': 1470009600000, 'doc_count': 6}, {'key_as_string': '2016-09', 'key': 1472688000000, 'doc_count': 0}, {'key_as_string': '2016-10', 'key': 1475280000000, 'doc_count': 0}, {'key_as_string': '2016-11', 'key': 1477958400000, 'doc_count': 8}, {'key_as_string': '2016-12', 'key': 1480550400000, 'doc_count': 82}, {'key_as_string': '2017-01', 'key': 1483228800000, 'doc_count': 266}, {'key_as_string': '2017-02', 'key': 1485907200000, 'doc_count': 512}, {'key_as_string': '2017-03', 'key': 1488326400000, 'doc_count': 94}, {'key_as_string': '2017-04', 'key': 1491004800000, 'doc_count': 31}, {'key_as_string': '2017-05', 'key': 1493596800000, 'doc_count': 111}, {'key_as_string': '2017-06', 'key': 1496275200000, 'doc_count': 12}, {'key_as_string': '2017-07', 'key': 1498867200000, 'doc_count': 13}, {'key_as_string': '2017-08', 'key': 1501545600000, 'doc_count': 32}, {'key_as_string': '2017-09', 'key': 1504224000000, 'doc_count': 14}]],

        ["histogram: date where (date >= 2016 and date <= 2017) interval month",
         [{"key_as_string": "2016-01", "key": 1451606400000, "doc_count": 0}, {"key_as_string": "2016-02", "key": 1454284800000, "doc_count": 0}, {"key_as_string": "2016-03", "key": 1456790400000, "doc_count": 0}, {"key_as_string": "2016-04", "key": 1459468800000, "doc_count": 1}, {"key_as_string": "2016-05", "key": 1462060800000, "doc_count": 1}, {"key_as_string": "2016-06", "key": 1464739200000, "doc_count": 0}, {"key_as_string": "2016-07", "key": 1467331200000, "doc_count": 0}, {"key_as_string": "2016-08", "key": 1470009600000, "doc_count": 1}, {"key_as_string": "2016-09", "key": 1472688000000, "doc_count": 0}, {"key_as_string": "2016-10", "key": 1475280000000, "doc_count": 0}, {"key_as_string": "2016-11", "key": 1477958400000, "doc_count": 2}, {"key_as_string": "2016-12", "key": 1480550400000, "doc_count": 7}, {"key_as_string": "2017-01", "key": 1483228800000, "doc_count": 24}, {"key_as_string": "2017-02", "key": 1485907200000, "doc_count": 37}, {"key_as_string": "2017-03", "key": 1488326400000, "doc_count": 8}, {"key_as_string": "2017-04", "key": 1491004800000, "doc_count": 3}, {"key_as_string": "2017-05", "key": 1493596800000, "doc_count": 10}, {"key_as_string": "2017-06", "key": 1496275200000, "doc_count": 1}, {"key_as_string": "2017-07", "key": 1498867200000, "doc_count": 1}, {"key_as_string": "2017-08", "key": 1501545600000, "doc_count": 3}, {"key_as_string": "2017-09", "key": 1504224000000, "doc_count": 1}, {"key_as_string": "2017-10", "key": 1506816000000, "doc_count": 0}, {"key_as_string": "2017-11", "key": 1509494400000, "doc_count": 0}, {"key_as_string": "2017-12", "key": 1512086400000, "doc_count": 0}]],

        ["histogram: date where (date >= 2017 and date <= 2018) interval month",
         [{u'key_as_string': u'2017-01', u'key': 1483228800000, u'doc_count': 24}, {u'key_as_string': u'2017-02', u'key': 1485907200000, u'doc_count': 37}, {u'key_as_string': u'2017-03', u'key': 1488326400000, u'doc_count': 8}, {u'key_as_string': u'2017-04', u'key': 1491004800000, u'doc_count': 3}, {u'key_as_string': u'2017-05', u'key': 1493596800000, u'doc_count': 10}, {u'key_as_string': u'2017-06', u'key': 1496275200000, u'doc_count': 1}, {u'key_as_string': u'2017-07', u'key': 1498867200000, u'doc_count': 1}, {u'key_as_string': u'2017-08', u'key': 1501545600000, u'doc_count': 3}, {u'key_as_string': u'2017-09', u'key': 1504224000000, u'doc_count': 1}, {u'key_as_string': u'2017-10', u'key': 1506816000000, u'doc_count': 0}, {u'key_as_string': u'2017-11', u'key': 1509494400000, u'doc_count': 0}, {u'key_as_string': u'2017-12', u'key': 1512086400000, u'doc_count': 0}, {u'key_as_string': u'2018-01', u'key': 1514764800000, u'doc_count': 0}, {u'key_as_string': u'2018-02', u'key': 1517443200000, u'doc_count': 0}, {u'key_as_string': u'2018-03', u'key': 1519862400000, u'doc_count': 0}, {u'key_as_string': u'2018-04', u'key': 1522540800000, u'doc_count': 0}, {u'key_as_string': u'2018-05', u'key': 1525132800000, u'doc_count': 0}, {u'key_as_string': u'2018-06', u'key': 1527811200000, u'doc_count': 0}, {u'key_as_string': u'2018-07', u'key': 1530403200000, u'doc_count': 0}, {u'key_as_string': u'2018-08', u'key': 1533081600000, u'doc_count': 0}, {u'key_as_string': u'2018-09', u'key': 1535760000000, u'doc_count': 0}, {u'key_as_string': u'2018-10', u'key': 1538352000000, u'doc_count': 0}, {u'key_as_string': u'2018-11', u'key': 1541030400000, u'doc_count': 0}, {u'key_as_string': u'2018-12', u'key': 1543622400000, u'doc_count': 0}]],

        ["histogram: date where (date >= 2017 and date <= 2017) interval week",
         [{"key_as_string": "2016-12-26", "doc_count": 0, "key": 1482710400000}, {"key_as_string": "2017-01-02", "key": 1483315200000, "doc_count": 0}, {"key_as_string": "2017-01-09", "key": 1483920000000, "doc_count": 4}, {"key_as_string": "2017-01-16", "key": 1484524800000, "doc_count": 2}, {"key_as_string": "2017-01-23", "key": 1485129600000, "doc_count": 17}, {"key_as_string": "2017-01-30", "key": 1485734400000, "doc_count": 3}, {"key_as_string": "2017-02-06", "key": 1486339200000, "doc_count": 21}, {"key_as_string": "2017-02-13", "key": 1486944000000, "doc_count": 8}, {"key_as_string": "2017-02-20", "key": 1487548800000, "doc_count": 1}, {"key_as_string": "2017-02-27", "key": 1488153600000, "doc_count": 10}, {"key_as_string": "2017-03-06", "key": 1488758400000, "doc_count": 0}, {"key_as_string": "2017-03-13", "key": 1489363200000, "doc_count": 1}, {"key_as_string": "2017-03-20", "key": 1489968000000, "doc_count": 1}, {"key_as_string": "2017-03-27", "key": 1490572800000, "doc_count": 1}, {"key_as_string": "2017-04-03", "key": 1491177600000, "doc_count": 1}, {"key_as_string": "2017-04-10", "key": 1491782400000, "doc_count": 1}, {"key_as_string": "2017-04-17", "key": 1492387200000, "doc_count": 0}, {"key_as_string": "2017-04-24", "key": 1492992000000, "doc_count": 1}, {"key_as_string": "2017-05-01", "key": 1493596800000, "doc_count": 5}, {"key_as_string": "2017-05-08", "key": 1494201600000, "doc_count": 0}, {"key_as_string": "2017-05-15", "key": 1494806400000, "doc_count": 0}, {"key_as_string": "2017-05-22", "key": 1495411200000, "doc_count": 5}, {"key_as_string": "2017-05-29", "key": 1496016000000, "doc_count": 0}, {"key_as_string": "2017-06-05", "key": 1496620800000, "doc_count": 1}, {"key_as_string": "2017-06-12", "key": 1497225600000, "doc_count": 0}, {"key_as_string": "2017-06-19", "key": 1497830400000, "doc_count": 0}, {"key_as_string": "2017-06-26", "key": 1498435200000, "doc_count": 0}, {"key_as_string": "2017-07-03", "key": 1499040000000, "doc_count": 1}, {"key_as_string": "2017-07-10", "key": 1499644800000, "doc_count": 0}, {"key_as_string": "2017-07-17", "key": 1500249600000, "doc_count": 0}, {"key_as_string": "2017-07-24", "key": 1500854400000, "doc_count": 0}, {"key_as_string": "2017-07-31", "key": 1501459200000, "doc_count": 0}, {"key_as_string": "2017-08-07", "key": 1502064000000, "doc_count": 0}, {"key_as_string": "2017-08-14", "key": 1502668800000, "doc_count": 0}, {"key_as_string": "2017-08-21", "key": 1503273600000, "doc_count": 3}, {"key_as_string": "2017-08-28", "key": 1503878400000, "doc_count": 0}, {"key_as_string": "2017-09-04", "key": 1504483200000, "doc_count": 0}, {"key_as_string": "2017-09-11", "key": 1505088000000, "doc_count": 0}, {"key_as_string": "2017-09-18", "key": 1505692800000, "doc_count": 1}, {"key_as_string": "2017-09-25", "key": 1506297600000, "doc_count": 0}, {"key_as_string": "2017-10-02", "key": 1506902400000, "doc_count": 0}, {"key_as_string": "2017-10-09", "key": 1507507200000, "doc_count": 0}, {"key_as_string": "2017-10-16", "key": 1508112000000, "doc_count": 0}, {"key_as_string": "2017-10-23", "key": 1508716800000, "doc_count": 0}, {"key_as_string": "2017-10-30", "key": 1509321600000, "doc_count": 0}, {"key_as_string": "2017-11-06", "key": 1509926400000, "doc_count": 0}, {"key_as_string": "2017-11-13", "key": 1510531200000, "doc_count": 0}, {"key_as_string": "2017-11-20", "key": 1511136000000, "doc_count": 0}, {"key_as_string": "2017-11-27", "key": 1511740800000, "doc_count": 0}, {"key_as_string": "2017-12-04", "key": 1512345600000, "doc_count": 0}, {"key_as_string": "2017-12-11", "key": 1512950400000, "doc_count": 0}, {"key_as_string": "2017-12-18", "key": 1513555200000, "doc_count": 0}, {"key_as_string": "2017-12-25", "key": 1514160000000, "doc_count": 0}]],

    ])
    def test_histogram(self, sel, query, expected):
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
        ["aggreg: date interval week",
         [1459728000000, 1460332800000, 1460937600000, 1461542400000, 1462147200000, 1462752000000, 1463356800000, 1463961600000, 1464566400000, 1465171200000, 1465776000000, 1466380800000, 1466985600000, 1467590400000, 1468195200000, 1468800000000, 1469404800000, 1470009600000, 1470614400000, 1471219200000, 1471824000000, 1472428800000, 1473033600000, 1473638400000, 1474243200000, 1474848000000, 1475452800000, 1476057600000, 1476662400000, 1477267200000, 1477872000000, 1478476800000, 1479081600000, 1479686400000, 1480291200000, 1480896000000, 1481500800000, 1482105600000, 1482710400000, 1483315200000, 1483920000000, 1484524800000, 1485129600000, 1485734400000, 1486339200000, 1486944000000, 1487548800000, 1488153600000, 1488758400000, 1489363200000, 1489968000000, 1490572800000, 1491177600000, 1491782400000, 1492387200000, 1492992000000, 1493596800000, 1494201600000, 1494806400000, 1495411200000, 1496016000000, 1496620800000, 1497225600000, 1497830400000, 1498435200000, 1499040000000, 1499644800000, 1500249600000, 1500854400000, 1501459200000, 1502064000000, 1502668800000, 1503273600000, 1503878400000, 1504483200000, 1505088000000, 1505692800000]],

        ["aggreg: date interval month",
         [1459468800000, 1462060800000, 1464739200000, 1467331200000, 1470009600000, 1472688000000, 1475280000000, 1477958400000, 1480550400000, 1483228800000, 1485907200000, 1488326400000, 1491004800000, 1493596800000, 1496275200000, 1498867200000, 1501545600000, 1504224000000]],

        ["histogram: date interval week",
         [1459728000000, 1460332800000, 1460937600000, 1461542400000, 1462147200000, 1462752000000, 1463356800000, 1463961600000, 1464566400000, 1465171200000, 1465776000000, 1466380800000, 1466985600000, 1467590400000, 1468195200000, 1468800000000, 1469404800000, 1470009600000, 1470614400000, 1471219200000, 1471824000000, 1472428800000, 1473033600000, 1473638400000, 1474243200000, 1474848000000, 1475452800000, 1476057600000, 1476662400000, 1477267200000, 1477872000000, 1478476800000, 1479081600000, 1479686400000, 1480291200000, 1480896000000, 1481500800000, 1482105600000, 1482710400000, 1483315200000, 1483920000000, 1484524800000, 1485129600000, 1485734400000, 1486339200000, 1486944000000, 1487548800000, 1488153600000, 1488758400000, 1489363200000, 1489968000000, 1490572800000, 1491177600000, 1491782400000, 1492387200000, 1492992000000, 1493596800000, 1494201600000, 1494806400000, 1495411200000, 1496016000000, 1496620800000, 1497225600000, 1497830400000, 1498435200000, 1499040000000, 1499644800000, 1500249600000, 1500854400000, 1501459200000, 1502064000000, 1502668800000, 1503273600000, 1503878400000, 1504483200000, 1505088000000, 1505692800000]],

        ["histogram: date interval 3w",
         [1458777600000, 1460592000000, 1462406400000, 1464220800000, 1466035200000, 1467849600000, 1469664000000, 1471478400000, 1473292800000, 1475107200000, 1476921600000, 1478736000000, 1480550400000, 1482364800000, 1484179200000, 1485993600000, 1487808000000, 1489622400000, 1491436800000, 1493251200000, 1495065600000, 1496880000000, 1498694400000, 1500508800000, 1502323200000, 1504137600000, 1505952000000]],

        ["histogram: date interval month",
         [1459468800000, 1462060800000, 1464739200000, 1467331200000, 1470009600000, 1472688000000, 1475280000000, 1477958400000, 1480550400000, 1483228800000, 1485907200000, 1488326400000, 1491004800000, 1493596800000, 1496275200000, 1498867200000, 1501545600000, 1504224000000]],
    ])
    def test_histogram(self, sel, query, expected_values):
        res = sel.search(TEST_INDEX, {"query": query})
        buckets = utils.get_lastest_sub_data(res["results"]["aggregations"]["aggreg_0"])["buckets"]
        values = [d["key"] for d in buckets]
        assert test_utils.list_equals(values, expected_values), \
            f"Got values: {values}\nExpected: {expected_values}"
