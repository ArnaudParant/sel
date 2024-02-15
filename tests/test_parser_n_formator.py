import json
import pytest
import traceback

from sel import query_string_parser
from sel.query_string_parser import (
    Value, QueryString, Comparator, Not, RangeFilter, Filter, Context,
    Aggreg, Sort, Group, NoBracketGroup, Query
)
from sel import query_object_formator


class TestParserNFormator:


    @pytest.mark.parametrize(["query", "expected"], [
        ["toto", "toto"],
        ['"toto tata titi"', "toto tata titi"],
        ["toto tata titi", None], # Exception, does not match type Value
    ])
    def test_value(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Value)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["foo", None], # Exception, must be delimited by quotes
        ["'\"foo bar\"'", {"query_string": '"foo bar"'}],
    ])
    def test_query_string(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=QueryString)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["'toto'", "toto"],
        ["'to\"to'", 'to"to'],
        ["'to'to'", None], # Exception, quoting error

        ["''toto''", "toto"],
        ["''to'to''", "to'to"],
        ["'''to\"\"to'''", 'to""to'],
        ["''to''to''", None], # Exception, quoting error

        ["'''toto'''", "toto"],
        ["'''to'to'''", "to'to"],
        ["'''to''to'''", "to''to"],
        ["'''to\"\"\"to'''", 'to"""to'],
        ["'''to'''to'''", None], # Exception, quoting error

        ['"toto"', "toto"],
        ['"to\'to"', "to'to"],
        ['"to"to"', None], # Exception, quoting error

        ['""toto""', "toto"],
        ['""to"to""', 'to"to'],
        ['""to\'\'to""', "to''to"],
        ['""to""to""', None], # Exception, quoting error

        ['"""toto"""', "toto"],
        ['"""to"to"""', 'to"to'],
        ['"""to""to"""', 'to""to'],
        ['"""to\'\'\'to"""', "to'''to"],
        ['"""to"""to"""', None], # Exception quoting error
    ])
    def test_quoting(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Value)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"

            res = query_string_parser.parse(query, grammar=QueryString)
            res = query_object_formator.formator(res)
            assert res["query_string"] == expected, ("Query: '%s'\nExpected: %s\nGot: %s\n" % (query, json.dumps(expected), json.dumps(res["query_string"])))

        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["=", "="],
        ["!=", "!="],
        ["<=", "<="],
        ["<", "<"],
        [">=", ">="],
        [">", ">"],
        ["~", "~"],
        ["!~", "!~"],
        ["==", None], # Exception does not match type Comparator
    ])
    def test_comparator(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Comparator)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["color = blue",
         {"field": "color", "comparator": "=", "value": "blue"}],

        ["content ~ #moet",
         {"field": "content", "comparator": "~", "value": "#moet"}],

        ["label.color = blue",
         {"field": "label.color", "comparator": "=", "value": "blue"}],

        [".media.label.color = blue",
         {"field": ".media.label.color", "comparator": "=", "value": "blue"}],

        [".media.label.color == toto", None],

        [".media.label.color in toto", None],
        [".media.label.color in toto, tata", None],
        [".media.label.color in [toto, ]", None],
        [".media.label.color ino ['toto 1', 'tata']", None],
        [".media.label.color in ['toto 1', 'tata']",
         {"field": ".media.label.color", "comparator": "in", "value": ["toto 1", "tata"]}
        ],
        [".media.label.color in ['toto 1']",
         {"field": ".media.label.color", "comparator": "in", "value": ["toto 1"]}
        ],
        [".media.label.color nin [toto, tata]",
         {"field": ".media.label.color", "comparator": "nin", "value": ["toto", "tata"]}
        ],
        [".media.label.color nin [toto]",
         {"field": ".media.label.color", "comparator": "nin", "value": ["toto"]}
        ],
        [".media.label.color not in [toto]",
         {"field": ".media.label.color", "comparator": "nin", "value": ["toto"]}
        ],

        ["date range (> 2018)", None],
        ["date range (> 2018, > 2019)", None],
        ["date range (> 2018, = 2019)", None],
        ["date range (> 2018, <= 2019)",
         {"field": "date", "comparator": "range", "value": {">": "2018", "<=": "2019"}}
        ],
        ["date nrange (> 2018, <= 2019)",
         {"field": "date", "comparator": "nrange", "value": {">": "2018", "<=": "2019"}}
        ],
        ["date not range (> 2018, <= 2019)",
         {"field": "date", "comparator": "nrange", "value": {">": "2018", "<=": "2019"}}
        ],
        ["date not rangeo (> 2018, <= 2019)", None],

        ["label prefix h",
         {"field": "label", "comparator": "prefix", "value": "h"}
        ],
        ["label nprefix h",
         {"field": "label", "comparator": "nprefix", "value": "h"}
        ],
        ["label not prefix h",
         {"field": "label", "comparator": "nprefix", "value": "h"}
        ],
        ["label not prefixo h", None],


        ["label in person, human", None],
        ["label in (person, human)", None],
        ["label in [person human]", None],
        ["label in [person, human]",
         {"field": "label", "comparator": "in", "value": ["person", "human"]}
        ],
        ["label nin [person, human]",
         {"field": "label", "comparator": "nin", "value": ["person", "human"]}
        ],

        ["color = blue where label = bag",
         {"field": "color", "comparator": "=", "value": "blue",
          "where": {"field": "label", "comparator": "=", "value": "bag"}}],

        ["color = blue whereo label = bag", None],

        ["image.tag.color = blue where image.tag = bag",
         {"field": "image.tag.color", "comparator": "=", "value": "blue",
          "where": {"field": "image.tag", "comparator": "=", "value": "bag"}}],

        ['color = blue where (label = "bag it" and label = foo)',
         {"field": "color", "comparator": "=", "value": "blue",
          "where": {"operator": "and", "items": [
              {"field": "label", "comparator": "=", "value": "bag it"},
              {"field": "label", "comparator": "=", "value": "foo"}
          ]}}],

        ["foo = something",
         {"field": "foo", "comparator": "=", "value": "something"}],

        ["color = blue where (label = bag it)", None],
    ])
    def test_filter(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Filter)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["2018 < date <= 2019",
         {"field": "date", "comparator": "range", "value": {">": "2018", "<=": "2019"}}
        ],
        ["2018 >= date <= 2019", None],
        ["2018 < date <= ", None],
        ["2018 < date <= 2019 where label = bag",
         {
             "field": "date", "comparator": "range", "value": {">": "2018", "<=": "2019"},
             "where": {"field": "label", "comparator": "=", "value": "bag"}
         }
        ],
    ])
    def test_range_filter(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=RangeFilter)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["label where (label = bag)",
         {"field": "label", "where": {"field": "label", "comparator": "=", "value": "bag"}}],

        ["label where (label = bag or label.color = red)",
         {"field": "label", "where": {"operator": "or", "items": [
             {"field": "label", "comparator": "=", "value": "bag"},
             {"field": "label.color", "comparator": "=", "value": "red"}
         ]}}],
    ])
    def test_context(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Context)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["aggreg: color",
         {"type": "aggreg", "field": "color"}],

        ["aggreg: color graph pie",
         {"type": "aggreg", "field": "color", "graph": "pie"}],

        ["aggreg: color label", None],

        ["aggrego: color", None],

        ["aggreg toto: color",
         {"type": "aggreg", "field": "color", "name": "toto"}],

        ["aggreg: tag subaggreg by (distinct: .author.id)",
         {"type": "aggreg", "field": "tag",
          "subaggreg": {"by": {"type": "distinct", "field": ".author.id"}}}
         ],

        ["aggreg: date subaggreg by (sum: like)",
         {"type": "aggreg", "field": "date",
          "subaggreg": { "by": {"type": "sum", "field": "like"}}}
        ],

        ["aggreg: date subaggrego by (sum: like)", None],

        ["aggreg: date subaggreg by (sum: like) subaggreg by (distinct: author.id)", None],

        ["aggreg: tag size 5",
         {"type": "aggreg", "field": "tag", "size": 5}],

        ["aggreg: tag sizeo 5", None],

        ["aggreg: tag size cinq", None],

        ["aggreg: date interval month",
         {"type": "aggreg", "field": "date", "interval": "month"}],

        ["aggreg: date intervalo month", None],

        ["histogram: date",
         {"type": "histogram", "field": "date"}],

        ["aggreg: image.color",
         {"type": "aggreg", "field": "image.color"}],

        ["aggreg: image.tag.color",
         {"type": "aggreg", "field": "image.tag.color"}],

        ["average: tag.score",
         {"type": "average", "field": "tag.score"}],

        ["stats: tag.score",
         {"type": "stats", "field": "tag.score"}],

        ["min: tag.score",
         {"type": "min", "field": "tag.score"}],

        ["max: tag.score",
         {"type": "max", "field": "tag.score"}],

        ["aggreg: color where label = bag",
         {"type": "aggreg", "field": "color", "where":
          {"field": "label", "comparator": "=", "value": "bag"}}],

        ["aggreg: color where (label = bag and model = foo)",
         {"type": "aggreg", "field": "color", "where":
          {"operator": "and", "items": [
              {"field": "label", "comparator": "=", "value": "bag"},
              {"field": "model", "comparator": "=", "value": "foo"},
          ]}}
         ],

        ["aggreg: color where (label = bag and model = foo", None],

        ["aggreg: label subaggreg texture (aggreg: texture) subaggreg color (aggreg: color)",
         {"type": "aggreg", "field": "label", "subaggreg": {
             "texture": {"type": "aggreg", "field": "texture"},
             "color": {"type": "aggreg", "field": "color"},
         }}],

        ["aggreg: label subaggreg color (aggreg: texture) subaggreg color (aggreg: color)", None]

    ])
    def test_aggreg(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Aggreg)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["sort: image.color",
         {"field": "image.color"}],

        ["sort: color asc",
         {"field": "color", "order": "asc"}],

        ["sort: color asco", None],

        ["sort: color mode min",
         {"field": "color", "mode": "min"}],

        ["sort: color modez min", None],

        ["sort: color asc where color = red",
         {"field": "color", "order": "asc", "where":
          {"field": "color", "comparator": "=", "value": "red"}}],

        ["sort: color under label where label = bag",
         {"field": "color", "under": "label", "where":
          {"field": "label", "comparator": "=", "value": "bag"}}],

        ["sort: color undero label where label = bag", None],

        ["sort: color asc where (color = red and model = foo)",
         {"field": "color", "order": "asc", "where":
          {"operator": "and", "items": [
              {"field": "color", "comparator": "=", "value": "red"},
              {"field": "model", "comparator": "=", "value": "foo"},
          ]}}
         ],

        ["sort: color asc where (color = red and model = foo)", None]
    ])
    def test_sort(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Sort)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["(color = red and color = blue)",
         {"operator": "and", "items": [
             {"field": "color", "comparator": "=", "value": "red"},
             {"field": "color", "comparator": "=", "value": "blue"},
         ]}],

        ["((color = red))",
         {"field": "color", "comparator": "=", "value": "red"}],

        ['("titi" or "tata" and not ""toto"" or not """plop""")',
         {"operator": "or", "items": [
             {"query_string": "titi"},
             {"operator": "and", "items": [
                 {"query_string": "tata"},
                 {"not": {"query_string": "toto"}}
             ]},
             {"not": {"query_string": "plop"}}
         ]}],

        ['foo = 1 ando bar = 2', None],

        ["(color = red", None]
    ])
    def test_group(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Group)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["(color = red) or label = bag",
         {"operator": "or", "items": [
             {"field": "color", "comparator": "=", "value": "red"},
             {"field": "label", "comparator": "=", "value": "bag"},
         ]}],

        ["(color = red) or (label = bag and (label = face or (label = toto and tag = tata)))",
         {"operator": "or", "items": [
             {"field": "color", "comparator": "=", "value": "red"},
             {"operator": "and", "items": [
                 {"field": "label", "comparator": "=", "value": "bag"},
                 {"operator": "or", "items": [
                     {"field": "label", "comparator": "=", "value": "face"},
                     {"operator": "and", "items": [
                         {"field": "label", "comparator": "=", "value": "toto"},
                         {"field": "tag", "comparator": "=", "value": "tata"},
                     ]},
                 ]},
             ]},
         ]}],

        ["color = red label = bag", None]
    ])
    def test_nobracketgroup(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=NoBracketGroup)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["not (color = red and color = blue)",
         {"not": {"operator": "and", "items": [
             {"field": "color", "comparator": "=", "value": "red"},
             {"field": "color", "comparator": "=", "value": "blue"},
         ]}}],

        ["not ((color = red))",
         {"not": {"field": "color", "comparator": "=", "value": "red"}}],

        ["not color = red",
         {"not": {"field": "color", "comparator": "=", "value": "red"}}],

        ["not not color = red",
         {"not": {"not": {"field": "color", "comparator": "=", "value": "red"}}}],

        ["not (color = red", None]
    ])
    def test_not_syntax(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Not)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)


    @pytest.mark.parametrize(["query", "expected"], [
        ["'foo'",
         {"query": {"query_string": "foo"}}],

        ["color = blue",
         {"query": {"field": "color", "comparator": "=", "value": "blue"}}],

        ["(((color = blue)))",
         {"query": {"field": "color", "comparator": "=", "value": "blue"}}],

        ["2018 < date <= 2019",
         {"query": {"field": "date", "comparator": "range", "value": {">": "2018", "<=": "2019"}}}],

        ["2018 < date <= 2019 where label = bag",
         {"query": {
             "field": "date", "comparator": "range", "value": {">": "2018", "<=": "2019"},
             "where": {"field": "label", "comparator": "=", "value": "bag"}
         }}],

        ["2018 < date <= ", None],

        ["score >= 0.9",
         {"query": {"field": "score", "comparator": ">=", "value": "0.9"}}],

        ["aggreg: toto",
         {"aggregations": {"aggreg_0": {"type": "aggreg", "field": "toto"}}}],

        ["aggreg toto: toto",
         {"aggregations": {"toto": {"type": "aggreg", "field": "toto"}}}],

        ["aggreg toto: toto aggreg toto: tata", None],

        ["aggreg toto: toto aggreg: tata",
         {"aggregations": {
             "toto": {"type": "aggreg", "field": "toto"},
             "aggreg_1": {"type": "aggreg", "field": "tata"},
         }}
        ],

        ["color=red and label=bag aggreg:color aggreg:toto sort:color",
         {"query": {"operator": "and", "items": [
             {"field": "color", "comparator": "=", "value": "red"},
             {"field": "label", "comparator": "=", "value": "bag"},
         ]},
         "aggregations": {
             "aggreg_0": {"type": "aggreg", "field": "color"},
             "aggreg_1": {"type": "aggreg", "field": "toto"},
         },
         "sort": [
             {"field": "color"}
         ]}
        ],
    ])
    def test_query(self, query, expected):
        try:
            res = query_string_parser.parse(query, grammar=Query)
            res = query_object_formator.formator(res)
            assert res == expected, f"Query: '{query}'\nExpected: {expected}\nGot: {red}\n"
        except Exception as exc:
            print(traceback.format_exc())
            assert expected is None, str(exc)
