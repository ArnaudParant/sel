"""
Defined Input Query String Grammar
Parse and formate in internal tree object format
"""
import re
from pypeg2 import Parser, List, attr, ignore, optional, some, maybe_some, blank
from .utils import InvalidClientInput


AGGREG_TYPES = ["aggreg", "histogram", "count", "distinct", "min", "max", "sum", "average", "stats"]

AGGREG_PARAMETER_MAPPING = {
    "subaggreg": None,
    "interval": None,
    "size": int,
    "under": None,
    "where": None,
    "graph": None,
}

##########################################################################
# GRAMMAR TOOLS
##########################################################################


def split_if_contains(keywords, name):
    suffix = ""
    for keyword in keywords:
        if keyword in name:
            name, suffix = name.split(keyword)
            suffix = f" {keyword} {suffix.strip()}"
    return name.strip(), suffix


def syntaxerror_parser(parser, text, pos=None, name=None, expected_keywords=None):
    str_pos = f" at line {pos[0]}, global position {pos[1]}" if pos else ""
    clean_text = text.strip().lower()
    expected = f", expect {name}" if name else " is unexpected"

    if not clean_text and not name:
        # Tell to the parser that it does not match, it's okay.
        # If <name> is setted we are in Error class case, if nothing left, we still want to raise
        return text, SyntaxError()

    if not clean_text:
        # We can only enter here if name is not None / Empty
        short_text = "nothing found, but"
        name, explanation = split_if_contains(["after", "for"], name)
        expected = f" {name} was expected{explanation}"

    else:
        first_line_remaining = text.strip().split("\n")[0]
        short_text = first_line_remaining[:40]
        if len(short_text) < len(clean_text):
            short_text += "..."
        short_text = f'"{short_text}"'

    if not expected_keywords:
        expected_keywords = []

    not_match_count = 0
    for keyword in expected_keywords:

        if not clean_text.startswith(keyword.lower()):
            not_match_count += 1

        elif keyword != ")":
            # If keyword match.
            # Check if no alpha-numberic additional char in present right after, without any spaces.
            # eg. 'aggrego' will fail, 'aggreg' will not.
            found = re.match(r"^[0-1a-zA-Z]+", clean_text[len(keyword):])
            if found and found.group(0):
                raise InvalidClientInput(f"Invalid syntax{str_pos}: {short_text}{expected}.")

    # Raise if not any expected_keywords is found
    # In context=None case, we always want to raise
    if not_match_count == len(expected_keywords):
        raise InvalidClientInput(f"Invalid syntax{str_pos}: {short_text}{expected}.")

    # Tell to the parser that it does not match, it's okay.
    return text, SyntaxError()


class SyntaxErrorChecker(str):
    """
    This class is put in the parser in some key places to generate precise syntax errors.
    Some keywords are actually expected and allowed, depending on the context / the place
    in the query, in other cases, it will raise an error.
    """
    expected_keywords = set()

    def __init__(self, context):
        # Right after a group/aggreg/sort, we can do an aggreg/sort, in other words, in all cases
        # Warning: aggregation has numerous types / keywords
        keywords = set(AGGREG_TYPES) | {"sort"}

        if context == "group":
            # After a group, we can combine to another group or close the group
            keywords |= {"and", "or", ")"}

            # A group can be inside an aggregation with "where" syntax, without any bracket.
            # Thus we have to add aggregation parameters in expected keywords
            # Note that "where" syntax for sort query are in bracket,
            # thus no additional keywords is needed
            keywords |= set(AGGREG_PARAMETER_MAPPING.keys())

        if context == "aggreg":
            # An aggregation can be inside a master aggregation, delimited by brackets.
            keywords |= {")"}

        # A sort can not be inside another sort or aggregation, thus brackets are not expected.

        self.expected_keywords = keywords


    def parse(self, parser, text, pos=None):
        return syntaxerror_parser(parser, text, pos=pos, expected_keywords=self.expected_keywords)


class Error(str):
    """
    This class is put in the parser in some key places to generate precise syntax errors.
    We use this class when the syntax has already partialy matched and we always want to raise
    an error with remaining text.
    """
    name = None

    def __init__(self, name):
        self.name = name

    def parse(self, parser, text, pos=None):
        return syntaxerror_parser(parser, text, pos=pos, name=self.name)


##########################################################################
# GRAMMAR BASIS
##########################################################################


class Value(str):
    """ General value definition """
    grammar = [
        re.compile(r'"""((?!""").)*"""'),
        re.compile(r'""((?!"").)*""'),
        re.compile(r'"((?!").)*"'),
        re.compile(r"'''((?!''').)*'''"),
        re.compile(r"''((?!'').)*''"),
        re.compile(r"'((?!').)*'"),
        re.compile(r'[\w\d\-\_\.\#\@/*]+')
    ]

class Values(str):
    grammar = (
        re.compile("\["),
        attr("values",
             (
                 Value,
                 maybe_some(re.compile(","), [Value, Error("value for list of values")])
             )
        ),
        re.compile("\]")
    )

class Comparator(str):
    """ Allow comparator in filters """
    grammar = [
        re.compile(r'(!=|!~|>=|>|<=|<|=|~)'),
        (re.compile("prefix", re.IGNORECASE), blank),
        (re.compile("nprefix", re.IGNORECASE), blank),
        (re.compile("not", re.IGNORECASE), blank, re.compile("prefix", re.IGNORECASE), blank)
    ]

class NumericalComparator(str):
    """ Allow numerical comparator in filters """
    grammar = re.compile(r'(>=|>|<=|<)')

class InComparator(str):
    """ Allow in value comparators """
    grammar = [
        re.compile("in", re.IGNORECASE),
        re.compile("nin", re.IGNORECASE),
        (re.compile("not", re.IGNORECASE), blank, re.compile("in", re.IGNORECASE))
    ]

class RangeComparator(str):
    """ Allow range value comparators """
    grammar = [
        re.compile("range", re.IGNORECASE),
        re.compile("nrange", re.IGNORECASE),
        (re.compile("not", re.IGNORECASE), blank, re.compile("range", re.IGNORECASE))
    ]

class Operator(str):
    """ Allow operators between filters """
    grammar = [
        re.compile("and", re.IGNORECASE),
        re.compile("or", re.IGNORECASE)
    ]

class Order(str):
    """ Allow sort order """
    grammar = [
        re.compile("asc", re.IGNORECASE),
        re.compile("desc", re.IGNORECASE)
    ]

class Name(str):
    """ Name grammar """
    grammar = re.compile(r"[\w\d\.\-\_]+")

class Integer(str):
    """ Integer grammar """
    grammar = re.compile(r"\d+")

class FieldPath(str):
    """ Field path for filters and aggregations """
    grammar = re.compile(r"[\w\-\.]+")


##########################################################################
# FILTER GRAMMAR
##########################################################################

class QueryString(str):
    """ Use as shorcut to query content with elastic query_string syntax """
    grammar = [
        re.compile(r'"""((?!""").)*"""'),
        re.compile(r'""((?!"").)*""'),
        re.compile(r'"((?!").)*"'),
        re.compile(r"'''((?!''').)*'''"),
        re.compile(r"''((?!'').)*''"),
        re.compile(r"'((?!').)*'")
    ]

class RangeValue(str):
    grammar = (
        re.compile("\("),
        attr("first_comparator", [NumericalComparator, Error("numerical comparator")]),
        attr("first_value", [Value, Error("numerical or date value")]),
        re.compile(","),
        attr("second_comparator", [NumericalComparator, Error("numerical comparator")]),
        attr("second_value", [Value, Error("numerical or date value")]),
        re.compile("\)")
    )

class Filter(str):
    """ Defined filter grammar bellow """
    pass

class RangeFilter(str):
    """ Defined range filter grammar bellow """
    pass

class Not(str):
    """ Defined 'not' grammar bellow """
    pass

class Context(str):
    """ Defined context grammar bellow """
    pass

class Group(List):
    """ Defined group of filter grammar bellow """
    pass

class NoBracketGroup(List):
    """ Group without bracket for main level query part """
    pass

class QueryElement(List):
    """ All possible elements in query part """
    grammar = (
        attr("subelements", [Group, Not, RangeFilter, Filter, Context, QueryString]),
        optional(SyntaxErrorChecker("group"))
    )

NoBracketGroup.grammar = attr("subelements", (
    QueryElement,
    maybe_some(
        Operator,
        blank,
        [QueryElement, Error("query after and/or")]
    )
))

Group.grammar = (
    ignore(re.compile("\(")),
    attr("subelements", NoBracketGroup),
    ignore(re.compile("\)"))
)

Not.grammar = (
    ignore(re.compile("not", re.IGNORECASE)),
    blank,
    attr("query", QueryElement)
)

Filter.grammar = (
    attr("field", FieldPath),
    blank,
    [
        (
            attr("comparator", Comparator),
            attr("value", [Value, Error("value after comparator")])
        ),
        (
            attr("comparator", InComparator),
            blank,
            attr("values", [Values, Error('"in values" after "in" comparator')])
        ),
        (
            attr("comparator", RangeComparator),
            blank,
            attr("values", [RangeValue, Error('range values after "range" comparator')])
        ),
    ],
    optional(
        re.compile("where", re.IGNORECASE),
        blank,
        attr("where", [QueryElement, Error('query after "where"')])
    )
)

RangeFilter.grammar = (
    attr("first_value", Value),
    attr("first_comparator", NumericalComparator),
    attr("field", FieldPath),
    attr("second_comparator", NumericalComparator),
    attr("second_value", [Value, Error("numerical or date value")]),
    optional(
        re.compile("where", re.IGNORECASE),
        blank,
        attr("where", [QueryElement, Error('query after "where"')])
    )
)

Context.grammar = (
    attr("field", FieldPath),
    blank,
    re.compile("where", re.IGNORECASE),
    blank,
    attr("where", [Group, Error('bracketed query after "where"')])
)


##########################################################################
# META GRAMMAR
##########################################################################

class AggregType(str):
    """ Allow types of aggregations """
    grammar = [re.compile(atype, re.IGNORECASE) for atype in AGGREG_TYPES]

class BracketAggreg(str):
    """ Bracket sub-aggregation used defined bellow """
    pass

class SubAggreg(str):
    """ Sub Aggragtion grammar """
    grammar = (
        attr("name", Name),
        attr("aggreg", [BracketAggreg, Error("bracketed aggregation for subaggregation")])
    )

class AggregParameter(str):
    """ Aggregation parameters, keyword and grammar """
    grammar = [
        (
            re.compile("subaggreg", re.IGNORECASE),
            blank,
            attr("subaggreg", [SubAggreg, Error('name and bracketed aggreg for subaggreg')])
        ),
        (
            re.compile("interval", re.IGNORECASE),
            blank,
            attr("interval", [Value, Error("interval value")])
        ),
        (
            re.compile("size", re.IGNORECASE),
            blank,
            attr("size", [Integer, Error('integer after "size"')])
        ),
        (
            re.compile("under", re.IGNORECASE),
            blank,
            attr("under", [FieldPath, Error('field path after "under"')])
        ),
        (
            re.compile("where", re.IGNORECASE),
            blank,
            attr("where", [QueryElement, Error('query after "where"')])
        ),
        (
            re.compile("graph", re.IGNORECASE),
            blank,
            attr("graph", [re.compile(r"\w+"), Error('query after "graph"')])
        )
    ]

class Aggreg(str):
    """ Aggregations grammar, parameters can be placed in any order """
    grammar = (
        attr("aggreg_type", AggregType),
        blank,
        optional(attr("name", Name)),
        ignore(re.compile(":")),
        attr("field", [FieldPath, Error("field path for aggregation")]),
        attr("parameters", maybe_some(blank, AggregParameter)),
        optional(SyntaxErrorChecker("aggreg"))
    )

BracketAggreg.grammar = (
    ignore(re.compile("\(")),
    attr("subelements", Aggreg),
    ignore(re.compile("\)"))
)

class SortParameter(str):
    """ Aggregation parameters, keyword and grammar """
    grammar = [
        (
            re.compile("seed", re.IGNORECASE),
            blank,
            attr("seed", [Integer, Error('integer after "seed"')])
        ),
        (
            re.compile("mode", re.IGNORECASE),
            blank,
            attr("mode", [Name, Error("mode name")])
        ),
        (
            re.compile("under", re.IGNORECASE),
            blank,
            attr("under", [FieldPath, Error('field path after "under"')])
        ),
        (
            re.compile("where", re.IGNORECASE),
            blank,
            attr("where", [QueryElement, Error('bracketed query after "where"')])
        )
    ]

class Sort(str):
    """ Sort grammar """
    grammar = (
        re.compile("sort", re.IGNORECASE),
        ignore(re.compile(":")),
        attr("field", [FieldPath, Error("field path for sort query")]),
        optional(blank, attr("order", Order)),
        attr("parameters", maybe_some(blank, SortParameter)),
        optional(SyntaxErrorChecker("sort"))
    )

class Query(List):
    """ Full query grammar """
    grammar = (
        optional(attr("query", NoBracketGroup)),
        optional(attr("aggreg", maybe_some(blank, Aggreg))),
        optional(attr("sort", maybe_some(blank, Sort)))
    )


##########################################################################
# RUN
##########################################################################

def unexpect_manager(input_string, remaining):
    """ Generate error message if query is not fully parsed """
    consumed = input_string[:-len(remaining)]

    lines = consumed.split("\n")
    str_pos = f"at line {len(lines)}, column {len(lines[-1])}"

    remaining = re.sub(r"[ \t]+", " ", remaining.strip())
    if not remaining:
        return

    after = ""
    if consumed:
        last_consumed_token = re.sub(r"[ \t]+", " ", consumed.strip()).split()
        after = f" after '{last_consumed_token[-1]}'"

    first_line_remaining = remaining.strip().split("\n")[0]
    remaining_light = first_line_remaining[:40]
    if len(remaining_light) < len(remaining):
        remaining_light += "..."
    return (f"Unable to parse {str_pos}{after}: \"{remaining_light}\"")


def parse(input_string, grammar=Query):
    """ Parse the whole query """
    parser = Parser()
    remaining, obj = parser.parse(input_string, grammar)
    unexpect = unexpect_manager(input_string, remaining)
    if unexpect:
        raise InvalidClientInput(unexpect)

    return obj
