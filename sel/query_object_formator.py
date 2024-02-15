from .query_string_parser import (
    Value, QueryString, Filter, RangeFilter, Not, Context, QueryElement, Group, NoBracketGroup,
    Comparator, Name, FieldPath, Aggreg, SubAggreg, BracketAggreg, Sort, Query, Operator,
    AGGREG_PARAMETER_MAPPING
)
from .utils import InternalServerError, InvalidClientInput


ALLOW_QUOTES = ['"""', '""', '"', "'''", "''", "'"]

SPECIAL_COMPARATORS = {
    "nin": ["not", "in"],
    "nrange": ["not", "range"],
    "nprefix": ["not", "prefix"],
}

REVERT_NUMERICAL_COMPARATORS = {
    ">": "<",
    ">=": "<=",
    "<": ">",
    "<=": ">=",
}

SORT_PARAMETER_MAPPING = {
    "seed": int,
    "mode": None,
    "under": None,
    "where": None,
}


def format_string(obj):
    return str(obj)


def format_value(obj):
    """ Removing quote wrapping, only used for parsing purpose """
    value = str(obj)
    for quote in ALLOW_QUOTES:
        if value.startswith(quote) and value.endswith(quote):
            quote_length = len(quote)
            return value[quote_length:-quote_length]
    return value


def format_query_string(obj):
    """ Same as Value, with ! syntax at the beginning for negation """
    res = {"query_string": format_value(obj)}
    return res


def format_filter(obj):
    """ Generate filter format """
    dic = {
        "field": str(obj.field),
        "comparator": str(obj.comparator).lower(),
    }

    for comp, extended_comp in SPECIAL_COMPARATORS.items():
        if str(obj.comparator).lower() == str(extended_comp):
            dic["comparator"] = comp

    if dic["comparator"] in ["in", "nin"]:
        dic["value"] = [format_value(v) for v in obj.values.values if isinstance(v, Value)]

    elif dic["comparator"] in ["range", "nrange"]:
        if obj.values.first_comparator == obj.values.second_comparator:
            raise InvalidClientInput("In range query, the both comparator MUST BE different.")
        dic["value"] = {
            obj.values.first_comparator: format_value(obj.values.first_value),
            obj.values.second_comparator: format_value(obj.values.second_value),
        }

    else:
        dic["value"] = format_value(obj.value)

    if hasattr(obj, "where"):
        dic["where"] = formator(obj.where)

    return dic


def format_range_filter(obj):
    """ Generate filter format """
    dic = {
        "field": str(obj.field),
        "comparator": "range",
    }

    first_comp = REVERT_NUMERICAL_COMPARATORS[obj.first_comparator]

    if first_comp == obj.second_comparator:
        raise InvalidClientInput("Invalid range filter comparators, both are identical after translation.")

    dic["value"] = {
        first_comp: format_value(obj.first_value),
        obj.second_comparator: format_value(obj.second_value),
    }

    if hasattr(obj, "where"):
        dic["where"] = formator(obj.where)

    return dic


def format_context(obj):
    """ Generate context format """
    return {
        "field": str(obj.field),
        "where": formator(obj.where)
    }


def format_class_container(obj):
    """ Basic container class, which only defined subelements attribute """
    return formator(obj.subelements)


def format_not(obj):
    """ 'Not' syntax formating """
    return {"not": formator(obj.query)}


def format_group(obj):
    """
    Generate group format, with priority on 'and' operator
    Meaning than 'and' groups are built before 'or' groups
    """
    group_and = []
    group_or = []       # Will contain list or group_and

    elements = obj
    while hasattr(elements, "subelements"):
        elements = elements.subelements

    for item in elements:

        if isinstance(item, Operator):
            if str(item) == "or":
                group_or.append(group_and)
                group_and = []

        elif isinstance(item, QueryElement):
            group_and.append(formator(item))

    # Adding potentiel remaining elements after the last 'or'
    group_or.append(group_and)

    def to_group(operator, items):
        if len(items) == 1:
            return items[0]
        return {"operator": operator, "items": items}

    groups = [to_group("and", g) for g in group_or if g]
    return to_group("or", groups)


def to_int(value, name=None):
    """ Convert a parameter value to int, raise on failure """
    try:
        return int(value)
    except ValueError:
        raise InvalidClientInput(f"{name} value MUST be an int, got: {value}")


def format_parameters(parameters, mapping):
    """ Used to format aggreg and sort parameters """
    obj = {}
    for parameter in parameters:
        for name in mapping.keys():
            if hasattr(parameter, name):

                param_type = mapping[name]
                func = to_int if param_type == int else formator
                result = func(getattr(parameter, name), name=name)

                if name != "subaggreg":
                    if name in obj:
                        raise InvalidClientInput(f"{name} can NOT be defined more than once.")
                    obj[name] = result

                else:
                    if "subaggreg" not in obj:
                        obj["subaggreg"] = {}
                    if result["name"] in obj["subaggreg"]:
                        raise InvalidClientInput(
                            f"Subaggreg name '{result['name']}' is already used")
                    obj["subaggreg"][result["name"]] = result["aggreg"]

    return obj


def format_aggreg(obj):
    """
    Generate aggregation format, and check syntax not managed by the grammar.

      1. Checking that parameters are not defined several times
      2. Check and convert size value to INT type
    """
    res = {"type": str(obj.aggreg_type).lower(), "field": str(obj.field)}

    if hasattr(obj, "name"):
        res["name"] = obj.name

    res.update(format_parameters(obj.parameters, AGGREG_PARAMETER_MAPPING))

    return res


def format_subaggreg(obj):
    return {"name": obj.name, "aggreg": formator(obj.aggreg)}


def format_sort(obj):
    """ Generate sort format """
    res = {"field": str(obj.field)}
    if hasattr(obj, "order"):
        res["order"] = str(obj.order)

    res.update(format_parameters(obj.parameters, SORT_PARAMETER_MAPPING))
    return res


def format_query(obj):
    """ Generate whole query format """
    res = {}

    if hasattr(obj, "query"):
        res["query"] = formator(obj.query)

    if hasattr(obj, "aggreg") and obj.aggreg:
        res["aggregations"] = {}

        for i, a in enumerate(obj.aggreg):
            aggreg = formator(a)

            name = "aggreg_%d" % i
            if "name" in aggreg:
                name = aggreg["name"]
                del aggreg["name"]

            if name in res["aggregations"]:
                raise InvalidClientInput(f"Aggreg name '{name}' is already used")

            res["aggregations"][name] = aggreg

    if hasattr(obj, "sort") and obj.sort:
        res["sort"] = [formator(a) for a in obj.sort]

    return res


TYPE_FORMAT_MAPPING = {
    Value: format_value,
    QueryString: format_query_string,

    Filter: format_filter,
    RangeFilter: format_range_filter,
    Context: format_context,
    QueryElement: format_class_container,
    Not: format_not,
    Group: format_group,
    NoBracketGroup: format_group,

    Comparator: format_string,
    Name: format_string,
    FieldPath: format_string,
    str: format_string,

    Aggreg: format_aggreg,
    SubAggreg: format_subaggreg,
    BracketAggreg: format_class_container,

    Sort: format_sort,

    Query: format_query,
}
def formator(obj, name=None):
    """
    Format the parsing output to internal object tree format.
    Thanks to the types and format functions above.
    """
    for obj_type, format_func in TYPE_FORMAT_MAPPING.items():
        if type(obj) == obj_type:
            return format_func(obj)

    raise InternalServerError(f"Invalid object type: {type(obj)}")
