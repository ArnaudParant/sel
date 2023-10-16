import json
import copy
import time
import datetime
import re
import logging

from . import schema_reader, utils, date_utils
from .schema_reader import SchemaReader
from .utils import InternalServerError, InvalidClientInput


NEGATIVE_COMPARATOR = ["!=", "!~", "nin", "nprefix", "nrange"]

NUMERICAL_AGGREG_TYPES = ["histogram", "min", "max", "sum", "average", "stats"]

AGGREG_TYPES = ["aggreg", "count", "distinct"] + NUMERICAL_AGGREG_TYPES

AGGREG_TYPE_MAPPING = {
    "count": "value_count",
    "distinct": "cardinality",
    "average": "avg",
    "stats": "extended_stats"
}

SORT_MODES = ["min", "max", "sum", "avg", "median"]

EXTENDED_QUERY_KEYS = [
    "_source", "fields", "script_fields", "fielddata_fields", "explain", "highlight", "rescore",
    "version", "indices_boost", "min_score"
]

COMPARATORS_MAPPING = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte", "=": "eq"}


class QueryGenerator:

    def __init__(self, conf, schema, log_level=logging.INFO):
        self.logger = logging.getLogger("query_generator")
        self.logger.setLevel(log_level)

        self.conf = conf
        self.schema_reader = SchemaReader(conf, schema)


################################################################################
####### Queries
################################################################################

    def format_query_exists(self, nested, field, query):
        """
        Build exists function query
        """
        value = to_boolean(field["str_path"], query["value"])
        query["comparator"] = query.get("comparator", "=")

        if query["comparator"] != "=":
            raise InvalidClientInput(f"exists: '{field['str_path']}' may only use comparator = or !=")
        query = format_nested_query(nested, {"exists": {"field": field["str_path"]}})
        if value == False:
            query = {"bool": {"must_not": [query]}}
        return query


    def format_query_missing(self, nested, field, query):
        """
        Build missing function query
        """
        value = to_boolean(field["str_path"], query["value"])
        query["comparator"] = query.get("comparator", "=")

        if query["comparator"] != "=":
            raise InvalidClientInput(f"missing: '{field['str_path']}' may only use comparator = or !=")
        query = format_nested_query(nested, {"missing": {"field": field["str_path"]}})
        if value == False:
            query = {"bool": {"must_not": [query]}}
        return query


    def format_query_filter(self, warns, group_nested, item):
        """
        Warning: Modify warns without returning it
        """
        field = self.schema_reader.get_field_info(item["field"], functions=True, nested=group_nested)
        nested = field["nested"] if field["str_nested"] != group_nested else None
        item["comparator"] = item.get("comparator", "=").lower()
        value = item["value"]

        if field["function"] == "exists":
            return field["str_path"], self.format_query_exists(nested, field, item)
        elif field["function"] == "missing":
            return field["str_path"], self.format_query_missing(nested, field, item)

        if item["comparator"] == "in" and not isinstance(value, list):
            raise InvalidClientInput("Value of in or nin comparator MUST be a list")
        if item["comparator"] == "range" and not isinstance(value, dict):
            raise InvalidClientInput("Value of range or nrange comparator MUST be a dict")
        if item["comparator"] not in ["in", "range"] and isinstance(value, (list, dict)):
            raise InvalidClientInput("Value MUST be a string, int, float or boolean")

        field_type = field["element"]["type"]
        value = boolean_manager(field_type, value, field["str_path"])
        value = numerical_manager(field_type, value, field["str_path"])

        if item["comparator"] == "=" and field_type != "date":
            return field["str_path"], format_nested_query(nested, {"term": {field["str_path"]: value}})

        elif item["comparator"] == "~":
            if field_type != "string":
                raise InvalidClientInput(f"'{field['str_path']}' Only string field may use such comparator ~ or !~")
            return field["str_path"], format_nested_query(nested, self.format_query_string(group_nested, item, path=field["str_path"]))

        elif item["comparator"] == "in":
            return field["str_path"], format_nested_query(nested, {"terms": {field["str_path"]: value}})

        elif item["comparator"] == "prefix":
            return field["str_path"], format_nested_query(nested, {"prefix": {field["str_path"]: value}})

        if not is_numerical(field_type):
            raise InvalidClientInput(f"'{field['str_path']}' Only numerical field may use such comparator (range, >, >=, <, <=): {item['comparator']}")


        if field_type == "date":
            range_query = format_date_query(item["comparator"], value, self.conf["Queries"]["TimeZone"])

        else:
            if item["comparator"] == "range":
                range_query = {}
                for sub_comparator, sub_value in value.items():
                    range_query[to_elastic_comparator(sub_comparator)] = sub_value

            else:
                comparator_name = to_elastic_comparator(item["comparator"])
                range_query = {comparator_name: value}


        return field["str_path"], format_nested_query(nested, {"range": {field["str_path"]: range_query}})


    def format_where_query_filter(self, warns, group_nested, group_aggreg, item):
        """
        Build where in filter query

        Warning: Modify warns without returning it
        """
        if not item.get("where"):
            _, query = self.format_query_filter(warns, group_nested, item)
            return query

        field_path, query = self.format_query_filter(warns, group_nested, item)
        parent_nested = query["nested"]["path"] if "nested" in query else None

        # Allow to do 'where' even if it's not necessary
        current_nested = parent_nested if parent_nested else group_nested

        # Format sub 'where' query
        where = self.format_query_group(
            warns, item["where"], nested=current_nested, group_aggreg=group_aggreg
        )

        if not current_nested:
            # If there is no parent nested context neither an higher nested group
            raise InvalidClientInput(
                "WhereFilter can only be used on nested fields, use and/or instead."
            )

        parent_field_path = re.sub(r"\.[^\.]+$", "", field_path)
        if parent_nested and parent_nested != group_nested:
            # If there is a parent nested context

            if query["nested"]["path"] != parent_field_path:
                # Return a warning when the nested context is not the first parent
                # Because it can be an unexpected behavior
                warns.append(
                    f"WhereFilter is applied on .{query['nested']['path']} "
                    f"for field .{field_path}"
                )

            must = [query["nested"]["query"], where]
            query["nested"]["query"] = {"bool": {"must": must}}

        else:
            # If there is no parent nested context but an higher nested group
            # It allowed to simplify query usage, but "where" query is not needed here
            query = {"bool": {"must": [query, where]}}
            warns.append(
                f"Nested 'where' filter is not necessary here on object .{parent_field_path}"
            )

        return query


    def format_context(self, warns, group_nested, group_aggreg, item):
        """
        Build context query

        Warning: Modify warns without returning it
        """
        field = self.schema_reader.get_field_info(item["field"], nested=group_nested)
        current_nested = field["str_nested"] if field["str_nested"] else group_nested
        query = self.format_query_group(warns, item["where"], nested=current_nested,
                                        group_aggreg=group_aggreg)
        if not field["nested"]:
            raise InvalidClientInput(
                "Context query can only be used on nested fields, use and/or instead."
            )

        parent_field_path = re.sub(r"\.[^\.]+$", "", field["str_path"])
        if group_nested == field["str_nested"]:
            warns.append(
                f"Nested context filter is not necessary here on object .{parent_field_path}"
            )
            return query

        return format_nested_query(field["nested"] , query)


    def format_not_query(self, warns, query, nested, group_aggreg):
        sub_query = query
        count = 0

        while "not" in sub_query:
            sub_query = query["not"]
            count += 1

        results = self.__format_query_mapper(warns, sub_query, nested, group_aggreg)

        if count % 2:
            return {"bool": {"must_not": [results]}}

        return results


    def __format_query_mapper(self, warns, query, nested, group_aggreg):
        """ """
        if not query:
            res = None
        elif query.get("not"):
            res = self.format_not_query(warns, query, nested=nested,
                                        group_aggreg=group_aggreg)
        elif query.get("operator"):
            res = self.format_query_group(warns, query, nested=nested,
                                          group_aggreg=group_aggreg)
        elif query.get("query_string"):
            res = self.format_query_string(nested, query)
        elif "value" not in query:
            res = self.format_context(warns, nested, group_aggreg, query)
        else:
            res = self.format_where_query_filter(warns, nested, group_aggreg, query)
        return res


    def format_query_group(self, warns, query, nested=None, group_aggreg=None,
                           top_level=False):
        """
        Warning: Modify warns without returning it
        """
        if not query:
            return {"match_all": {}} if top_level else None
        if isinstance(query, list):
            raise InvalidClientInput(f"Invalid query type: list\nQuery: {json.dumps(query)}")

        queries = []
        not_queries = []
        for item in query.get("items", [query]):

            copy_item = copy.deepcopy(item)
            if item.get("comparator", "") in NEGATIVE_COMPARATOR:
                copy_item["comparator"] = item["comparator"][1:]

            sub_query = self.__format_query_mapper(warns, copy_item, nested=nested,
                                                   group_aggreg=group_aggreg)
            if sub_query is not None:
                if item.get("comparator", "") in NEGATIVE_COMPARATOR:
                    not_queries.append(sub_query)
                else:
                    queries.append(sub_query)

        if not queries and not not_queries:
            return {"match_all": {}} if top_level else None
        elif len(queries) == 1 and len(not_queries) == 0:
            return queries[0]

        es_query = {}
        operator = query.get("operator", "and").lower()
        if operator == "and":
            if queries:
                es_query["must"] = queries
            if not_queries:
                es_query["must_not"] = not_queries

        elif operator == "or":
            es_query["should"] = []
            if queries:
                es_query["should"] = queries
            if not_queries:
                es_query["should"] += [{"bool": {"must_not": q}} for q in not_queries]
            if not es_query["should"]:
                del es_query["should"]

        else:
            raise InvalidClientInput(f"Invalid operator: '{operator}'")

        return {"bool": es_query}




################################################################################
####### Aggregations
################################################################################

    def format_aggreg(self, warns, field, aggreg):
        """
        Build the query aggreg
        - check type and values
        - apply parameters
        """
        path = field["str_path"]
        aggreg = aggreg_set_default_parameter(field, aggreg, self.conf)

        aggreg_data = {
            "field": field,
            "query_field": schema_reader.path_to_pretty(field["query_field"]),
            "aggreg": copy.deepcopy(aggreg)
        }

        if aggreg["size"] > 0:
            aggreg["size"] += 1

        if aggreg["type"] not in AGGREG_TYPES:
            raise InvalidClientInput(
                f"aggreg: {path}, only {','.join(AGGREG_TYPES)} are allowed as aggregation type"
            )

        query = {"terms": {"field": path, "size": aggreg["size"]}}
        if aggreg["type"] != "aggreg":
            aggreg_obj = {"field": path}

            if aggreg["type"] in NUMERICAL_AGGREG_TYPES and \
               not is_numerical(field["element"]["type"]):
                raise InvalidClientInput(f"aggreg: {path}, only numerical fields might use such aggregation type")

            if aggreg["type"] in AGGREG_TYPE_MAPPING:
                aggreg["type"] = AGGREG_TYPE_MAPPING[aggreg["type"]]

            if aggreg["type"] == "cardinality":
                aggreg_obj["precision_threshold"] = 40000

            query = {aggreg["type"]: aggreg_obj}

        if aggreg["type"] == "histogram":
            ## aggreg: date is set to histogram in aggreg_set_default_parameter
            query = self.format_aggreg_histogram(path, field, aggreg.get("interval"))
        elif aggreg.get("interval") is not None:
            raise InvalidClientInput(f"aggreg: {path}, interval is only available for histogram")

        if aggreg.get("subaggreg"):
            for subname, subaggreg in aggreg["subaggreg"].items():
                query = self.format_subaggreg(warns, query, field, subname, subaggreg, aggreg_data)

        return query, aggreg_data


    def format_aggreg_histogram(self, path, field, interval):
        """
        Build date and numerical histogram aggregation
        """
        aggreg_type = "histogram"
        query_obj = {"field": path}

        if field["element"]["type"] == "date":

            ## default interval is setted in aggreg_set_default_parameter
            aggreg_type = "date_histogram"
            query_obj["format"] = "yyyy-MM-dd"

        else:
            if interval is None:
                raise InvalidClientInput(f"histogram: {path}, interval is mandatory for numerical histogram")
            try:
                interval = int(interval)
            except ValueError:
                raise InvalidClientInput(f"histogram: {path}, interval '{interval}' must be an int")

        query_obj["interval"] = interval
        return {aggreg_type: query_obj}


    def format_subaggreg(self, warns, query, field, subname, subaggreg, aggreg_data):
        """ Manage aggreg by parameter """
        subfield = self.schema_reader.get_field_info(subaggreg["field"])

        subquery, subaggreg_data = self.format_aggreg(warns, subfield, subaggreg)

        subquery = self.format_aggreg_under_and_where(
            warns, subquery, subfield, subaggreg, parent_nested=field["nested"])

        if "aggs" not in query:
            query["aggs"] = {}

        query["aggs"][subname] = subquery

        if "subaggreg" not in aggreg_data:
            aggreg_data["subaggreg"] = {}
        aggreg_data["subaggreg"][subname] = subaggreg_data

        return query


    def __aggreg_where(self, warns, aggreg, query, context_str_nested, query_data=None):
        """
        Extract and apply filter (where) on aggreg if specify
        Info: Nested aggreg can be done before, to allow parent nested or global filtering

        Warning: Modify warns without returning it
        """
        if aggreg.get("where"):
            sub = copy.deepcopy(query)
            sub_filter = self.format_query_group(warns, aggreg["where"],
                                                 nested=context_str_nested,
                                                 group_aggreg=True)
            sub_filter = query_map(lambda f: aggreg_nested_formater(context_str_nested, f), sub_filter)
            query = {"filter": sub_filter, "aggs": {"sub": sub}}

            if query_data is not None:
                where = copy.deepcopy(flatten_query(aggreg["where"]))
                for elm in where:
                    elm["field"] = self.schema_reader.get_field_info(
                        elm["field"],
                        nested=context_str_nested,
                        functions=True)
                query_data["where"] = where

        return query


    def single_aggreg(self, warns, queries, aggreg, key, query_data=None):
        """
        Aggreg manager setup data to build the actual aggregation query
        + manage aggreg where

        Warning: Modify warns and query_data without returning it
        """
        field = self.schema_reader.get_field_info(aggreg["field"])
        aggreg = aggreg_set_default_parameter(field, aggreg, self.conf)

        query, aggreg_data = self.format_aggreg(warns, field, aggreg)

        query = self.format_aggreg_under_and_where(warns, query, field, aggreg, query_data=aggreg_data)

        # Allow multiple aggreg on same field
        queries[key] = query

        if query_data is not None:
            query_data["aggregations"][key] = aggreg_data

        return queries


    def format_aggreg_under_and_where(self, warns, query, field, aggreg,
                                      parent_nested=None, query_data=None):
        """ Set the proper nested context according to <under> and <field> parameters
            to apply the <where> parameter
        """
        under_nested = None
        if aggreg.get("under", ".") != ".":
            under_nested = self.schema_reader.get_field_info(aggreg["under"])

        str_under_nested = under_nested.get("str_nested") if under_nested else ""
        str_field_nested = field.get("str_nested", "")

        # if under is setted and different of the aggreg nested context
        if aggreg.get("under") and str_under_nested != str_field_nested:

            if not str_field_nested.startswith(str_under_nested):
                raise InvalidClientInput(f"Nested context '{str_field_nested}' MUST BE child of under context '{str_under_nested}'.")

            safe_under_nested = under_nested["nested"] if under_nested else None
            safe_str_under_nested = under_nested.get("str_nested") if under_nested else None

            reverse = False
            if parent_nested and \
               not is_parent_nested(parent_nested, safe_under_nested) and \
               parent_nested != safe_under_nested:
                reverse = True

            query = nested_aggreg(query, field.get("str_nested"))
            query = self.__aggreg_where(
                warns, aggreg, query, safe_str_under_nested, query_data=query_data)

            if safe_under_nested != parent_nested:
                query = nested_aggreg(query, safe_str_under_nested, reverse=reverse)

        else:

            reverse = False
            if field["nested"]:
                if parent_nested and \
                   not is_parent_nested(parent_nested, field["nested"]) and \
                   parent_nested != field["nested"]:
                    reverse = True

            elif parent_nested is not None:
                reverse = True

            query = self.__aggreg_where(
                warns, aggreg, query, field.get("str_nested"), query_data=query_data)

            if field["nested"] != parent_nested:
                query = nested_aggreg(query, field.get("str_nested"), reverse=reverse)

        return query



    def format_aggregations(self, warns, aggregs, query=None, query_data=None):
        """
        Manage all aggregations

        Warning: Modify warns and query_data without returning it
        """
        if query is None:
            query = {}
        if query_data is not None and "aggregations" not in query_data:
            query_data["aggregations"] = {}

        for key, aggreg in aggregs.items():
            query = self.single_aggreg(warns, query, aggreg, str(key),
                                       query_data=query_data)

        return query


################################################################################
####### Sort
################################################################################

    def format_sorts(self, warns, sorts):
        """
        Build sort query

        Warning: Modify warns without returning it
        """
        sub_properties = self.conf["Queries"]["DefaultObjectSortField"].split(",")

        sorts_queries = []
        for item in sorts:
            self.logger.debug("sort: %s" % json.dumps(item))

            field = self.schema_reader.get_field_info(
                item["field"],
                sub_properties=sub_properties
            )

            if "seed" in item:
                raise InvalidClientInput(f"Invalid seed field for standart sort.\Item: {json.dumps(item)}")

            mode = item.get("mode", "avg").lower()
            if mode not in SORT_MODES:
                raise InvalidClientInput(
                    f"Invalid sort mode: {mode}. Allow modes: {', '.join(SORT_MODES)}"
                )

            query = {
                "order": item.get("order", "desc").lower(),
                "mode": mode
            }
            where = item.get("where")
            item_warns = []

            # manage where conditioning by the under nested context
            under_nested = None
            if item.get("under", ".") != ".":
                under_nested = self.schema_reader.get_field_info(item["under"]).get("str_nested")

            query["nested_path"] = under_nested if under_nested else field.get("str_nested")

            # if under is setted and different of the nested context
            if item.get("under") and under_nested != field.get("str_nested"):

                if not field.get("str_nested", "").startswith(under_nested if under_nested else ""):
                    raise InvalidClientInput(f"Nested context ({field.get('str_nested', '')}) MUST BE child of under context ({under_nested if under_nested else ''}).")

                where_query = self.format_query_group(item_warns,
                                                      where,
                                                      nested=under_nested,
                                                      group_aggreg=True)

            else:
                where_query = self.format_query_group(item_warns,
                                                      where,
                                                      nested=field["str_nested"],
                                                      group_aggreg=True)

            if not item.get("auto_sort"):
                warns += item_warns

            query["nested_filter"] = where_query

            sorts_queries.append({field["str_path"]: query})

        return sorts_queries


    def auto_sort_generator(self, query):
        """
        Generate auto-sorting in function of the query
          1. Flatten all filters out of groups and operators
          2. Forward where inside generated sort
          3. Do sort on first 3 found fields
        """
        sub_properties = self.conf["Queries"]["DefaultObjectSortField"].split(",")

        def gen_sort(query_filter):
            query_field = query_filter["field"]
            original_field = self.schema_reader.get_field_info(query_field,
                                                               sub_properties=sub_properties,
                                                               can_raise=False)
            if original_field.get("error"):
                return None

            copy_filter = copy.deepcopy(query_filter)
            where = copy_filter
            if "where" in copy_filter:

                if "value" not in copy_filter:

                    while "where" in copy_filter and "value" not in copy_filter:

                        copy_filter = copy_filter["where"]
                        if copy_filter.get("items"):
                            copy_filter = copy_filter["items"][0]

                        if "field" in copy_filter:
                            query_field = copy_filter["field"]
                            sub_field = self.schema_reader.get_field_info(
                                query_field,
                                sub_properties=sub_properties,
                                can_raise=False
                            )
                            if sub_field.get("error"):
                                return None

                else:
                    where = {
                        "operator": "and",
                        "items": [copy_filter] + [copy_filter["where"]]
                    }
                    del copy_filter["where"]

            obj = {
                "auto_sort": True,
                "field": query_field,
                "where": where
            }
            if original_field.get("str_nested"):
                obj["under"] = original_field["str_nested"]

            self.logger.debug("auto_sort: %s" % json.dumps(obj))
            return obj

        flatten = flatten_query(query)
        new_auto_sorts = [gen_sort(q) for q in flatten if q.get("field")]
        return [s for s in new_auto_sorts[:3] if s is not None]


    def build_random_sort(self, body, random_seed):
        """ Build random sort format """
        body["query"] = {"function_score" : {
            "query": body["query"],
            "random_score": {"seed": random_seed}
        }}
        return body


################################################################################
####### Generate Query
################################################################################

    def generate_query(self, warns, data):
        """
        Warning: Modify warns without returning it
        """
        data = copy.deepcopy(data)
        self.logger.debug("input query: %s" % json.dumps(data))

        query = data.get("query")
        meta = data["meta"] if data.get("meta") else {}

        # First sort are the must important
        # Does not do auto sort if available sort is setted in input query
        sorts = data["sort"] if data.get("sort") else []
        sorts, auto_sort, random_seed = sort_query_controller(self.conf, sorts)

        if auto_sort and not sorts:
            sorts += self.auto_sort_generator(query)

        query_data = {}
        body = {"query": self.format_query_group(warns, query, top_level=True)}
        if data.get("aggregations"):
            body["aggregations"] = self.format_aggregations(
                warns, data["aggregations"], query_data=query_data
            )
        if sorts:
            body["sort"] = self.format_sorts(warns, sorts)

        if random_seed:
            body = self.build_random_sort(body, random_seed)

        body = utils.set_if_exists(meta, body, ["from", "size"])
        body = utils.set_if_exists(data.get("extended"), body, EXTENDED_QUERY_KEYS)

        return body, query_data


    def format_query_string(self, group_nested, query, path=None):
        """
        Build query from key word, the simpliest syntax
        """
        path = path if path else self.conf["Queries"]["DefaultQueryStringFieldPath"]

        if group_nested and not path.startswith(group_nested):
            raise InvalidClientInput(f"'.{path}' field must be under nested object: '.{group_nested}'")
        query_string = query.get("query_string", query.get("value"))
        if query_string is None:
            raise InternalServerError(f"Failed to extract query string from filter: {json.dumps(query)}")
        return {"query_string": {"query": query_string, "fields": [path]}}




################################################################################
### Utils
################################################################################

def format_nested_query(nested, query):
    if nested != None:
        query = {"nested": {"path": schema_reader.path_to_string(nested), "query": query}}
    return query


def aggreg_nested_formater(context_str_nested, field):
    """
    Check if field is under nested context and set it under
    """
    if field.get("nested"):
        field_str_nested = field["nested"]["path"]
        field_nested = field_str_nested.split(".")
        context_nested = context_str_nested.split(".") if context_str_nested else []
        parent_nested = field_nested[:len(context_nested)]
        if context_nested and parent_nested != context_nested:
            raise InvalidClientInput(f"Where Aggregations must be under the same nested context: '{field_str_nested}' is not under '{context_str_nested}'")
        if field_nested == context_nested:
            field = field["nested"]["query"]
    return field


def format_date_query(comparator, value, time_zone):
    """
    Elastic Search translate date to timestamp in second.
    Then comparator on date does not works as expected.
    '>=' is almost like '>' (less one second)
    This wrapper fix date comparators
    """
    range_query = {"format": date_utils.ELASTIC_DATE_FORMAT, "time_zone": time_zone}

    if comparator == "range":
        for sub_comparator, sub_value in value.items():
            range_query.update(format_date_query(sub_comparator, sub_value, time_zone))

    else:
        comparator_name = to_elastic_comparator(comparator)
        date, date_format = date_utils.str_date_to_datetime(value)
        if comparator_name == "gt":
            date = date_utils.date_add_to_last_element(date, 1, date_format)
            range_query["gte"] = datetime.datetime.strftime(date, date_format)

        elif comparator_name == "lte":
            date = date_utils.date_add_to_last_element(date, 1, date_format)
            range_query["lt"] = datetime.datetime.strftime(date, date_format)

        elif comparator_name == "eq":
            range_query["gte"] = value
            date = date_utils.date_add_to_last_element(date, 1, date_format)
            range_query["lt"] = datetime.datetime.strftime(date, date_format)

        else:
            range_query[comparator_name] = value

    return range_query


def is_numerical(t):
    return t in ["float", "integer", "long", "double", "date"]


def to_boolean(path, value):
    if isinstance(value, bool):
        return value
    if value == "1" or value.lower() == "true":
        return True
    elif value == "0" or value.lower() == "false":
        return False
    raise InvalidClientInput(f"'{path}' Invalid value for boolean '{value}'")


def boolean_manager(field_type, value, path):
    if field_type == "boolean":
        if isinstance(value, list):
            return [to_boolean(path, v) for v in value]
        if isinstance(value, dict):
            return {k: to_boolean(path, v) for k, v in value.items()}
        return to_boolean(path, value)
    return value


def to_float(path, value):
    try:
        return float(value)
    except Exception as e:
        raise InvalidClientInput(f"'{path}' Invalid value for numerical '{value}'")


def numerical_manager(field_type, value, path):
    if is_numerical(field_type) and not field_type == "date":
        if isinstance(value, list):
            return [to_float(path, v) for v in value]
        if isinstance(value, dict):
            return {k: to_float(path, v) for k, v in value.items()}
        return to_float(path, value)
    return value


def query_map(func, query):
    """ Apply func on all first level filters """
    new_query = copy.deepcopy(query)
    if "bool" in query:
        if "must" in query["bool"]:
            new_query["bool"]["must"] = [func(q) for q in new_query["bool"]["must"]]
        if "should" in query["bool"]:
            new_query["bool"]["should"] = [func(q) for q in new_query["bool"]["should"]]
        if "must_not" in query["bool"]:
            new_query["bool"]["must_not"] = [func(q) for q in new_query["bool"]["must_not"]]
    else:
        new_query = func(new_query)
    return new_query


def to_elastic_comparator(symbol):
    if symbol in COMPARATORS_MAPPING:
        return COMPARATORS_MAPPING[symbol]
    raise InvalidClientInput(f"Unknown operator '{symbol}'")


def find_filter(data, str_field):
    filters = flatten_query(data.get("query"))
    found = [q for q in filters if str_field in q.get("field", "")]
    return found[0] if found else None


def top_insert_filter(data, operator, new_filter):
    data["query"] = data.get("query")

    items = [new_filter, data["query"]]
    items = [i for i in items if i]

    if len(items) == 1:
        data["query"] = items[0]
    elif len(items) == 0:
        del data["query"]
    else:
        data["query"] = {"operator": operator, "items": items}

    return data


def aggreg_set_default_parameter(field, aggreg, conf):
    """
    + Set default size
    + Set default type to aggreg
    + Set aggreg: date to histogram
    + Set histogram: date default interval
    """
    aggreg = copy.deepcopy(aggreg) if aggreg else {}
    aggreg["type"] = aggreg.get("type", "aggreg").lower()

    if field["element"]["type"] == "date" and aggreg["type"] == "aggreg":
        aggreg["type"] = "histogram"

    if aggreg["type"] == "histogram":
        aggreg["size"] = aggreg.get("size", 0)
        if field["element"]["type"] == "date" and "interval" not in aggreg:
            aggreg["interval"] = conf["Aggregations"]["DefaultDateInterval"]

    if "size" not in aggreg:
        aggreg["size"] = conf["Aggregations"].getint("DefaultSize")

    return aggreg


def sort_query_controller(conf, sorts):
    """
    1. Enable auto sort by value in configuration
    2. Can be overwrite by query with:
         sort: auto
         sort: null
    3. Enable random sort by custom query, sort: random
    4. Remove custom sorts (auto, null, random) from real query sorts
    """
    auto_sort = conf["Queries"].getboolean("AutoSort")
    random_seed = None
    remains = []

    for elm in sorts:

        if elm["field"] == "auto":
            auto_sort = True

        elif elm["field"] == "null":
            auto_sort = False

        elif elm["field"] == "random":
            random_seed = elm.get("seed", int(time.time()))
            auto_sort = False

        else:
            remains.append(elm)

    return remains, auto_sort, random_seed


def flatten_query(query):
    """
    Return a list of all filters, without group, operator.

    Warning: does not flat where
    """
    if not query:
        return []

    if query.get("operator") is None:
        return [query]

    filters = []
    for item in query["items"]:
        filters += flatten_query(item)
    return filters


def is_parent_nested(parent, should_be_child):
    if parent is None and should_be_child is not None:
        return True
    if should_be_child is None:
        return False
    if len(should_be_child) <= len(parent):
        return False
    return should_be_child[:len(parent)] == parent


def nested_aggreg(aggreg_query, str_nested, reverse=False):
    """
    Set nested or reverse in aggregations if necessary
    """
    if str_nested is None and not reverse:
        return aggreg_query

    if "aggs" not in aggreg_query or len(aggreg_query) > 1:
        aggreg_query = {"aggs": {"sub": aggreg_query}}

    nested_key = "nested" if not reverse else "reverse_nested"
    obj = {"path": str_nested} if str_nested is not None else {}
    aggreg_query[nested_key] = obj
    return aggreg_query
