import json
from difflib import SequenceMatcher
import copy

from . import meta
from .utils import InternalServerError, InvalidClientInput


ALLOWED_FUNCTIONS = ["exists"]


class SchemaError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class SchemaReader:


    def __init__(self, conf, schema):
        self.conf = conf
        self.schema = schema


    def get_field_info(self, field, sub_properties=None, functions=False,
                       nested=None, can_raise=True):
        """
        Return full information about a query field
        Can raise exceptions
        """
        if sub_properties is None:
            sub_properties = self.conf["Queries"]["DefaultObjectSubfield"].split(",")
        frac_field = field_to_fraction(field)
        frac_field, function = self.__function_detector(frac_field, functions, root=nested)
        fields = self.schema_finder(frac_field, root=nested)
        fields = self.__query_field_pretty(fields)

        # Not found
        if len(fields) == 0:
            under_str = (" under %s" % nested) if nested else ""
            suffix_str = ""

            suggest_list = self.search_field(field, root=nested, sub_properties=sub_properties)
            if len(suggest_list) > 0:
                str_paths = ["'%s'" % s["pretty_str_path"] for s in suggest_list]
                suffix_str = " Do you mean %s ?" % " or ".join(str_paths)
                suggest_list = self.__query_field_object(suggest_list, nested,
                                                         sub_properties)

            message = "Not Found: '%s'%s.%s" % (field, under_str, suffix_str)
            if can_raise:
                if suggest_list:
                    message += f"\nSuggest: {json.dumps(suggest_list)}"
                raise SchemaError(message)
            else:
                return {"error": message, "fields": suggest_list}

        # Ambiguous
        elif len(fields) > 1:
            str_paths = ['"%s"' % f["pretty_str_path"] for f in fields[:6]]
            fields = self.__query_field_object(fields, nested, sub_properties)
            can_be = ", ".join(str_paths)
            if len(fields) > 6:
                can_be += ", etc"
            message = f'Ambiguous: "{field}". It can be: {can_be}.'
            if can_raise:
                raise SchemaError(message)
            else:
                return {"error": message, "fields": fields}

        fields = self.__query_field_object(fields, nested, sub_properties)
        found = fields[0]
        found["function"] = function
        return found


    def __function_detector(self, field, functions, root=None):
        """
        Split field path and function
        - functions: Boolean / String List
                     True, allow all functions, List to restrict
        """
        if functions and len(field) > 1:
            func_name = field[-1]
            restricted = isinstance(functions, list) # Functions restriction
            allow = (restricted and func_name in functions) or not restricted
            if allow and func_name in ALLOWED_FUNCTIONS:
                field = field[:-1]
                return field, func_name

        return field, None


    def __query_field_pretty(self, fields):
        for field in fields:
            field["str_path"] = path_to_string(field["path"])
            field["pretty_str_path"] = "." + field["str_path"]
            field["short_path"] = self.short_path(field["path"])
            field["str_short_path"] = path_to_string(field["short_path"])
            field["str_nested"] = path_to_string(field["nested"])
            field["accept_function"] = self.accept_function(field["pretty_str_path"])
            if "type" not in field["element"] and "properties" in field["element"]:
                field["element"]["type"] = "object"
        return fields


    def __query_field_object(self, fields, nested, sub_properties):
        """
        For each found fields:
        1. Auto-select sub-properties (for objects only)
        2. Set nested field (path to the parent context, which is not root)
        3. Set type field (nested, object, string, long, ...)
        4. Set paths (path, string path, short path, short string path)
        """
        for field in fields:
            if sub_properties and field["element"].get("properties"):

                for sub in sub_properties:
                    if field["element"]["properties"].get(sub):

                        # Set nested field
                        if field["element"].get("type") == "nested":
                            field["nested"] = list(field["path"])

                        # Select sub-properties
                        field["element"] = field["element"]["properties"][sub]

                        # Set object type (implicite in ElasticSearch)
                        if field["element"].get("properties") and \
                           field["element"].get("type") is None:
                            field["element"]["type"] = "object"

                        # Set paths
                        field["path"].append(sub)
                        field["str_path"] = path_to_string(field["path"])
                        field["short_path"] = self.short_path(field["path"], sub_properties=sub_properties)
                        field["str_short_path"] = path_to_string(field["short_path"])
                        break

        return fields


################################################################################
####### Utils
################################################################################

    def accept_function(self, pretty_str_path):
        founds = self.match_field(pretty_str_path)
        if not founds:
            raise InternalServerError(f"Not found field: {pretty_str_path}")
        elif len(founds) > 1:
            raise InternalServerError(f"Ambigiuous path: {pretty_str_path}")
        return founds[0]["accept_function"]


    def short_path(self, path, sub_properties=None):
        if sub_properties and path[-1] in sub_properties:
            path = path[:-1]

        pretty_str_path = "." + path_to_string(path)
        founds = self.match_field(pretty_str_path)
        if not founds:
            raise InternalServerError(f"Not found field: {pretty_str_path}")
        elif len(founds) > 1:
            raise InternalServerError(f"Ambigiuous path: {pretty_str_path}")
        return founds[0]["short_path"]


    def schema_object_matching(self, field, path, root):
        """
        TODO: REFACTO

        This function try to match object field
        """
        found = []
        if path[-1] == field[0]:
            if len(field) == 1:
                """ Nested = path ??? WTF """
                found = [{"element": root, "nested": path, "path": path}]

            elif "properties" in root:
                """ Re-launch research with nested parameters """
                field = path + field[1:]
                found = self.schema_finder(field, path=path,
                                           root=root["properties"],
                                           nested=path)

        if len(found) == 0:
            """ Re-launch research without nested parameters """
            found = self.schema_finder(field)
            sub_nested = lambda f: f and f.get("nested") and path == f["nested"][:len(path)]
            found = [e for e in found if sub_nested(e)]

        return found


    def schema_finder(self, query_field, root=None, path=[], nested=None):
        """
        Look for candidats of given field path in the schema
        Allow partial path (label.model) and absolute path (.media.label.model)

        WARNING: Recurcive function, do NOT use path and nested

        Parameters
         - field: field path split by dot '.' (can start with dot for absolute path)
         - root: root path in string where start searching from
        """
        if not query_field or len(query_field) <= 0:
            raise InternalServerError("schema_finder: research field must not be empty")

        found_fields = []
        field = query_field

        ## Route Managing
        root_object = False
        if root is None:
            root = self.schema["properties"]
        elif isinstance(root, str):
            root_object = True
            root, field, path = self.__root_from_string(root, field)

        ## Root object elements' iteration
        for element in root:
            elm_path = list(path)
            elm_path.append(element)
            sub_nested = nested
            sub_root = None
            if isinstance(root[element], dict) and root[element].get("properties"):
                sub_root = root[element]["properties"]
                if root[element].get("type") == "nested":
                    sub_nested = elm_path

            obj = {"element": root[element], "nested": sub_nested, "path": elm_path}
            if field[0] == "." and len(elm_path) == len(field) - 1:
                if elm_path == field[1:]:
                    found_fields.append(obj)

            elif len(elm_path) >= len(field):
                sub = len(elm_path) - len(field)
                partial_path = elm_path[sub:]
                if partial_path == field:
                    found_fields.append(obj)

            if sub_root is not None:
                #pylint: disable-all
                found_fields += self.schema_finder(field, path=elm_path,
                                                   root=sub_root,
                                                   nested=sub_nested)

        ## Object name matching
        ## Enter for initial schema_finder call (with root input), without any results found
        if root_object and len(found_fields) == 0:
            found_fields = self.schema_object_matching(query_field, path, root)

        return [self.__schema_finder_format_output(query_field, f) for f in found_fields]


    def __root_from_string(self, root, field):
        """
        Fould root object from root string and setup field and path from root for schema_finder

        TODO: Refacto
        """
        path = root.split(".")
        found = self.match_field(root)
        if len(found) == 0:
            raise InternalServerError(f"schema_finder: '{root}' root does not exists")
        if len(found) > 1:
            raise InternalServerError(f"schema_finder: '{root}' ambigous root")
        root = found[0]["element"]

        if field:
            if len(path) > 0:
                if len(field) == 1:
                    if path[-1] != field[0]:
                        root = root["properties"]
                elif len(field) > 1:
                    if isinstance(root, dict) and root.get("properties"):
                        root = root["properties"]
            if field[0] == ".":
                field = path + field[1:]

        return root, field, path


    def __schema_finder_format_output(self, query_field, field):
        root_prefix = ""
        if query_field[0] == ".":
            root_prefix = "."
            query_field = query_field[1:]
        field["query_field"] = query_field
        field["str_query_field"] = root_prefix + path_to_string(query_field)
        return field


    def schema_walker(self, path, position=0, root=None, nested=None):
        """
        Extract value of deep object from input path
        """
        key = path[position]
        if root is None:
            root = self.schema["properties"]
        if key in root:
            elm = root[key]
            if len(path) > 1:
                if "properties" in elm:
                    if "type" in elm and elm["type"] == "nested":
                        nested = path[:position]
                    elm = elm["properties"]
                #pylint: disable-all
                return self.schema_walker(path,
                                          position=position + 1,
                                          root=elm,
                                          nested=nested)
            else:
                return {"nested": nested, "element": elm}
        else:
            suggest_str = ""
            suggest = self.search_field(path[:-1])
            if len(suggest) > 0:
                str_paths = ["'%s'" % s["pretty_str_path"] for s in suggest]
                suggest_str = ". Do you mean %s ?" % " or ".join(str_paths)
            raise InternalServerError(f"'{path}': not found in the schema{suggest_str}")


    def list_field(self, root=None, path=[], nested=None, sub_properties=None):
        """
        List all existing fields of the schema in details
        """
        if root is None:
            root = self.schema["properties"]
        elif isinstance(root, str):
            root, _, _ = self.__root_from_string(root, None)

        fields = []
        for key, value in root.items():
            sub_path = copy.deepcopy(path)
            sub_path.append(key)
            if isinstance(value, dict):
                value["type"] = value.get("type", "object")
                fields.append({
                    "field": key,
                    "element": value,
                    "path": sub_path,
                    "str_path": path_to_string(sub_path),
                    "pretty_str_path": "." + path_to_string(sub_path),
                    "nested": nested,
                    "str_nested": path_to_string(nested),
                    "format": value.get("format")
                })

                if "properties" in value:
                    sub_nested = sub_path if value.get("type") == "nested" else nested
                    #pylint: disable-all
                    fields += self.list_field(root=root[key]["properties"],
                                              path=sub_path,
                                              nested=sub_nested)

        return fields


    def __compute_field_short_path(self, field, fields):
        """
        Compute the shortest path of a field to distingue it from the other fields
        + add accept functions
        """
        other_fields = [f for f in fields if f["path"] != field["path"]]
        is_valid = False
        short_size = 1

        while short_size <= len(field["path"]) and not is_valid:
            short_path = field["path"][-short_size:]
            matchs = [f for f in other_fields if f["path"][-short_size:] == short_path]
            is_valid = len(matchs) == 0
            short_size += 1

        field["short_path"] = short_path
        str_prefix = "." if short_path == field["path"] else ""
        field["str_short_path"] = str_prefix + path_to_string(short_path)

        path = field["path"]
        field["accept_function"] = ["exists"]

        return field


    def search_field(self, target_field, root=None, sub_properties=None, min_score=0.6):
        """
        Search for the closest field in the schema
        """
        fields = self.list_field(root=root, sub_properties=sub_properties)

        scored_list = []
        for field in fields:

            if field["path"][-1].startswith("_"):
                continue

            best_score = 0
            for path_size in range(1, len(field["path"]) + 2):
                str_path = field["pretty_str_path"]
                if path_size <= len(field["path"]):
                    str_path = path_to_string(field["path"][-path_size:])
                score = SequenceMatcher(None, str_path, target_field).ratio()
                if score > best_score:
                    best_score = score

            field["score"] = best_score
            if field["score"] >= min_score:
                scored_list.append(field)

        for i, field in enumerate(scored_list):
            scored_list[i] = self.__compute_field_short_path(field, fields)

        return sorted(scored_list, key=lambda f: f["score"], reverse=True)[:3]


    def match_field(self, target_field, root=None):
        """
        Found fields matching target_field path
        """
        fields = self.list_field(root=root)

        founds = []
        for field in fields:

            for path_size in range(1, len(field["path"]) + 2):
                str_path = field["pretty_str_path"]
                if path_size <= len(field["path"]):
                    str_path = path_to_string(field["path"][-path_size:])
                if str_path == target_field:
                    founds.append(field)
                    break

        for i, field in enumerate(founds):
            founds[i] = self.__compute_field_short_path(field, fields)

        return founds


    def subfield(self, field, ignore_endswith=None, field_type=None):
        """
        List all subfield of given field path
        """
        if ignore_endswith is None:
            ignore_endswith = "id"
        field_path = field.split(".")
        if field.startswith("."):
            field_path = ["."] + field_path
        fields = self.schema_finder(field_path)
        if len(fields) == 0:
            raise InternalServerError(f"Not found: {field}")
        elif len(fields) > 1:
            mightbe = ["'%s'" % f["field"] for f in fields]
            raise InternalServerError(f"Ambiguous field '{field}' might be {' or '.join(mightbe)}")
        element = fields[0]["element"]
        if "properties" in element:
            element = element["properties"]

        subfields = element.items()
        if field_type is not None:
            get_type = lambda key, obj: obj.get("type", "object")
            subfields = [f for f in subfields if get_type(*f) in field_type]
        return [k for k, o in subfields if not k.endswith(ignore_endswith)]


################################################################################
### Utils
################################################################################

def path_to_string(path):
    if path is not None and len(path) > 0 and path[0] != None:
        return ".".join(path)
    return None


def string_to_path(str_path):
    path = str_path.split(".")
    if len(path[0]) == 0:
        path[0] = "."
    return path


def field_to_fraction(field):
    if not field:
        raise InvalidClientInput(f"Invalid fieldpath, CAN NOT be empty.")
    if ".." in field:
        raise InvalidClientInput(f"Invalid fieldpath, CAN NOT contains multiple dot consecutively: \"{field}\"")
    if field == ".":
        raise InvalidClientInput(f"Invalid fieldpath, CAN NOT be a single dot.")
    if field.endswith("."):
        raise InvalidClientInput(f"Invalid fieldpath, CAN NOT ends with dot: \"{field}\"")

    frac = field.split(".")
    if frac[0] == "":
        frac[0] = "."
    return frac


def path_to_pretty(path, nested=None):
    """
    Return pretty string of path, manage nested path
    """
    pretty_path = path_to_string(path)
    if pretty_path is None:
        return ""
    if pretty_path.startswith(".."):
        pretty_path = pretty_path[1:]
    if nested is not None:
        pretty_path += " under " + nested
    return pretty_path
