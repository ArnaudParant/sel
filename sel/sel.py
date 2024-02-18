# External deps
import json
import copy
import logging
from typing import List, Union, Generator, Any, Callable, Tuple
from datetime import datetime
from elasticsearch.exceptions import NotFoundError
import elasticsearch
import configparser
from collections import defaultdict

# Internal deps
from . import (
    meta, utils, date_utils, upload, scroll, query_generator, query_string_parser, config,
    query_object_formator
)
from .utils import InternalServerError, InvalidClientInput, NotFound
from .query_generator import QueryGenerator
from .schema_reader import SchemaReader
from .post_formater import PostFormater


DEFAULT_CONF = config.read()


class SEL:
    """
    Simple Elastic Language make ES query easier

    :param elastic: Elasticsearch connection
    :param conf: Configuration of the query system, default: :ref:`conf.ini`
    :param log_level: Log level to use, default: logging.INFO
    """

    def __init__(
            self,
            elastic: elasticsearch.Elasticsearch,
            conf: configparser.ConfigParser = DEFAULT_CONF,
            log_level=logging.INFO
    ):
        self.logger = logging.getLogger("SEL")
        self.logger.setLevel(log_level)
        self.log_level = log_level

        self.conf = conf
        self.elastic = elastic
        self.PostFormater = PostFormater()


    def _schema_reader(self, index: str) -> SchemaReader:
        """
        Get SchemaReader on the given index

        :param index: Index(es) to read the must recent schema, eg. "foo" or "foo,bar"
        :return: Instance of SchemaReader on the input index
        """
        schema = self.get_schema(index)
        return SchemaReader(self.conf, schema)


    @utils.elastic_exception_detailor
    def get_schema(self, index: str) -> dict:
        """
        Get must recent schema of given index(es)

        :param index: Index(es) to get schema(s), eg. "foo" or "foo,bar"
        :return: Must recent mapping

        .. code-block:: python

            > sel.get_schema("foo")
            {doc_type: {mapping ... }}

        """

        try:
            schemas = self.elastic.indices.get(index=index)
        except NotFoundError:
            raise NotFound(f"Index(es) not found: {index}")

        latest_created_index = meta.read_meta(schemas)[0]["index"]

        return schemas[latest_created_index]["mappings"]


    @utils.elastic_exception_detailor
    def list_index(self, index: str = None) -> List[dict]:
        """
        List Elasticsearch indexes

        :param index: Optional Index(es) to limit, eg. "foo_*" or "foo_*,bar_*"
        :return: List of indexes with metadata (if set at creation)

        .. code-block:: python

            > sel.list_index()
            [
               {'index': 'myindex', 'doc_type': 'document', 'meta': None, 'creation_date': datetime.datetime(2023, 9, 13, 13, 26, 42, 251000)},
               ...
            ]

        """
        if index is None:
            index = "*"

        try:
            response = self.elastic.indices.get(index, allow_no_indices=True)
        except NotFoundError:
            raise NotFound(f"Index(es) not found: {index}")

        return meta.read_meta(response)


    @utils.elastic_exception_detailor
    def scroll(self, index: str, query: dict, cash_time: str, scroll_id: str = None) -> dict:
        """
        Scroll over documents with a query, can get all documents of index(es).
        First call without scroll_id will return a scroll_id to use for next requests.

        Warning: Don't forget to clear scroll_id after usage

        :param index: Index(es) to scroll on, eg. "foo" or "foo,bar"
        :param query: SEL query (string or object) to filter documents
        :param cash_time: Duration of scroll cash between each call
        :param scroll_id: Scroll id to continue scrolling
        :return: Dictionary with scroll_id and documents

        .. code-block:: python

            > sel.scroll("foo", None, "1m")
            {'scroll_id': 'cXVlc...', 'documents': [{...}, ...]}

            > sel.scroll("foo", None, "1m", scroll_id="cXVlc...")
            {'scroll_id': 'cXVlc...', 'documents': [{...}, ...]}

            > sel.clear_scroll("cXVlc...")
        """
        query_obj = self.generate_query(query, index=index)["elastic_query"]
        scroll_id, documents = scroll.scroll(
            self.elastic, index, query_obj, cash_time, scroll_id=scroll_id
        )

        return {"scroll_id": scroll_id, "documents": documents}


    @utils.elastic_exception_detailor
    def clear_scroll(self, scroll_id: str) -> None:
        """
        Clear scroll even before the end of the cash time to free ES memory

        :param scroll_id: Scroll id to clear
        :return: None

        .. code-block:: python

            > sel.clear_scroll("cXVlc...")
        """
        self.elastic.clear_scroll(scroll_id=scroll_id)


    @utils.elastic_exception_detailor
    def download_aggreg(
            self, index: str, base_aggreg: dict, query: dict
    ) -> Generator[dict, None, None]:
        """
        Return all buckets of one aggregation

        - Order is not warranty

        - Keys can be returned multiple times, add all doc_count to have the total

        It partion index(es) with base_aggreg to proceed the query aggregation.

        :param index: Index(es) to process the aggregation, eg. "foo" or "foo,bar"
        :param base_aggreg: Base SEL aggregation to partion the index(es)
        :param query: SEL query (string or object) with one aggregation
        :return: Generator of buckets

        .. code-block:: python

            > base_aggreg = {"field": "date", "interval": "week"}
            > query = {"aggregations": {"my_aggreg": {"field": ".id"}}}
            > list(sel.download_aggreg("foo", base_aggreg, query))
            [{'key': '1446587002614128796', 'doc_count': 1}, ...]
        """
        query["meta"] = query["meta"] if query.get("meta") else {}
        query["meta"]["size"] = 0
        query = self._to_queryobject(query)

        if len(query.get("aggregations", {}).keys()) == 0:
            raise InvalidClientInput("Download aggreg MUST HAVE ONE aggregation")
        if len(query.get("aggregations", {}).keys()) > 1:
            raise InvalidClientInput("Download aggreg MUST HAVE ONLY ONE aggregation")

        self.logger.debug("Partioning ...")
        original_query = copy.deepcopy(query)
        partition_query = copy.deepcopy(query)
        base_aggreg["size"] = 0
        interval = base_aggreg.get("interval")
        partition_query["aggregations"] = {"parts": base_aggreg}
        res = self.search(index, partition_query)

        partitions = utils.get_lastest_sub_data(res["results"]["aggregations"]["parts"])["buckets"]
        partitions = sorted(
            [p for p in partitions if p["doc_count"] > 0],
            key=lambda p: p["doc_count"],
            reverse=True
        )
        self.logger.debug(f"Found {len(partitions)} partitions")

        aggreg_key = list(original_query["aggregations"].keys())[0]
        part_field = base_aggreg.get("field")
        query["aggregations"][aggreg_key]["size"] = 0

        self.logger.debug("Downloading aggregation buckets ...")

        if not partitions:
            yield from self._download_aggreg_one_partition(index, original_query)

        for part in partitions:
            yield from self._download_aggreg_one_partition(
                index, original_query, part=part, part_field=part_field, interval=interval
            )


    def _download_aggreg_one_partition(
            self, index: str, query: dict,
            part: dict = None, part_field: str = None, interval: str = None
    ) -> Generator[dict, None, None]:
        """
        Query aggregation on one partition

        :param index: Index(es) to process
        :param query: SEL query object with one aggregation
        :param part: Partition value as {"key": ...} or {"key_as_string": ...}
        :param part_field: Field path for the partition
        :return: Buckets generator
        """
        part_query = copy.deepcopy(query)
        items = [part_query.get("query")]

        if part:
            reader = self._schema_reader(index)
            info = reader.get_field_info(part_field)

            if info["element"]["type"] == "date":
                key = part["key_as_string"]
                value = {">=": key, "<=": self._get_end_date(key, interval=interval)}
                items.append({"field": part_field, "comparator": "range", "value": value})

            else:
                items.append({"field": part_field, "value": part["key"]})

        part_query["query"] = utils.build_group("and", items)
        res = self.search(index, part_query)

        aggreg_key = list(part_query["aggregations"].keys())[0]
        data = utils.get_lastest_sub_data(res["results"]["aggregations"][aggreg_key])["buckets"]
        yield from data


    def _get_end_date(self, key_as_string: str, interval: str = None) -> str:
        """
        Get end_date from key_as_string and interval

        :param key_as_string: date as string
        :param interval: interval of the aggregation
        :return: end date as string
        """
        interval = interval if interval else self.conf["Aggregations"]["DefaultDateInterval"]
        delta = date_utils.interval_to_delta_time(interval)
        key, date_format = date_utils.str_date_to_datetime(key_as_string)
        return datetime.strftime(key + delta, date_format)


##########################################################################
# SEARCH
##########################################################################

    def __filter_deleted_documents(self, reader: SchemaReader, query: dict) -> dict:
        """
        Add filter for deleted documents into the query

        1. If configured to exclude deleted documents
        2. And user does not set any filter on deleted documents
        3. And the current index schema has a deleted field

        :param reader: SchemaReader instance
        :param query: SEL query object
        :return: The query with deleted documents filter
        """
        exclude_deleted_docs = self.conf["Queries"].getboolean("DefaultExcludeDeletedDocuments")
        found = query_generator.find_filter(query, "deleted")

        if not found and exclude_deleted_docs:
            found_fields = reader.schema_finder([".", "deleted"])
            if found_fields:
                new_filter = {"field": ".deleted", "comparator": "!=", "value": True}
                query = query_generator.top_insert_filter(query, "and", new_filter)

        return query


    def _to_queryobject(self, input_query: dict) -> dict:
        """
        Convert input query to query object

        :param input_query: SEL query (string or object)
        :return: SEL query object
        """
        if input_query is None:
            input_query = {}

        if not isinstance(input_query, dict):
            raise InvalidClientInput(f"Invalid input query type: {type(input_query)}")

        query_obj = None

        if not isinstance(input_query.get("query"), str):
            query_obj = copy.deepcopy(input_query)

        elif isinstance(input_query.get("query"), str) and \
           ("aggregations" in input_query or "sort" in input_query):
            raise InvalidClientInput(
                "'aggregations' and 'sort' CAN'T be set with 'query' as string"
            )

        else:
            query_string = input_query.get("query", "")
            self.logger.debug("query string = %s", json.dumps(query_string))
            results = query_string_parser.parse(query_string)
            query_obj = query_object_formator.formator(results)

        query_obj = utils.set_if_exists(input_query, query_obj, ["meta"])

        self.logger.debug("query object = %s", json.dumps(query_obj))
        return query_obj


    @utils.elastic_exception_detailor
    def generate_query(
            self, query: dict, schema: dict = None, index: str = None, no_deleted: bool = True
    ) -> dict:
        """
        Generate Elasticsearch query from SEL query

        :param query: SEL query (string or object)
        :param schema: Will get it back if not given (to avoid multiple requests)
        :param index: Index(es), can be None if schema is given, otherwise eg. "foo" or "foo,bar"
        :param no_deleted: True to filter out deleted documents (if configured to), default: True
        :return: Dictionary warns, elastic_query, internal_query, query_data

        .. code-block:: python

            > query = {"query": ".id = 93428yr9"}                       # Query String
            > query = {"query": {"field": ".id", "value": "93428yr9"}}  # Query Object

            > sel.generate_query(query, index="foo", no_deleted=False)
            {
               'warns': [],
               'elastic_query': {'query': {'term': {'id': '93428yr9'}}, 'sort': [{'id': {'order': 'desc', 'mode': 'avg'}}]},
               'internal_query': {'query': {'field': '.id', 'value': '93428yr9'}},
               'query_data': {}
            }

        """
        warns = []
        query_obj = None

        if index is None and schema is None:
            raise InternalServerError("GenerateQuery: index or schema must be given")

        if index is not None:
            schema = self.get_schema(index)

        query_obj = self._to_queryobject(query)
        generator = QueryGenerator(self.conf, schema, log_level=self.log_level)

        if no_deleted:
            query_obj = self.__filter_deleted_documents(generator.schema_reader, query_obj)

        elastic_query, query_data = generator.generate_query(warns, query_obj)

        return {
            "warns": list(set(warns)),
            "elastic_query": elastic_query,
            "internal_query": query_obj,
            "query_data": query_data
        }


    @utils.elastic_exception_detailor
    def search(self, index: str, query: dict, no_deleted: bool = True) -> dict:
        """
        Search with SEL query

        :param index: Index(es) to search on, eg. "foo" or "foo,bar"
        :param query: SEL query (string or object)
        :param no_deleted: True to filter out deleted documents (if configured to), default: True
        :return: Dictionary 'results' as ES results, 'warns' for query system warnings

        .. code-block:: python

            > query = {"query": ".id = 1435886281564398679"}                       # Query String
            > query = {"query": {"field": ".id", "value": "1435886281564398679"}}  # Query Object

            > sel.search("foo", query)
            {
               'results': {
                  'took': 1,
                  'timed_out': False,
                  '_shards': {...},
                  'hits': {
                     'total': 1,
                     'max_score': None,
                     'hits': [{
                        '_index': 'foo',
                        '_type': 'document',
                        '_id': '1435886281564398679',
                        '_score': None,
                        '_source': {...},
                        'sort': []
                     }]
                  },
                  'aggregations': {}
               },
               'warnings': []
             }
        """
        query_obj = self.generate_query(query, index=index, no_deleted=no_deleted)
        warns = query_obj["warns"]

        self.logger.debug("es query = %s" % json.dumps(query_obj["elastic_query"]))
        response = self.elastic.search(
            index=index,
            body=query_obj["elastic_query"],
            doc_type=self.conf["Elasticsearch"]["DocType"],
            _source=True
            #analyze_wildcard=True  # Does not exists in 5.x ?
        )

        results = self.PostFormater(warns, query_obj["query_data"], response)

        for warn in warns:
            self.logger.warning(warn)

        return {"results": results, "warnings": list(set(warns))}


    def get_one_document(self, index: str, doc_id: str) -> dict:
        """
        Get one document of an index

        :param index: Index(es) to get the document, eg. "foo" or "foo,bar"
        :param doc_id: The document id
        :return: The whole document

        .. code-block:: python

            > sel.get_one_document("foo", "1435886281564398679")
            {
               '_index': 'test_index',
               '_type': 'document',
               '_id': '1435886281564398679',
               '_score': None,
               '_source': {...},
               'sort': []
            }
        """
        query = {"field": ".id", "value": doc_id}
        res = self.search(index, {"query": query}, no_deleted=False)
        hits = res["results"]["hits"].get("hits", [])

        if not hits:
            raise NotFound(f"Not found document id: {doc_id}")
        if len(hits) > 1:
            raise InvalidClientInput(f"Numerous documents found with this id: {doc_id}")

        return hits[0]


##########################################################################
# FIELD FUNCTIONS
##########################################################################

    @utils.elastic_exception_detailor
    def list_fields(self, index: str) -> List[dict]:
        """
        List all fields of an index

        :param index: Index(es), eg. "foo" or "foo,bar"
        :return: All found fields' information

        .. code-block:: python

            > sel.list_fields("foo")
            [
               {
                   'field': 'author',
                   'element': {
                      'type': 'object',
                      'properties': {
                         'follower': {'type': 'integer'},
                         'id': {'type': 'string', 'index': 'not_analyzed'},
                         'name': {'type': 'string', 'index': 'not_analyzed'}
                      }
                   },
                   'path': ['author'],
                   'str_path': 'author',
                   'pretty_str_path': '.author',
                   'nested': None,
                   'str_nested': None,
                   'format': None
               },
               ...
            ]
        """
        reader = self._schema_reader(index)
        return reader.list_field()


    @utils.elastic_exception_detailor
    def search_field(self, index: str, field_path: str) -> List[dict]:
        """
        Search for a field into an index

        :param index: Index(es), eg. "foo" or "foo,bar"
        :param field_path: The field path to search
        :return: Potential fields

        .. code-block:: python

            > sel.search_field("foo", "id")
            [
               {
                  'field': 'id',
                  'element': {'type': 'string', 'index': 'not_analyzed'},
                  'path': ['author', 'id'],
                  'str_path': 'author.id',
                  'pretty_str_path': '.author.id',
                  'nested': None,
                  'str_nested': None,
                  'format': None,
                  'score': 1.0,
                  'short_path': ['author', 'id'],
                  'str_short_path': '.author.id',
                  'accept_function': ['exists']
               },
               ...
            ]
        """
        reader = self._schema_reader(index)
        return reader.search_field(field_path)


    @utils.elastic_exception_detailor
    def subfields(
            self, index: str, fields_path: List[str], no_empty=True
    ) -> Generator[dict, None, None]:
        """
        Get subfields of fields of an index

        :param index: Index(es), eg. "foo" or "foo,bar"
        :param fields_path: The fields path to get subfields
        :param no_empty: Filter out empty subfields, default: True
        :return: All subfields of each given fields

        .. code-block:: python

            > list(sel.subfields("foo", "media.label"))
            [
               {
                  'field': 'media.label',
                  'subfields': ['attribute', 'color', 'model', 'style', 'texture', 'type']
               }
            ]
        """
        reader = self._schema_reader(index)

        for field_path in fields_path:
            subfields = reader.subfield(field_path, field_type=["object", "nested"])

            if no_empty:
                subfields = self.__filter_no_empty_subfields(index, field_path, subfields)

            yield {"field": field_path, "subfields": subfields}


    def __filter_no_empty_subfields(
            self, index: str, field_path: str, subfields: List[str]
    ) -> List[str]:
        """
        Keep subfields which have not empty buckets

        :param index: Index(es), eg. "foo" or "foo,bar"
        :param field_path: The field path to get subfields
        :param subfields: List of subfields to check
        :return: List of no-empty subfields
        """
        def build_aggreg(subfield):
            return {"field": "%s.%s" % (field_path, subfield), "size": 1}

        query = {"aggregations": {f: build_aggreg(f) for f in subfields}}
        res = self.search(index, query)

        aggs = res["results"]["aggregations"]
        return sorted([f for f, a in aggs.items() if utils.get_lastest_sub_data(a)["buckets"]])



##########################################################################
# DELETE DOCUMENTS
##########################################################################

    def _delete_document_action(self, deleted_info: Any = None) -> Callable[[dict], dict]:
        """
        Build a function for the delete action

        :param deleted_info: Any type and information you want in the delete document
        :return: A function for the delete action
        """

        def aux(source: dict) -> dict:
            """ Do the delete action """
            source["deleted"] = True

            if deleted_info is not None:
                source["deleted_info"] = deleted_info

            return source
        return aux


    def _undelete_document_action(self, source: dict) -> dict:
        """
        Do the undelete action

        :param source: the document
        :return: The document unflag as deleted
        """
        source["deleted"] = False

        if "deleted_info" in source:
            del source["deleted_info"]

        return source


    def __delete_documents(
            self, index: str, query: dict, action_id: str, deleted_info: Any = None
    ) -> dict:
        """
        Delete documents of indexes based on SEL query

        :param index: Index(es) to delete documents, eg. "foo" or "foo,bar"
        :param query: ES query to match documents to delete
        :param action_id: delete / undelete
        :param deleted_info: Any information you want in deleted documents
        :return: Dictionary action_id, count
        """
        # Get all documents
        docs = scroll.scroll_all(self.elastic, index, query)

        # Apply action delete/undelete on found documents
        action = self._delete_document_action(deleted_info)
        if action_id == "undelete":
            action = self._undelete_document_action

        # Structure documents by indexes
        index_documents = defaultdict(list)
        for doc in docs:
            index_documents[doc["_index"]].append(action(doc))

        # Update documents in indexes
        count = 0
        doc_type = self.conf["Elasticsearch"]["DocType"]
        id_getter = lambda d: d["id"]

        for index_name, documents in index_documents.items():
            count += len(documents)
            upload.bulk(self.elastic, index_name, doc_type, documents, id_getter)

        return {"action": action_id, "count": count}


    def _delete_query_to_query(self, index: str, query: dict) -> dict:
        """
        Delete query to SEL query object

        - Can contains "ids" to simplify query

        :param index: Index(es), eg. "foo" or "foo,bar"
        :param query: SEL query (string or object) to filter documents
        :return: SEL query
        """
        if query.get("ids"):
            query = {"query": {"terms": {"id": query["ids"]}}}
        elif query.get("query"):
            query = {"query": query["query"]}
            query = self.generate_query(query, index=index, no_deleted=False)["elastic_query"]
        else:
            raise InvalidClientInput("Invalid input: id or query MUST BE given in input json")

        return query


    @utils.elastic_exception_detailor
    def delete_documents(
            self, index: str, query: dict, undelete: bool = False, deleted_info: Any = None
    ) -> dict:
        """
        Delete documents of indexes based on SEL query

        :param index: Index(es) to delete documents, eg. "foo" or "foo,bar"
        :param query: SEL query (string or object) to match documents to delete. Can also contains "ids" to simplify query
        :param undelete: to unflag documents, default: False
        :param deleted_info: Any information you want in deleted documents
        :return: Dictionary action_id, count

        .. code-block:: python

            > query = {"query": ".id = 1435886281564398679"}                       # Query String
            > query = {"query": {"field": ".id", "value": "1435886281564398679"}}  # Query Object
            > query = {"ids": ["1435886281564398679"]}                             # Ids format

            > sel.delete_documents("foo", query)
            {'action': 'delete', 'count': 1}
        """
        action_id = "undelete" if undelete else "delete"
        query = self._delete_query_to_query(index, query)

        return self.__delete_documents(index, query, action_id, deleted_info=deleted_info)


##########################################################################
# REALLY DELETE DOCUMENTS
##########################################################################


    def __really_delete_documents(self, index: str, query: dict) -> int:
        """
        Really delete documents (not just flag them) from a SEL query

        :param index: Index(es) to delete documents, eg. "foo" or "foo,bar"
        :param query: ES query to match documents to delete.
        :return: Number of deleted documents
        """
        # Get all documents
        docs = scroll.scroll_all(self.elastic, index, query)

        # Structure documents by indexes
        index_documents = defaultdict(list)
        for doc in docs:
            index_documents[doc["_index"]].append({"_id": doc["id"]})

        # Update documents in indexes
        count = 0
        doc_type = self.conf["Elasticsearch"]["DocType"]
        id_getter = lambda d: d["_id"]
        for index_name, documents in index_documents.items():
            count += len(documents)
            upload.bulk(self.elastic, index_name, doc_type, documents, id_getter, operation="delete")

        return count


    @utils.elastic_exception_detailor
    def really_delete_documents(self, index: str, query: dict) -> int:
        """
        Really delete documents (not just flag them) from a SEL query

        :param index: Index(es) to delete documents, eg. "foo" or "foo,bar"
        :param query: SEL query (string or object) to match documents to delete. Can also contains "ids" to simplify query
        :return: Number of deleted documents

        .. code-block:: python

            > query = {"query": ".id = 1435886281564398679"}                       # Query String
            > query = {"query": {"field": ".id", "value": "1435886281564398679"}}  # Query Object
            > query = {"ids": ["1435886281564398679"]}                             # Ids format

            > sel.really_delete_documents("foo", query)
            1
            > sel.really_delete_documents("foo", query)
            0
        """
        query = self._delete_query_to_query(index, query)
        return self.__really_delete_documents(index, query)
