from itertools import islice


def _document_wrapper(index, documents, id_getter, operation):
    for doc in documents:

        wrapper = {"action": {operation: {
            "_index": index,
            "_id": id_getter(doc)
        }}}

        if operation != "delete":
            wrapper["source"] = doc

        yield wrapper


def _sender(elastic, bulk, operation):
    body = [s for e in bulk for s in [e["action"], e.get("source")] if s is not None]
    res = elastic.bulk(body=body, refresh=True)

    failure = [i[operation] for i in res["items"] if "error" in i[operation]]
    if failure:
        raise Exception(str(failure))


def _manager(elastic, documents, size, operation):

    while True:
        bulk = list(islice(documents, size))
        if not bulk:
            break

        _sender(elastic, bulk, operation)


def bulk(elastic, index, documents, id_getter, bulk_size=100, operation="index"):
    docs = _document_wrapper(index, documents, id_getter, operation)
    _manager(elastic, docs, bulk_size, operation)
