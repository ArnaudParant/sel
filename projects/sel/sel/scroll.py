def _reader(data):
    if "hits" in data and "hits" in data["hits"]:
        for hit in data["hits"]["hits"]:
            source = hit["_source"]
            if source is not None:
                source["_score"] = hit.get("_score")
                source["_index"] = hit.get("_index")
                yield source


def scroll(elastic, index, query, cash_time, scroll_id=None):

    if not scroll_id:
        res = elastic.search(index=index, body=query, scroll=cash_time)
    else:
        res = elastic.scroll(scroll_id=scroll_id, scroll=cash_time)

    return res["_scroll_id"], list(_reader(res))


def scroll_all(elastic, index, query, cash_time=3, bulk_size=1000):
    query["size"] = bulk_size
    cash_time = f"{cash_time}m"
    scroll_id = None

    while True:
        scroll_id, docs = scroll(elastic, index, query, cash_time, scroll_id=scroll_id)
        yield from docs

        if len(docs) < bulk_size:
            break

    elastic.clear_scroll(scroll_id=scroll_id)
