from datetime import datetime


def get_doc_type(mapping):
    """ Get first found doc_type in mapping schema """
    keys = list(mapping.keys())
    if "settings" in keys:
        del keys[keys.index("settings")]
    if not keys:
        return None
    return keys[0]


def read_meta(mappings):
    results = []

    for index, schema in mappings.items():
        doc_type = get_doc_type(schema["mappings"])
        settings = schema["settings"]
        creation_date_in_ms = float(settings["index"]["creation_date"]) / 1000

        results.append({
            "index": index,
            "doc_type": doc_type,
            "meta": schema["mappings"].get(doc_type, {}).get("_meta"),
            "creation_date": datetime.fromtimestamp(creation_date_in_ms)
        })

    return sorted(results, key=lambda m: m["creation_date"], reverse=True)
