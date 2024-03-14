from datetime import datetime


def read_meta(mappings):
    results = []

    for index, schema in mappings.items():
        settings = schema["settings"]
        creation_date_in_ms = float(settings["index"]["creation_date"]) / 1000

        results.append({
            "index": index,
            "meta": schema["mappings"].get("_meta"),
            "creation_date": datetime.fromtimestamp(creation_date_in_ms)
        })

    return sorted(results, key=lambda m: m["creation_date"], reverse=True)
