from sel import utils


def list_equals(l1, l2):
    if len(l1) != len(l2):
        return False
    for i, e in enumerate(l1):
        if e != l2[i]:
            return False
    return True


def dict_equals(d1, d2):
    for k, v in d1.items():
        if k not in d2 or type(d2[k]) != type(v):
            return False
        if isinstance(v, dict):
            if not dict_equals(v, d2[k]):
                return False
        elif d2[k] != v:
            return False
    for k, v in d2.items():
        if k not in d1 or type(d1[k]) != type(v):
            return False
        if isinstance(v, dict):
            if not dict_equals(v, d2[k]):
                return False
        elif d2[k] != v:
            return False
    return True


def buckets_formator(buckets):
    formated = []

    for b in buckets:
        subkeys = sorted(list(set(b.keys()) - {"key", "doc_count"}))
        subs = [(sk, b[sk].get("doc_count"), subaggreg_formator(b[sk])) for sk in subkeys]
        formated.append((b["key"], b["doc_count"], subs))

    return formated


def subaggreg_formator(aggreg):
    sub = utils.get_lastest_sub_data(aggreg)

    if "buckets" in sub:
        return buckets_formator(sub["buckets"])

    return sub["value"]
