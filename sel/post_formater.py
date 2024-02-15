from datetime import datetime
from . import utils, date_utils


class PostFormater(object):

    def __call__(self, warns, query_data, results):
        """
        Warning: Modify warns without returning it
        """
        results["aggregations"] = self._format_aggreg(warns, query_data, results)
        return results


    def __init_aggreg(self, key, aggs, aggreg_data):
        """ Link with original aggreg_data """
        data = aggs
        data["aggreg_type"] = aggreg_data["aggreg"]["type"]
        data["field"] = f'.{aggreg_data["field"]["str_path"]}'

        data = utils.set_if_exists(aggreg_data, data, ["query_field"])
        data = utils.set_if_exists(aggreg_data["aggreg"], data, ["interval", "graph"])

        return data


    def _format_aggreg(self, warns, query_data, res):
        """
        Warning: Modify warns without returning it
        """
        if "aggregations" not in res:
            return {}

        aggregs = {}

        for key, aggs in res["aggregations"].items():
            aggreg_data = query_data["aggregations"][key]
            data = self.__init_aggreg(key, aggs, aggreg_data)

            sub_data = utils.get_lastest_sub_data(data)
            #pylint: disable=unsupported-membership-test
            if "buckets" in sub_data:
                warns = self.format_buckets(warns, sub_data, aggreg_data)

            aggregs[key] = data

        return aggregs


    def format_buckets(self, warns, data, aggreg_data):
        max_size = aggreg_data["aggreg"]["size"]

        #pylint: disable=unsubscriptable-object
        warn_large_bucket(warns, data["buckets"], aggreg_data)

        #pylint: disable=unsupported-assignment-operation
        warns, data = limit_buckets(warns, data, aggreg_data, max_size)

        #pylint: disable=unsubscriptable-object
        data["buckets"] = self.__format_sub_aggreg(warns, data["buckets"], aggreg_data)

        if aggreg_data["field"]["element"]["type"] == "date" and \
           aggreg_data["aggreg"]["type"] == "histogram":
            data = self._format_key_as_string(warns, data, aggreg_data)

        return warns


    def __format_sub_aggreg(self, warns, buckets, aggreg_data):
        """
        Warning: Modify warns without returning it

        2. Format sub aggregations
        """
        for bucket in buckets:
            for key, subaggreg in list(bucket.items()):
                if not isinstance(subaggreg, dict):
                    continue

                subaggreg_data = aggreg_data["subaggreg"][key]
                subaggreg = self.__init_aggreg(key, subaggreg, subaggreg_data)

                subaggreg = utils.get_lastest_sub_data(subaggreg)
                if "buckets" in subaggreg:
                    warns = self.format_buckets(warns, subaggreg, subaggreg_data)

        return buckets



    def _format_key_as_string(self, warns, data, aggreg_data):
        """
        Format aggreg date key_as_string

        Warning: Modify warns without returning it
        """
        date_format = date_utils.date_format_from_interval(aggreg_data["aggreg"]["interval"])

        for bucket in data["buckets"]:
            start = timestamp_to_datetime(bucket["key"])
            bucket["key_as_string"] = datetime.strftime(start, date_format)

        return data["buckets"]


##########################################################################
##### UTILS
##########################################################################

def limit_buckets(warns, aggreg, aggreg_data, max_size):
    if max_size > 0 and len(aggreg["buckets"]) > max_size:
        aggreg["buckets"] = aggreg["buckets"][:max_size]
        warns.append(f"Aggreg: '{aggreg_data['query_field']}' has more than {max_size} results")

    return warns, aggreg


def warn_large_bucket(warns, buckets, aggreg_data):
    """
    Warning: Modify warns without returning it
    """
    if aggreg_data["aggreg"]["type"] != "histogram" and \
       aggreg_data["field"]["element"]["type"] != "date" and \
       len(buckets) > 10000:
        warns.append(
            f'Aggreg values is too large for field {aggreg_data["query_field"]}. '\
            "Queries might be slow down. "\
            "Add 'size' parameter to improve query performance."
        )


def timestamp_to_datetime(ts):
    if ts is None:
        return None
    return date_utils.from_timestamp(ts / 1000.)
