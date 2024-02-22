# Queries Documentation

## Multiple schema querying

Elasticsearch and SEL support to store and query multiple schema. You can even query several indexes in the same time with different schemas, basically it's how works wildcard `foo_*`.

By default SEL will take last inserted schema to build the query. It will only be an issue if you have the same field with two different types or more into schemas.

## Queries' structure

### Query string

Queries string are composted of three parts: filters, aggregations and sorts, ordered as bellow:

```
{filters} {aggregations} {sorts}
```

Filters limit the number of hits and sort allowed to order returned hits.

Aggregations does not have any impact on returned documents, but are only made on matched documents. You can do several aggregations in a single query, it will not impact each other and a key allow to differentiate them. Aggregations can also contains a specific filter with the `where` keyword, which will not impact the main query neither other aggregations. Of course sort does not have any impact on aggregations.

Sort query order matter and it's apply in the same order into Elasticsearch. The first sort is applied, and, on equals values, according on this sort, the second sort is applied, etc.

### Auto sort

By default there is an auto sort system in SEL, see `conf.ini` to disable it.  
It take all your filters in the same order to make a sort on the inner `DefaultObjectSortField` field (`conf.ini`), with the original query as filter `where`.

Those generated sorts are put after sorts you have set into your query.

### Object

```
{
  "query": (Optional) String or Object,
  "aggregations": (Optional) Object - For object query only,
  "sort": (Optional) Object List - For object query only,
  "meta": (Optinal) Object - Limit hits,
  "extended": (Optional) Object - Additional ES query keys
}
```

**Meta**

Used for paging, it does not impact aggregations, such:

```
{
  "size": (Optional) Int - default 20
  "from": (Optional) Int - default 0
}
```

**Extended**

Allowed keys: `_source`, `fields`, `script_fields`, `fielddata_fields`, `explain`, `highlight`, `rescore`, `version`, `indices_boost`, `min_score`.  
See [ES Search request body](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-body.html)

**Examples**

Query string
```
{
  "query": "label = bag aggreg: label.model sort: like",
  "meta": {"from": 100, "size": 20}
}
```

Query object
```
{
  "query": {"field": "label", "value": "bag"},
  "aggregations": {"myaggreg": {"field": "label.model"}},
  "sort": [{"field": "like"}],
  "meta": {"from": 100, "size": 20}
}
```


## Field path

To simplify queries, SEL allow to use short path for field path, if it's ambiguous or if it does not found the field, it will raise an error.  

If you have this field path into your schema for eg. `media.label.id`, can use directly `id`.  
But if there are other fields `id` into your schema, it will raise an error and you will need to specify which one you would like to use.  

If you have for exemple `media.label.id` and `id` at root, you can match them respectivly with `label.id` and `.id`.  
As you can see it's possible to use dot `.` at the beginning to specify that it's at the root level.


## Filters

🗒️ **Note**: All examples bellow refer to `Schema example`.

### String comparison

#### Equals

🗒️ **Note**: The default comparator is `=`

```
label.color = blue
{"field": "label.color", "value": "blue"}
{"field": "label.color", "comparator": "=", "value": "blue"}
```

#### Not Equals

```
label.color != blue
{"field": "label.color", "comparator": "!=", "value": "blue"}
```

#### Spaces and Special characters

```
label.brand = "foo bar"
{"field": "label.brand", "value": "foo bar"}

label.entity = 'bg:model'
{"field": "label.entity", "value": "bg:model"}
```

#### Query String

Query string use [ES query string format](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-query-string-query.html)

```
label ~ "*pant*"
{"field": "label", "comparator": "~", "value": "*pant*"}
```

#### Not match query string

Query string use [ES query string format](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-query-string-query.html)

```
label !~ "*pant*"
{"field": "label", "comparator": "!~", "value": "*pant*"}
```

#### Prefix

```
label prefix h
{"field": "label", "comparator": "prefix", "value": "h"}
```

#### Not prefix

```
label nprefix h
label not prefix h
{"field": "label", "comparator": "nprefix", "value": "h"}
```

#### In

```
label in [human, person]
{"field": "label", "comparator": "in", "value": ["human", "person"]}
```

#### Not in

```
label nin [human, person]
label not in [human, person]
{"field": "label", "comparator": "nin", "value": ["human", "person"]}
```

### Numerical comparison

#### Equals

🗒️ **Note**: The default comparator is `=`

```
label.model.score = 0.8
{"field": "label.model.score", "value": 0.8}
{"field": "label.model.score", "comparator": "=", "value": 0.8}
```

#### Not Equals

```
label.model.score != 0.8
{"field": "label.model.score", "comparator": "!=", "value": 0.8}
```

#### Greater than or Equals

```
label.model.score >= 0.8
{"field": "label.model.score", "comparator": ">=", "value": 0.8}
```

#### Greater than

```
label.model.score > 0.8
{"field": "label.model.score", "comparator": ">", "value": 0.8}
```

#### Less than or Equals

```
label.model.score <= 0.8
{"field": "label.model.score", "comparator": "<=", "value": 0.8}
```

#### Less than

```
label.model.score < 0.8
{"field": "label.model.score", "comparator": "<", "value": 0.8}
```

#### Range

```
label.model.score range (> 0.2, < 0.8)
0.2 < label.model.score < 0.8

{"field": "label.model.score", "comparator": "range", "value": {">": 0.2, "<": 0.8}}
```

#### Not in range

```
label.model.score nrange (> 0.2, < 0.8)
label.model.score not range (> 0.2, < 0.8)
{"field": "label.model.score", "comparator": "nrange", "value": {">": 0.2, "<": 0.8}}

not 0.2 < label.model.score < 0.8
{"not": {"field": "label.model.score", "comparator": "range", "value": {">": 0.2, "<": 0.8}}}
```

### Date comparison

#### Date formats

```
date >= 2014
{"field": "date", "comparator": ">=", "value": "2014"}
```

```
date >= 2014-05
{"field": "date", "comparator": ">=", "value": "2014-05"}
```

```
date >= 2014-10-04
{"field": "date", "comparator": ">=", "value": "2014-10-04"}
```

```
date >= "2014-10-04 15" 
{"field": "date", "comparator": ">=", "value": "2014-10-04 15"}
```

```
date >= "2014-10-04 15:42"
{"field": "date", "comparator": ">=", "value": "2014-10-04 15:42"}
```

```
date >= "2014-10-04 15:42:10"
{"field": "date", "comparator": ">=", "value": "2014-10-04 15:42:10"}
```

#### Range

```
date range (>= 2018, <= 2019)
2018 <= date <= 2019

{"field": "date", "comparator": "range", "value": {">=": "2018", "<=": "2019"}}
```

#### Not in range

```
date nrange (>= 2018, <= 2019)
date not range (>= 2018, <= 2019)
{"field": "date", "comparator": "nrange", "value": {">=": "2018", "<=": "2019"}}

not 2018 <= date <= 2019
{"not": {"field": "date", "comparator": "nrange", "value": {">=": "2018", "<=": "2019"}}}
```

### Query string

Query string will match the `DefaultQueryStringFieldPath` (from `conf.ini`) with [ES query string format](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-query-string-query.html).  

```
"foam cage"
{"query_string": "foam cage"}
```

The field path can be set with the following syntax:

```
mycontent ~ "foam cage"
{"field": "mycontent", "comparator": "~", "value": "foam cage"}
```


#### AND / OR

By default, query string use OR operator

```
"foam OR cage"
{"query_string": "foam OR cage"}
```

AND operator is possible as well

```
"foam AND cage"
{"query_string": "foam AND cage"}
```

#### Grouping

```json
"(foam AND cage) OR (all AND blue)"
{"query_string": "foam OR cage"}
```

#### Wildcards

Use `?` to replace a single character, `*` to replace zero or more characters.

```json
"foa?"
{"query_string": "foa?"}
```

```json
"foa*"
{"query_string": "foa*"}
```

#### Regular expressions

Regular expression patterns can be embedded in the query string by wrapping them in forward-slashes (`"/"`)

```json
"/joh?n(ath[oa]n)/"
{"query_string": "/joh?n(ath[oa]n)/"}
```

#### Fuzziness

We can search for terms that are similar to, but not exactly like our search terms, using the “fuzzy” operator

```json
"quikc~"
{"query_string": "quikc~"}
```

The default edit distance is `2`, but you can edit the distance

```json
"quikc~1"
{"query_string": "quikc~1"}
```

#### Exact match

It’s also possible to do exact match query with double quote.

```
'"foam cage"'
{"query_string": "\"foam cage\""}
```

#### Proximity search

While a phrase query (eg `"john smith"`) expects all of the terms in exactly the same order, a proximity query allows the specified words to be further apart or in a different order. In the same way that fuzzy queries can specify a maximum edit distance for characters in a word, a proximity search allows us to specify a maximum edit distance of words in a phrase:

```
'"foam cage"~3'
{"query_string": "\"foam cage\"~3"}
```

#### Boosting

Use the boost operator `^` to make one term more relevant than another. For instance, if we want to find all documents about foxes, but we are especially interested in quick foxes:

```json
"quick^2 fox"
{"query_string": "quick^2 fox"}
```

The default `boost` value is 1, but can be any positive floating point number. Boosts between 0 and 1 reduce relevance.

Boosts can also be applied to phrases or to groups:

```
'"john smith"^2'
{"query_string": "\"john smith\"^2"}

"(foo bar)^4"
{"query_string": "(foo bar)^4"}
```

#### Boolean operators

By default, all terms are optional, as long as one term matches. A search for `foo bar baz` will find any document that contains one or more of `foo` or `bar` or `baz`. There are also boolean operators which can be used in the query string itself to provide more control.

The preferred operators are `+` (this term **must** be present) and `-` (this term **must not** be present). All other terms are optional. For example, this query:

```json
"quick brown +fox -news"
{"query_string": "quick brown +fox -news"}
```

states that:

- `fox` must be present
- `news` must not be present
- `quick` and `brown` are optional — their presence increases the relevance

The familiar operators `AND`, `OR` and `NOT` (also written `&&`, `||` and `!`) are also supported. However, the effects of these operators can be more complicated than is obvious at first glance. `NOT` takes precedence over `AND`, which takes precedence over `OR`. While the `+` and `-` only affect the term to the right of the operator, `AND` and `OR` can affect the terms to the left and right.

Rewriting the above query using `AND`, `OR` and `NOT` demonstrates the complexity:

### Functions

#### Exists

Returns documents that contain at least one value for `author.id`.

```
author.id.exists = true
{"field": "author.id.exists", "value": true}
```

#### Missing

Returns documents that only have null values or not any value for this field.

```
label.color.missing = true
{"field": "label.color.missing", "value": true}
```

### Not

We can use `not` keyword to not match a query.  
It can be very useful on complexe queries.  

```
not label = bag
{"not": {"field": "label", "value": "bag"}}
```

```
not label = bag where color = blue
{"not": {"field": "label", "value": "bag", "where": {"field": "color", "value": "blue"}}}
```

```
not (label = bag where color = blue or label = hat where color = red)
{"not": {"operator": "or", "items": [
   {"field": "label", "value": "bag", "where": {"field": "color", "value": "blue"}},
   {"field": "label", "value": "hat", "where": {"field": "color", "value": "red"}}
]}}
```

### Combinations

Filters can not be just separated by spaces, it need a combination instruction.

#### And combination

⚠️  **Warning**: This query return an image containing bag and something in red.

```
label = bag and label.color = red

{"operator": "and", "items": [
   {"field": "label", "value": "bag"},
   {"field": "label.color", "value": "red"}
]
```

#### Or combination

🗒️  **Note**: And combination is **priority**, thus brackets is not necessary.

```
(label.color = blue and label.color = green) or label.color = yellow

label.color = blue and label.color = green or label.color = yellow

{"operator": "or", "items": [
   {"operator": "and", "items": [
      {"field": "label.color", "value": "blue"},
      {"field": "label.color", "value": "green"}
   ]},
   {"field": "label.color", "value": "yellow"}
]
```

#### Where combination

The `where` combination is different of the `and` combination.

The where syntax allow to make a filter on a specific item of a list, instead of the whole document.

For example the following query return all documents with a bag and something in red, which is not necessary the bag.

`label = bag and label.color = red`

💡 **Note**: You can do an infinite number of `where` inside a `where`, there is no depth limit.

This query will return all red bag

```
label = bag where color = red

{"field": "label", "value": "bag", "where": {"field": "color", "value": "red"}}
```

This one will get all red foo bag.

```
label = bag where (color = red and model = foo)

{"field": "label", "value": "bag", "where": {"operator": "and", "items": [
   {"field": "color", "value": "red"},
   {"field": "model", "value": "foo"},
]}
```

But the where syntax in not perfect, because it can only be applied on `nested` type object, and not simple `object`. 

Thus the where syntax automatically selection the deeper nested field to apply the filter, then you can not control it, and the where order can sometimes just not work.

For example the first query will not work but the second will.

```sql
attribute = foo where label = bag
label = bag where attribute = foo
```

Here label and attribute are nested object, and attribute are under label, but label are not under attribute, this is why it does not work.

On not nested object, the both order will work.

```sql
label = pants where texture = denim
texture = denim where label = pants
```

Here texture is a simple `object`, then the deeper nested field is label, then the both query will apply on label level.

#### Context syntax

The `context` syntax works in the same way than `where` syntax but allow a better control on the nested level. Basically you can chose the nested level instead of let SEL decide for you.

💡 **Note**: You can do an infinite number of `where` inside a `where`, there is no depth limit.

For example if we want to select bag or leather of red color. 

```
label where ((label = bag or texture = leather) and color = red)

{"field": "label", "where": {"operator": "and", "items": [
  {"operator": "or", "items": [
    {"field": "label", "value": "bag"},
    {"field": "texture", "value": "leather"}
  ]}
  {"field": "color", "value": "red"}
]}}
```

Just for the example, even if this is not optimal, we can do this:

```
label where (label = top and label where (gender = female and color where (color = multicolor)))

{"field": "label", "where": {"operator": "and", "items": [
  {"field": "label", "value": "top"},
  {"field": "label", "where": {"operator": "and", "items": [
    {"field": "gender", "value": "female"},
    {"field": "color", "where": {"field": "color", "value": "multicolor"}
  ]}}
]}}
```

## Sorts

🗒️ **Note**: All examples bellow refer to `Schema example`.  

Must be placed at the end of the query string. Numerous sorts can be put, the order is important, from left to right.

For query object, use the key `"sort"` as a list.

```json
{"sort": [
  {"field": "label"},
  {"field": "label.model"},
]}
```

### Auto sort

By default there is an auto sort system in SEL, see `conf.ini` to disable it.  
Auto sort are made for each fields containing `DefaultObjectSortField`, such as:

```sql
label = bag # generate following sorts
sort: label desc where label = bag # In order to return best bag first.
```

🗒️  **Note**: Your own sorts are put before auto sorts.

### Special values

Disabled auto sort: `sort: null`

Enable auto sort: `sort: auto`

Random sort: `sort: random`

💡 **Note**: A special integer parameter "seed" can control the random: `sort: random seed 1`

### Order

Sort in descending order.

🗒️  **Note**: It's the default order, you don't have to specify it.

```
sort: label
sort: label desc

{"field": "label"}
{"field": "label", "order": "desc"}
```

Sort in ascending order

```
sort: label asc
{"field": "label", "order": "asc"}
```

### Mode

The mode allow to control the manner to build the score for a single document / if there is numerous score present in a single document.

Sort by average of values

🗒️  **Note**: It's the default order, you don't have to specify it.

```
sort: label mode avg 
{"field": "label", "mode": "avg"}
```

Sort by the minimum of all values in the document.

```
sort: label mode min
{"field": "label", "mode": "min"}
```

 

Sort by the maximum of all values in the document.

```
sort: label mode max
{"field": "label", "mode": "max"}
```

Sort by the sum of all values the document.

```
sort: label mode sum
{"field": "label.model", "mode": "sum"}
```

Sort by the median of all values in the document.

```
sort: label mode median
{"field": "label.model", "mode": "median"}
```

### Where filters

Apply filter on sort.

💡 **Note**: The whole filter syntax is available here.

Get best bag first.

```
sort: label where (label = bag)
{"field": "label", "where": {"field": "label", "value": "bag"}}
```

#### Under

Under syntax is used to apply given nested context for the `where` filter.

Get best bags' attribute first.

```
sort: label.attribute under label where (label = bag)

{
  "field": "label.attribute",
  "under": "label",
  "where": {"field": "label", "value": "bag"}
}
```

This sort can not works without `under`, because `label` field is not contained inside the attribute nested object. See [schema](/schema).

## Aggregations

🗒️ **Note**: All examples bellow refer to `Schema example`.  

Aggregations must be at the end of the query string.

### Query object

In query object keys are used to find back your aggregations in returned results, it's useful if you are doing numerous aggregations in a single query.

```json
{"aggregations": {
  "my_first_aggreg": {"field": "label"},
  "my_second_aggreg": {"type": "histogram", "field": "date"}
}}
```

**Return example**

```
{
  "results": {
    "aggregations": {

      "my_first_aggreg": {
	    "buckets": [ ... ]
      },

      "my_second_aggreg": {
        "buckets": [ ... ]
      }

    }
  }
}
```

### Aggregation types

Aggreg type is how you want to aggregate values.

- **aggreg**: Return all existing values, and it's number of occurrence.
- **histogram**: Count occurrence of range of values, for numerical and date. Default `size` is `0`.
- **count**: Count the number of occurrence of the field, whatever it's value.
- **distinct**: Count the number of unique values for the field.
- **min**: Return the minimum value of the field, for numerical and date only.
- **max**: Return the maximum value of the field, for numerical and date only.
- **sum**: Return the sum of values of the field, for numerical and date only.
- **average**: Return the average of values of the field, for numerical and date only.
- **stats**: Return numerous stats on the field: min, max, average, count, ..., for numerical only.

**Examples**

Get all values of labels and it's number of occurrence.

🗒️  **Note**: In query object `type` is `aggreg` by default.

```
aggreg: label
{"field": "date"}
{"type": "aggreg", "field": "date"}
```

Count posts per day.

💡 **Note**: `aggreg` type on date type field is actually doing an histogram aggregations.  
💡 **Note**: By default histogram `size` is set to `0`.  

```
aggreg: date
histogram: date
{"type": "histogram", "field": "date"}
```

Get all occurrences of labels, whatever it's value.

```
count: label
{"type": "count", "field": "label"}
```

Get number of different labels' value.

```
distinct: label
{"type": "distinct", "field": "label"}
```

### Named aggregations

You can named to your aggregations in queries, which is useful when you are doing numerous aggregations in the same query.

```sql
aggreg bag_texture: label.texture where label = bag
aggreg shoes_texture: label.texture where label = shoes
```

```json
{"aggregations": {
  "bag_texture": {
    "field": "label.texture",
    "where": {"field": "label", "value": "bag"}
  },
  "shoes_texture": {
    "field": "label.texture",
    "where": {"field": "label", "value": "shoes"}
  },
}}
```

### Where filters

Filter aggregations with a query.

💡 **Note**: The whole filter syntax is available here.

⚠️  **Warning**: Apply the filter on the nested context of the aggregated field, use `under` to change the nested field.

**Examples**

Limit aggregation results

```
aggreg: label.color where (color in [red, blue])

{
  "field": "label.color",
  "where": {"field": "color", "comparator": "in", "value": ["red", "blue"]}
}
```

Get all labels, excepted dress.

```
aggreg: label where label != dress

{
  "field": "label",
  "where": {"field": "label", "comparator": "!=", "value": "dress"}
}
```

#### Under

Specify the nested level on which the filter (where keyword) will be applied.

Get attributes of skirt.

```
aggreg: attribute under label where label = skirt

{
  "field": "attribute",
  "under": "label",
  "where": {"field": "label", "value": "dress"}
}
```

### Limit size

Select the number of top results.

🗒️  **Note**: Default is `Aggregations.DefaultSize` in `conf.ini`. A warning will be send if there is more values.
🗒️  **Note**: For histogram the default size is 0.

```
aggreg: label size 40
{"field": "label", "size": 40}
```

Return all results

```
aggreg: author size 0
{"field": "author", "size": 0}
```

### Histogram interval

Select the date aggregation interval for numerical or date type.

🗒️  **Note**: Default is `Aggregations.DefaultDateInterval` in `conf.ini`.  

Available expressions for interval: year, quarter, month, week, day, hour, minute, second

```
aggreg: date interval month
{"field": "date", "interval": "month"}
```

```
histogram: date interval year
{"type": "histogram", "field": "date", "interval": "year"}
```

Advance interval exists, such as:

```sql
<number><interval_letter>
2d  # 2 days
3h  # 3 hours
```

See [Time units](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/common-options.html#time-units)

### Sub aggregations

Will proceed an aggregation under each bucket of the parent aggregation.

Sub aggregations are named and several sub aggregations can be done on the same parent aggregation.

For example to get all color values of each labels:

```
aggreg: label subaggreg my_aggreg (aggreg: color size 2)

{
  "field": "label",
  "subaggreg": {"my_aggreg": {"field": "color"}}
}
```

To get all colors and textures values for each labels.

```
aggreg: label subaggreg col (aggreg: color size 2) subaggreg tex (aggreg: texture size 2)

{
  "field": "label",
  "subaggreg": {
    "col": {"field": "color"},
    "tex": {"field": "texture"}
  }
}
```

It will return something like:

```
{
  "buckets": [
    {
      "key": "dress",
      "doc_count": 203,
      "col": {
        "buckets": [
          {"key": "black", "doc_count": 88},
          {"key": "brown", "doc_count": 80}
        ]
      },
      "tex": {
        "buckets": [
          {"key": "printed", "doc_count": 52},
          {"key": "floral", "doc_count": 29}
        ]
      }
    },
    {
      "key": "shoes",
      "doc_count": 162,
      "col": {"buckets": []},
      "tex": {
        "buckets": [
          {"key": "leather", "doc_count": 3},
          {"key": "printed", "doc_count": 2}
        ]
      }
    },
    ...
}
```

## Big Examples

🗒️ **Note**: All examples bellow refer to `Schema example`.  

### Trend moodboard

```sql
trend.entity = "pants:women:denim:straight"
AND .author.geozone = eu
AND 2021-07 <= date <= 2021-12

SORT: date
```

### Fetch all trend signals

```sql
label WHERE (
   gender = female
   AND label.trend.entity = "pattern:plain"
   AND label IN ["tops", "outerwear", "pants", "shorts", "dresses", "skirts"]
)
AND 2015-08-10 <= date <= 2021-02-14

AGGREG labels: label WHERE (gender = female AND label.trend.entity = "pattern:plain" AND label IN ["tops", "outerwear", "pants", "shorts", "dresses", "skirts"])
   SUBAGGREG raw_signal (AGGREG: date INTERVAL week WHERE author.geozone = eu)
   SUBAGGREG mainstream (AGGREG: date INTERVAL week WHERE (follower < 12000 AND author.geozone = eu))
   SUBAGGREG trendy (AGGREG: date INTERVAL week WHERE (12000 <= follower < 40000 AND author.geozone = eu))
   SUBAGGREG edgy (AGGREG: date INTERVAL week WHERE (follower >= 40000 AND author.geozone = eu))
   SUBAGGREG followers_low (AGGREG: date INTERVAL week WHERE (follower < 1350 AND author.geozone = eu))
   SUBAGGREG followers_mid (AGGREG: date INTERVAL week WHERE (1350 <= follower < 7000 AND author.geozone = eu))
   SUBAGGREG followers_high (AGGREG: date INTERVAL week WHERE (follower >= 7000 AND author.geozone = eu))
   SUBAGGREG fashion_forwards (AGGREG: date INTERVAL week WHERE panel_name = fashion_forward)
```
The heaviest query ever used. First part of the query filter documents and second part return timeseries' histogram for each type of label.  
The label query must be repeat inside the first aggregation level to be sure the aggregation is apply on the trend. For eg. `tops` can be in labels but not on the expected trend.  
Finally each subaggreg will return a specific timeserie used for trend metrics and forecasting and will be applied on each labels.  
This query can be too heavy to apply at once if there is lot of data in ES or if the cluster has not enought computation power and results to timeout. In that case the best is to apply each subaggreg query one by one.


### Key Metrics

```sql
label.model.entity = "bag:30montaigneleatherflap"
AND panel_category IN ["fashion", "sport"]
AND tag != commercial
AND tag != owned_content WHERE (tag.brand.entity = "br:750")
AND 2020-06-19 <= date <= 2020-07-19

COUNT post: .id
SUM follower: follower
SUM engagement: engagement
```

### Evolution chart

```sql
panel_category IN ["fashion", "sport"]
AND tag != commercial
AND tag != owned_content WHERE (tag.brand.entity = "br:750")
AND 2020-06-19 <= date <= 2020-07-19

AGGREG norm: date WHERE (label.model.exists = true)
AGGREG models: date INTERVAL day WHERE (label.model.entity = "bag:30montaigneleatherflap")
```

### Evolution of audience breakdown

```sql
label.model.entity = "bag:30montaigneleatherflap"
AND panel_category IN ["fashion", "sport"]
AND tag != commercial
AND tag != owned_content WHERE (tag.brand.entity = "br:750")
AND 2020-06-19 <= date <= 2020-07-19

AGGREG voice: date INTERVAL day WHERE (follower < 1000 OR follower.exists = false)
AGGREG micro: date INTERVAL day WHERE 1000 <= follower < 10000
AGGREG macro: date INTERVAL day WHERE 10000 <= follower < 500000
AGGREG superstar: date INTERVAL day WHERE follower >= 500000
```

### Geographical breakdown

```sql
label.model.entity = "bag:30montaigneleatherflap"
AND panel_category IN ["fashion", "sport"]
AND tag != commercial
AND tag != owned_content WHERE (tag.brand.entity = "br:750")
AND 2020-06-19 <= date <= 2020-07-19

COUNT norm: .id
AGGREG models: continent SIZE 0
```

### Commercial content

```sql
label.model.entity = "bag:30montaigneleatherflap"
AND panel_category IN ["fashion", "sport"]
AND tag != owned_content
AND 2020-06-19 <= date <= 2020-07-19

COUNT norm: .id
COUNT commercial: .id WHERE tag = commercial
COUNT not_commercial: .id WHERE tag != commercial
```

### Brand mentions

```sql
label.model.entity = "bag:30montaigneleatherflap"
AND tag != commercial
AND tag != owned_content WHERE (tag.brand.entity = "br:750")
AND panel_category IN ["fashion", "sport"]
AND 2020-06-19 <= date <= 2020-07-19

COUNT norm: .id
COUNT brand_mention: .id WHERE (tag = brand_mention WHERE (brand.entity = "br:750"))
```