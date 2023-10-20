# Quickstart

SEL is using index schema to generate queries.  
Be aware it will request ES schema at any query generation.  

## Compagny
SEL was initially developed for Heuritech in 2016 and used by everybody inside the compagny tech and no-tech people since that time to explore internal data, generate reports and analysis.

## SEL as ES interface
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.search("my_index", "label = bag")
```


## SEL as ES query generator
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.generate_query("label = bag", index="my_index")["elastic_query"]
```


## SEL as offline ES query generator
```
from sel.sel import SEL

sel = SEL(None)
sel.generate_query("label = bag", schema=my_index_schema)["elastic_query"]
```
