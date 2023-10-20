# SEL
For Simple Elastic Language offer an easy way to query ElasticSearch for every body even no-tech people and even on a big, complex and nested schema.  
  
The project is split into two subprojects:  
- SEL, which is the library  
- SEL Server, unlock quick usage by connecting directly to ES.  


## Versions
Two first digits of SEL version match Elasticsearch version and then it's the inner SEL version, eg 2.4.1 works with ES 2.4, v1 of SEL for this version of ES


## Full documentation
[SEL doc](https://arnaudparant.github.io/sel)  
[SEL Server doc](http://localhost:9000/docs) (need `make start-server`)  


## Compagny
SEL was initially developed for Heuritech in 2016 and used by everybody inside the compagny tech and no-tech people since that time to explore internal data, generate reports and analysis.


## Quickstart
SEL is using index schema to generate queries.  
Be aware it will request ES schema at any query generation.  


### SEL as ES interface
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.search("my_index", "label = bag")
```

### SEL as ES query generator
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.generate_query("label = bag", index="my_index")["elastic_query"]
```

### SEL as offline ES query generator
```
from sel.sel import SEL

sel = SEL(None)
sel.generate_query("label = bag", schema=my_index_schema)["elastic_query"]
```

### SEL as API (SEL Server)
```
curl -X POST -H "Content-Type: application/json" -d '{"query": "label = bag"}' http://localhost:9000/search/my_index
curl -X POST -H "Content-Type: application/json" -d '{"query": {"field": "label", "value": "bag"}}' http://localhost:9000/search/my_index
```

  
## Makefile rules  
  
### docker
Build a SEL docker

### docker-test
Build a SEL docker for tests

### lint
Run lint on the code

### tests
Run all tests

### upshell
Up a shell into a docker test.  
Useful to run only one or few tests.

### install-sphinx
Install Sphinx and dependencies to generate documentation

### doc
Generate the documentation in `docs/build/html/`  
