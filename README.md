# SEL
Simple Elastic Language offer an easy way to query ElasticSearch for everybody even no-tech people and even on a big, complex and nested schema.  
  
The project is split into two sub projects:  
- [SEL](https://github.com/ArnaudParant/sel), which is the library  
- [SEL Server](https://github.com/ArnaudParant/sel_server), unlock quick usage by connecting directly to ES.  


## Versions
Two first digits of SEL version match Elasticsearch version and then it's the inner SEL version, eg 6.8.1 works with ES 6.8, v1 of SEL for this version of ES


## Full documentation
[SEL doc](https://arnaudparant.github.io/sel) - Containing [Big queries' examples](https://arnaudparant.github.io/sel/query_guide.html#big-examples) and all the query synthax  
[SEL Server doc](https://arnaudparant.github.io/sel_server/)  


## Compagny
SEL was initially developed for Heuritech in 2016 and used by everybody inside the compagny tech and no-tech people since that time to explore internal data, generate reports and analysis.


## Quickstart
SEL is using index schema to generate queries.  
Be aware it will request ES schema at any query generation.  

#### Add as dependency
```
sel @ git+https://github.com/ArnaudParant/sel.git@v6.8.1
```

### SEL as ES interface
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.search("my_index", {"query": "label = bag"})
```

### SEL as ES query generator
```
from elasticsearch import Elasticsearch
from sel.sel import SEL

es = Elasticsearch(hosts="http://elasticsearch")
sel = SEL(es)
sel.generate_query({"query": "label = bag"}, index="my_index")["elastic_query"]
```

### SEL as offline ES query generator
```
from sel.sel import SEL

sel = SEL(None)
sel.generate_query({"query": "label = bag"}, schema=my_index_schema)["elastic_query"]
```

### SEL as API (SEL Server)
See [SEL Server](https://github.com/ArnaudParant/sel_server) for API usage
  
## Makefile rules  
  
 - **docker** - Build SEL docker
 - **docker-test** - Build SEL test docker
 - **lint** - Lint the code
 - **tests** - Run all tests
 - **upshell** - Up a shell into the docker, useful to run only few tests.  
 - **install-sphinx** - Install Sphinx and dependencies to generate documentation.  
 - **doc** - Generate the documentation in `docs/build/html/`  
 - **clean** - Clean all `__pycache__`


## Known issue

```
[1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
```

Execute the following command
```
sysctl -w vm.max_map_count=262144
```