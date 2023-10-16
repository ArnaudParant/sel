# SEL
For Simple Elastic Language offer an easy way to query ElasticSearch for every body even no-tech people and even on a big, complex and nested schema.  
  
The project is split into two subprojects:  
- SEL, which is the library  
- SEL Server, unlock quick usage by connecting directly to ES.  


## Versions
Two first digits of SEL version match Elasticsearch version and then it's the inner SEL version, eg 2.4.1 works with ES 2.4, v1 of SEL for this version of ES


## Full documentation
[SEL doc](projects/sel/docs/build/html/index.html)  
[SEL Server doc](http://localhost:9000/docs) (need to `make start-server`)  


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
Here `{project}` is `sel` or `sel_server`.  
  
### docker-`{project}`
Build a docker of the project

### docker-test-`{project}`
Build a docker of the project for test purpose

### lint-`{project}`
Lint the code of the project

### tests-`{project}`
Run all tests of the project

### upshell-`{project}`
Up a shell into a docker test of the project.  
Useful to run only one or few tests.

### start-server  
Start SEL Server project into a docker.  
Accessible locally on port [9000](http://localhost:9000)  
See server documentation [here](http://localhost:9000/docs)  

### down-server  
Down SEL Server project

### install-sphinx
Install Sphinx and dependencies to generate documentation

### doc
Generate the documentation  
Will be accessible [here](projects/sel/docs/build/html/index.html)
