# ElasticSearchDump
Lightweight tool for dumping ElasticSearch index to json file

It expects https for connection to host.

Usage example:
1. create **output** folder (path to it specified in **output** parameter)
2. Execute 

`-user=elastic -password=<YOURPASSWORD> -port=9200 -host=localhost -output=./output -index=variables_index`

By default data is downloaded in batched and saved by 1000 documents per file. 
