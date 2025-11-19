This repo contains execution of Elastic search on csv file that has large amount of data. The search speed is significantly fast. (0.05 -2 secs). Here elastic search is used without enabling the password authentication. This can be configured as per need.

Pre-requisites: Docker with Elastic image/ local manual setup of Elastic Search server.

Step-1: Start docker server for Elastic Search with: docker run -d --name elasticsearch -p 9200:9200 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:8.14.3

Step-2: Do the indexing and store it. Run index_elastic.py. index_elastic.py: contains code to index the csv file and store it in docker volumes with suitable INDEX_NAME.

Step-3: Run the search in streamlit app. streamlit run search_elastic.py. search_elastic.py: contains code to connect to INDEX_NAME on localhost and runs a streamlit app to perform search.

or can run search_elastic_with_fuziness.py to use search with fuziness (spelling mistake/missing spealling also can be searched). 

Adjust FUZZINESS = 2 as per need


Usefull docker commands: 


docker rm elasticsearch


docker ps 


docker restart elasticsearch
