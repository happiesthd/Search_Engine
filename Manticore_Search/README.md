This repo contains execution of Manticore search on csv file that has lagra amount of data. The search speed is significantly fast. (0.05 -2 secs)

Pre-requisites:
Docker with Manticore image/ local manual setup of Manticore server.


Step-1: Start docker server for Manticore with:
docker run -d --name manticore-server -p 9308:9308 -e EXTRA=1 manticoresearch/manticore

Step-2: Do the indexing and store it. Run index_manticore.py.
index_manticore.py: contains code to index the csv file and store it in docker volumes with suitable INDEX_NAME.

Step-3: Run the search in streamlit app. stramlit run search_manticore.py.
search_manticore.py: contains code to connect to INDEX_NAME on localhost and runs a streamlit app to perform search.

Usefull docker commands:
docker rm manticore-server
docker ps
docker restart manticore-server
