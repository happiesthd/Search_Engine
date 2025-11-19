from elasticsearch import Elasticsearch, helpers
import os
import csv
import sys 
import time

# --- 1. Configuration ---
ES_HOST = "http://localhost:9200"
INDEX_NAME = "data3"
CSV_FILE_PATH = "part_1_extracted.csv" # ADD PATH TO YOUR CSV

# --- 2. Connect to Elasticsearch ---
try:
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError("Could not connect to Elasticsearch.")
    print("Connected to Elasticsearch")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()


# --- 3. Create Index with Dynamic Mapping ---
if es.indices.exists(index=INDEX_NAME):
    es.indices.delete(index=INDEX_NAME)
    print(f"Deleted existing index: {INDEX_NAME}")

es.indices.create(index=INDEX_NAME, body={"mappings": {"dynamic": True}})
print(f"Created new index: {INDEX_NAME}")


# --- 4. Increase CSV Field Size Limit ---
# This allows the csv reader to handle very large fields
try:
    print("Setting CSV field size limit to maximum...")
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(int(2**31 - 1)) # Fallback for older systems


# --- 5. Read CSV and Prepare Data for Bulk Indexing ---
def yield_docs_from_csv(file_path):
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            doc_id = 1
            for row in reader:
                yield {
                    "_index": INDEX_NAME,
                    "_id": doc_id,
                    "_source": row,
                }
                doc_id += 1
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return

print(f"\nStarting to index documents from '{CSV_FILE_PATH}'...")
start_time = time.time()

try:
    es_with_timeout = es.options(request_timeout=60)
    success, failed = helpers.bulk(es_with_timeout, yield_docs_from_csv(CSV_FILE_PATH), chunk_size=500)
    
    end_time = time.time()
    print(f"Successfully indexed {success} documents.")
    if failed:
        print(f"Failed to index {len(failed)} documents.")
    print(f"Indexing took {end_time - start_time:.2f} seconds.")

except Exception as e:
    print(f"An error occurred during bulk indexing: {e}")

time.sleep(1)

# --- 6. Search Function ---
def search_debtor(debtor_name):
    """
    Searches for a document based on the 'debtor_name' field.
    """
    print(f"\nSearching for debtor_name = '{debtor_name}'")
    
    query = {
        "match": {
            "debtor_name": {
                "query": debtor_name,
                "operator": "and"
            }
        }
    }

    try:
        results = es.search(index=INDEX_NAME, query=query)
        hits = results.get("hits", {}).get("hits", [])
        
        if hits:
            print(f"Found {len(hits)} document(s):")
            for hit in hits:
                source = hit['_source']
                print(f"  → ID: {hit['_id']} | Filing Type: {source.get('filing_type', 'N/A')} | City: {source.get('debtor_city', 'N/A')}")
        else:
            print("  -> No results found.")
            
    except Exception as e:
        print(f"An error occurred during search: {e}")

# --- 7. Run Debtor Searches ---
search_debtor("JOHN DOE")
search_debtor("DONALD TRUMP")
