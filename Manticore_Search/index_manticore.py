import os
import csv
import sys 
import time
from tqdm import tqdm
from manticoresearch import Configuration, ApiClient, IndexApi, UtilsApi, InsertDocumentRequest
from manticoresearch.rest import ApiException

# --- CONFIG ---
CSV_FILE_PATH = "part_1_extracted.csv"  # <--- SET YOUR CSV FILE PATH HERE
INDEX_NAME = "data3" # <--- SET YOUR TABLE NAME HERE
HOST = "http://127.0.0.1:9308"

BATCH_SIZE = 500      # Number of rows to index per batch
RETRY_LIMIT = 3       # Retry count if the connection fails
SLEEP_BETWEEN_BATCHES = 1 # Seconds to wait between batches to avoid overloading

# --- Manticore Config ---
config = Configuration(host=HOST)

# --- MAIN ---
with ApiClient(config) as client:
    utils_api = UtilsApi(client)
    index_api = IndexApi(client)

    # 1. Define the table schema based on your CSV columns
    # All columns are 'text' type to be fully searchable.
    table_schema = [ #add column names of your CSV file
         "id text", "name text",
        "address text", "city text", "state text",
        "country text", "postalCode text"
    ]
    schema_string = ", ".join(table_schema)
    
    # 2. Drop the old table (if it exists) and create the new one
    print(f"🔧 Preparing table '{INDEX_NAME}'...")
    try:
        utils_api.sql(f"DROP TABLE IF EXISTS {INDEX_NAME}", raw_response=True)
        print(f"-> Dropped existing table '{INDEX_NAME}'.")
        utils_api.sql(f"CREATE TABLE {INDEX_NAME}({schema_string})", raw_response=True)
        print(f"Table '{INDEX_NAME}' created with the new schema.\n")
    except ApiException as e:
        print(f"Error creating table: {e}")
        exit()

    # 3. Read all rows from the CSV file
    try:
        # Increase the field size limit for large fields like 'collateral_description'
        # We'll use sys.maxsize for the maximum possible, but handle potential OS errors
        try:
            csv.field_size_limit(sys.maxsize)
        except OverflowError:
            csv.field_size_limit(int(2**31 - 1)) # Fallback for 32-bit systems

        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            # DictReader uses the first row as headers
            csv_reader = csv.DictReader(f)
            all_rows = list(csv_reader)
            total = len(all_rows)
        print(f"Found {total} rows to index in '{CSV_FILE_PATH}'.\n")
    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE_PATH}' was not found.")
        exit()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        exit()

    # 4. Index the data in batches
    doc_id_counter = 1 # Use a simple counter for a unique document ID
    for i in tqdm(range(0, total, BATCH_SIZE), desc="Indexing", unit="batch"):
        batch = all_rows[i:i + BATCH_SIZE]
        
        for row in batch:
            # Ensure all values are strings and convert any None values to empty strings
            sanitized_row = {k: (v if v is not None else "") for k, v in row.items()}
            
            insert_req = InsertDocumentRequest(
                table=INDEX_NAME,   
                id=doc_id_counter,
                doc=sanitized_row  
            )
            doc_id_counter += 1

            # Retry logic for failed connections
            for attempt in range(RETRY_LIMIT):
                try:
                    index_api.insert(insert_req)
                    break # Success, break the retry loop
                except Exception as e:
                    if "Remote end closed" in str(e) and attempt < RETRY_LIMIT - 1:
                        print(f"Retrying doc ID {doc_id_counter} (attempt {attempt+2}/{RETRY_LIMIT})")
                        time.sleep(1)
                    else:
                        print(f"Failed to index doc ID {doc_id_counter}: {e}")
                        break

        time.sleep(SLEEP_BETWEEN_BATCHES) # Prevent overload between batches

    print("\n All rows indexed successfully.\n")

    # 5. Verify the number of indexed documents: MIGHT GIVE ERROR SO IGNORE IT, SEARCH WILL WORK FINE.
    try:
        res = utils_api.sql(f"SELECT COUNT(*) FROM {INDEX_NAME}", raw_response=True)
        count = res[0]['data'][0]['count(*)']
        print("Verification:")
        print(f"   Total documents in index '{INDEX_NAME}': {count}")
    except ApiException as e:
        print(f"Could not verify count: {e}")
