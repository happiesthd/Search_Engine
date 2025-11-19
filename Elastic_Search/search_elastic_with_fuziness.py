
import streamlit as st
from elasticsearch import Elasticsearch
import json
import time

# -------------------------------
# Elasticsearch connection setup
# -------------------------------
ES_HOST = "http://localhost:9200"
INDEX_NAME = "data3"

es = Elasticsearch(ES_HOST)

# -------------------------------
# Streamlit UI
# -------------------------------
st.set_page_config(page_title="Name Search", page_icon="💼", layout="centered")
st.title("Advanced Debtor Search (Elasticsearch)")
st.info(
    """
    **Search Logic:**
    - **Fuzziness:** All searches allow for up to 2 typos (fuzziness: 2) on *each word*.
    - **Debtor Name:** - 2 words or less (e.g., `JOHN): Uses a broad **OR** search.
        - 3 words or more (e.g., `JOHN CHRISTOPER DO`): Uses a precise **AND** search.
    - **Address:** Uses the same OR/AND logic.
    - **Combined Search:** If both name and address are provided, documents must match **both** criteria.
    """
)

debtor_name = st.text_input("Enter Debtor Name", placeholder="e.g. JOHN DOE")
debtor_address = st.text_input("Enter Debtor Address", placeholder="e.g. 1234 MAIN ST, TX")
search_button = st.button("Search")

# -------------------------------
# Helper function
# -------------------------------
def extract_hit_payload(hit):
    return hit.get("_source", {})


# -------------------------------
# Search function
# -------------------------------
def search_debtor(name=None, address=None):
    
    if not name and not address:
        return [], "No query provided." 

    query_clauses = []
    
    NAME_FIELD = "debtor_name"
    ADDRESS_FIELD = "debtor_address"
    FUZZINESS = 2 # Define fuzziness level

    if name:
        name_query_str = name.strip()
        name_parts = name_query_str.split()
        if len(name_parts) > 0:
            fuzzy_name_parts = [f"{part}~{FUZZINESS}" for part in name_parts]

            if len(name_parts) >= 2:
                join_operator = " AND " # Precision search
            else:
                join_operator = " OR " # Discovery/broad search
            
            name_query_string = join_operator.join(fuzzy_name_parts)
            
            query_clauses.append({
                "query_string": {
                    "query": name_query_string,
                    "default_field": NAME_FIELD
                }
            })
       
    if address:
        address_query_str = address.strip()
        address_parts = address_query_str.split()
        if len(address_parts) > 0:
            fuzzy_address_parts = [f"{part}~{FUZZINESS}" for part in address_parts]
            
            if len(address_parts) >= 2:
                join_operator = " AND " # Precision search
            else:
                join_operator = " OR " # Discovery/broad search

            address_query_string = join_operator.join(fuzzy_address_parts)

            query_clauses.append({
                "query_string": {
                    "query": address_query_string,
                    "default_field": ADDRESS_FIELD
                }
            })
       
    if len(query_clauses) == 0:
        return [], "No query provided."
    elif len(query_clauses) == 1:
        final_query_body = {"query": query_clauses[0]}
    else:
        final_query_body = {"query": {"bool": {"must": query_clauses}}}

    try:
        response = es.search(index=INDEX_NAME, body=final_query_body, size=50)
        return response.get("hits", {}).get("hits", []), None # Return (results, error) tuple
    except Exception as e:
        error_message = f"Elasticsearch API Error: {e}"
        try:
            error_details = e.info
            st.error("Error Details:")
            st.json(error_details, expanded=False)
        except:
            st.exception(e)
        return [], error_message 


# -------------------------------
# UPDATED UI Logic
# -------------------------------
if search_button:
    name_input = debtor_name.strip()
    address_input = debtor_address.strip()

    if not name_input and not address_input:
        st.warning("Please enter at least a debtor name or address.")
    else:
        with st.spinner("Searching Elasticsearch for all name variations..."):
            start_time = time.time()
           
            results, error = search_debtor(
                name=name_input or None,
                address=address_input or None,
            )
            elapsed_time = time.time() - start_time
            
            if error:
                st.error(f"{error}")
            else:
                st.info(f"Search completed in {elapsed_time:.2f} seconds")

                if not results:
                    st.error("No matching documents found.")
                else:
                    st.success(f"Found {len(results)} matching document(s)")

                    for i, hit in enumerate(results, start=1):
                        st.markdown(f"### Result {i}")
                        payload = extract_hit_payload(hit)
                        if isinstance(payload, dict):
                            st.json(payload, expanded=True)
                        else:
                            st.write(payload)

st.markdown("---")
