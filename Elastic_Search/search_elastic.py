import streamlit as st
from elasticsearch import Elasticsearch
import json
import time
from itertools import permutations

# -------------------------------
# Elasticsearch connection setup
# -------------------------------
ES_HOST = "http://localhost:9200"
INDEX_NAME = "data3"

es = Elasticsearch(ES_HOST)

# -------------------------------
# Streamlit UI
# -------------------------------
st.set_page_config(page_title="Debtor Search", page_icon="💼", layout="centered")
st.title("Advanced Debtor Search (Elasticsearch)")

# UPDATED Info Box to explain new logic
st.info(
    """
    **Search Logic:**
    - **Debtor Name:** For multi-word names (e.g., JOHN DOE`), the search will find all possible orders (like `DOE JOHN`).
    - **Address:** Uses exact phrase search (`match_phrase`) for multiple words and partial term search (`match`) for single words.
    - **Combined Search:** If both name and address are provided, documents must match **both** criteria (AND search).
    """
)

debtor_name = st.text_input("Enter Debtor Name", placeholder="e.g.JOHN DOE")
debtor_address = st.text_input("Enter Debtor Address", placeholder="e.g. 1234 MAIN ST, TX")
search_button = st.button("Search 🔎")

# -------------------------------
# Helper function
# -------------------------------
def extract_hit_payload(hit):
    """Extract document data safely from Elasticsearch hits."""
    return hit.get("_source", {})


# -------------------------------
# REBUILT Search function
# -------------------------------
def search_debtor(name=None, address=None):
    if not name and not address:
        return [], "No query provided." # Return tuple for error handling

    query_clauses = []
    NAME_FIELD = "debtor_name"
    ADDRESS_FIELD = "debtor_address"
    if name:
        name_parts = name.strip().split()
        if len(name_parts) > 1:
            # Generate permutations
            name_permutations = set([" ".join(p) for p in permutations(name_parts)])
            permutation_clauses = [
                {"match_phrase": {NAME_FIELD: name_variant}} for name_variant in name_permutations
            ]
            # Create an OR (bool/should) query for all permutations
            name_query = {
                "bool": {
                    "should": permutation_clauses,
                    "minimum_should_match": 1
                }
            }
            query_clauses.append(name_query)
        else:
            query_clauses.append({"match": {NAME_FIELD: name}})

    if address:
        if ' ' in address.strip():
            # Multi-word address: use 'match_phrase'
            query_clauses.append({"match_phrase": {ADDRESS_FIELD: address}})
        else:
            # Single word address: use 'match'
            query_clauses.append({"match": {ADDRESS_FIELD: address}})

    # --- Determine the final query structure ---
    if len(query_clauses) == 0:
        return [], "No query provided."
    elif len(query_clauses) == 1:
        # Only one field was searched
        final_query_body = {"query": query_clauses[0]}
    else:
        # Both fields searched: use AND (bool/must)
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
        return [], error_message # Return (results, error) tuple


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
