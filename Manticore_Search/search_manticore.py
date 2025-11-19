import streamlit as st
from manticoresearch import Configuration, ApiClient, SearchApi, SearchRequest
from manticoresearch.rest import ApiException
import json
import time
from itertools import permutations

# -------------------------------
# Manticore connection setup
# -------------------------------
MANTICORE_HOST = "http://127.0.0.1:9308"
INDEX_NAME = "data3"  # Make sure this is your correct index name

# Manticore configuration
config = Configuration(host=MANTICORE_HOST)

# -------------------------------
# Streamlit UI
# -------------------------------
st.set_page_config(page_title="Legal Data Search", page_icon="", layout="centered")
st.title("Advanced Data Search")
st.markdown(f"Searching Manticore Index: **{INDEX_NAME}**")

st.info(
    """
    **Search Logic:**
    - **Name:** For multi-word names (e.g., `JOHN DOE`), the search will find all possible orders (like `DOE JOHN`).
    - **Address:** Uses exact phrase search for multiple words and partial term search for single words.
    - **Combined Search:** If both name and address are provided, documents must match **both** criteria.
    """
)

# --- UI with separate input fields ---
debtor_name_query = st.text_input(
    "Debtor Name",
    placeholder="e.g., JOHN DOE"
)

address_query = st.text_input(
    "Debtor Address",
    placeholder="e.g., 123 Main St Anytown (for exact) or Anytown (for partial)"
)

search_button = st.button("Search 🔎")

# -------------------------------
# Helper function
# -------------------------------
def extract_hit_payload(hit):
    """Extract document data safely from Manticore hits."""
    return hit.source

# -------------------------------
# REFINED Search function with Permutations
# -------------------------------
def search_manticore(debtor_name="", address=""):
    """
    Refined search logic:
    - For multi-word names, it generates all permutations and performs an OR search.
    - Uses 'match_phrase' for multi-word address queries.
    - Uses 'match' for single-word queries.
    - Combines name and address with an AND condition.
    """
    if not debtor_name and not address:
        return [], "No query provided."

    query_clauses = []

    # --- UPDATED logic for Debtor Name with Permutations ---
    if debtor_name:
        name_parts = debtor_name.strip().split()
        if len(name_parts) > 1:
            name_permutations = set([" ".join(p) for p in permutations(name_parts)])
            permutation_clauses = [
                {"match_phrase": {"debtor_name": name_variant}} for name_variant in name_permutations
            ]
            name_query = {
                "bool": {
                    "should": permutation_clauses,
                    "minimum_should_match": 1
                }
            }
            query_clauses.append(name_query)
        else:
            query_clauses.append({"match": {"debtor_name": debtor_name}})

    # --- Logic for Address ---
    if address:
        if ' ' in address.strip():
            query_clauses.append({"match_phrase": {"debtor_address": address}})
        else:
            query_clauses.append({"match": {"debtor_address": address}})

    # --- Determine the final query structure ---
    if len(query_clauses) == 2:
        final_query = {"bool": {"must": query_clauses}}
    elif len(query_clauses) == 1:
        final_query = query_clauses[0]
    else:
        return [], "No query provided."

    search_request = SearchRequest(
        table=INDEX_NAME, 
        query=final_query,
        limit=50,  
        _source=["*"]      # Using the more standard '_source' parameter
    )

    try:
        with ApiClient(config) as client:
            search_api = SearchApi(client)
            response = search_api.search(search_request)
            return response.hits.hits, None
    except ApiException as e:
        error_message = f"Manticore API Error: {e.reason}"
        st.error(f"{error_message}")
        error_details = json.loads(e.body)
        st.json(error_details, expanded=False)
        return [], error_message
    except Exception as e:
        error_message = "An unexpected error occurred during the search."
        st.error(f"{error_message}")
        st.exception(e)
        return [], error_message

# -------------------------------
# UI Logic
# -------------------------------
if search_button:
    name_input = debtor_name_query.strip()
    address_input = address_query.strip()

    if not name_input and not address_input:
        st.warning("Please enter a Name or an Address to search.")
    else:
        with st.spinner("🔍 Searching Manticore for all name variations..."):
            start_time = time.time()
            results, error = search_manticore(debtor_name=name_input, address=address_input)
            elapsed_time = time.time() - start_time

            if error:
                pass
            else:
                st.info(f"Search completed in {elapsed_time:.3f} seconds")
                if not results:
                    st.error("No matching documents found.")
                else:
                    st.success(f"Found {len(results)} matching document(s)")
                    for i, hit in enumerate(results, start=1):
                        st.markdown(f"--- \n### 📄 Result {i}")
                        payload = extract_hit_payload(hit)
                        st.json(payload, expanded=True)

st.markdown("---")

# =========with fuzziness TBA=============
