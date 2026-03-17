from domain_concept_mapping import get_max_ids, get_routing_map_batch
from person_map import lookup_person_id
import psycopg
from db import TARGET_URL

max_ids = get_max_ids(psycopg.connect(TARGET_URL))
last_ids = {
    "condition_occurrence": max_ids["condition_occurrence"],
    "measurement": max_ids["measurement"],
    "procedure_occurrence": max_ids["procedure_occurrence"],
    "drug_exposure": max_ids["drug_exposure"],
    "observation": max_ids["observation"],
}


def transform_note(row):
    """
    Input: (id, content, created_at)
    Output: Tuple for target_notes OR None if invalid
    """
    row = list(row) # Convert to list for mutability
    new_id = lookup_person_id(row[1]) # person_id is the second column in the note table
    # map to the new person_id created by Carrot-Transform
    row[1] = new_id
    # Use the OMOP concept_id for 

    # ensure no unlinked notes are inserted
    if new_id is None:
        return None
        
    return tuple(row), None

def get_domain_row(domain, nlp_row):
    new_row = None
    table_name = None
    if domain == "Condition":
        table_name = "condition_occurrence"
        new_row = (
            last_ids["condition_occurrence"] + 1,
            str(lookup_person_id(nlp_row[-1])), # person_id
            nlp_row[6], # standard_concept_id
            nlp_row[9], # date
            "32424" # "NLP derived" condition concept name
        )
        last_ids["condition_occurrence"] += 1
    elif domain == "Measurement":
        table_name = "measurement"
        new_row = (
            last_ids["measurement"] + 1,
            lookup_person_id(nlp_row[-1]), # person_id
            nlp_row[6], # standard_concept_id
            nlp_row[9], # date
            "32423" # "NLP derived" meas concept name
        )
        last_ids["measurement"] += 1
    elif domain == "Procedure":
        table_name = "procedure_occurrence"
        new_row = (
            last_ids["procedure_occurrence"] + 1,
            lookup_person_id(nlp_row[-1]), # person_id
            nlp_row[6], # standard_concept_id
            nlp_row[9], # date
            "32425" # "NLP derived" procedure concept name
        )
        last_ids["procedure_occurrence"] += 1
    elif domain == "Drug":
        table_name = "drug_exposure"
        new_row = (
            last_ids["drug_exposure"] + 1,
            lookup_person_id(nlp_row[-1]), # person_id
            nlp_row[6], # standard_concept_id
            nlp_row[9], # start date
            nlp_row[9], # end date (using same as start for simplicity)
            "32426" # "NLP derived" drug concept name
        )
        last_ids["drug_exposure"] += 1
    elif domain == "Observation":
        table_name = "observation"
        new_row = (
            last_ids["observation"] + 1,
            lookup_person_id(nlp_row[-1]), # person_id
            nlp_row[6], # standard_concept_id
            nlp_row[9], # date
            "32445" # "NLP derived" observation concept name
        )
        last_ids["observation"] += 1
    return (table_name, new_row)

def transform_nlp_batch(conn, rows):
    """
    Input: conn (an open psycopg connection), rows (list of note_nlp.*, note.person_id tuples)
    Output: List of (transformed_nlp_row, domain_row) tuples
    """
    if not rows:
        return []

    # Extract all unique SNOMED codes from the batch (assuming it's at index 6)
    # Using a set comprehension guarantees uniqueness, keeping our DB query as small as possible
    unique_snomed_codes = {str(row[6]) for row in rows if row[6]}

    # Fetch the mapping for ALL codes in this batch at once
    domain_lookup_map = get_routing_map_batch(conn, unique_snomed_codes)

    results = []
    valid_domains = {"Condition", "Measurement", "Procedure", "Drug", "Observation"}

    # Iterate through the batch and apply transformations in memory
    for row in rows:
        row_list = list(row) 
        snomed_code = str(row_list[6])
        domain_row = None
        
        # Look up the mapping from our in-memory dictionary
        domain_lookup = domain_lookup_map.get(snomed_code)

        if domain_lookup:
            domain_id = domain_lookup["domain_id"]
            if domain_id in valid_domains:
                row_list[6] = domain_lookup["standard_concept_id"]
                # Assuming get_domain_row is defined elsewhere
                domain_row = get_domain_row(domain_id, row_list) 
        
        row_tuple = tuple(row_list[:-1]) # Remove the joined person_id
        results.append((row_tuple, domain_row))

    return results