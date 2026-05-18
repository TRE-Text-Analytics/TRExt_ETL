import psycopg
from psycopg.rows import dict_row

def get_max_ids(conn):
    tables = [
        "condition_occurrence",
        "measurement",
        "procedure_occurrence",
        "drug_exposure",
        "observation",
    ]

    id_cols = {
        "condition_occurrence": "condition_occurrence_id",
        "measurement": "measurement_id",
        "procedure_occurrence": "procedure_occurrence_id",
        "drug_exposure": "drug_exposure_id",
        "observation": "observation_id",
    }

    result = {}
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(
                f"SELECT COALESCE(MAX({id_cols[table]}),0) FROM omop_nlp.{table}"
            )
            result[table] = cur.fetchone()[0]

    return result

def get_routing_map_batch(conn, snomed_codes):
    """Fetches OMOP mappings for a list of SNOMED codes in a single query."""
    if not snomed_codes:
        return {}

    query = """
    SELECT 
        source.concept_code,
        COALESCE(target.concept_id, source.concept_id) AS standard_concept_id,
        COALESCE(target.domain_id, source.domain_id) AS domain_id
    FROM omop_nlp.concept source
    LEFT JOIN omop_nlp.concept_relationship cr
        ON source.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        AND cr.invalid_reason IS NULL
    LEFT JOIN omop_nlp.concept target
        ON cr.concept_id_2 = target.concept_id
    WHERE source.concept_code = ANY(%s)  -- Fetch all matches in one go
    AND source.vocabulary_id ILIKE 'SNOMED%%';
    """

    # We cast the set of codes to a list so psycopg translates it to a Postgres array
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (list(snomed_codes),))
        rows = cur.fetchall()

    # Convert the results into a lookup dictionary keyed by the original concept_code
    return {row["concept_code"]: row for row in rows}