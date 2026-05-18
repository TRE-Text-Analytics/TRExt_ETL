import os
import psycopg
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / 'config.env')

SOURCE_URL = os.getenv('SOURCE_DB_URL')
TARGET_URL = os.getenv('TARGET_DB_URL')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5000))
FETCH_SIZE = int(os.getenv('FETCH_SIZE', 2000))

def get_connections():
    """Returns a tuple of (source_conn, target_conn)"""
    src = psycopg.connect(SOURCE_URL)
    tgt = psycopg.connect(TARGET_URL)
    return src, tgt

def get_checkpoint(cursor, table_name):
    cursor.execute("SELECT last_processed_id FROM omop_temp.etl_checkpoint WHERE table_name = %s", (table_name,))
    row = cursor.fetchone()
    return row[0] if row else 0

def create_checkpoint_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS omop_temp.etl_checkpoint (
            table_name VARCHAR(255) PRIMARY KEY,
            last_processed_id BIGINT
        )
    """)
    cursor.connection.commit()

def update_checkpoint(cursor, table_name, last_id):
    cursor.execute("""
        INSERT INTO omop_temp.etl_checkpoint (table_name, last_processed_id) 
        VALUES (%s, %s)
        ON CONFLICT (table_name) DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id
    """, (table_name, last_id))

def generic_flush(cursor, data, table_name, columns=None):
    """
    A reusable flush function for any table using psycopg3's optimized executemany.
    """
    table_name = table_name.replace("omop_temp", "omop_nlp")
    if not data:
        return

    # Generate the correct number of placeholders: (%s, %s, %s)
    num_cols = len(data[0])
    placeholders = ", ".join(["%s"] * num_cols)
    
    if columns:
        cols_str = ", ".join(columns)
        statement = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    else:
        # Relies on the transformed row matching the table's exact column order
        statement = f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        
    cursor.executemany(statement, data)

def flush_drug_exposure(cursor, data):
    """
    A specialized flush function for the drug_exposure table, which may have specific constraints or indexes.
    For now, it behaves the same as generic_flush but can be optimized later if needed.
    """
    generic_flush(cursor, data, "omop_nlp.drug_exposure", columns=["drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date", "drug_type_concept_id"])

def flush_observation(cursor, data):
    """
    A specialized flush function for the observation table, which may have specific constraints or indexes.
    For now, it behaves the same as generic_flush but can be optimized later if needed.
    """
    generic_flush(cursor, data, "omop_nlp.observation", columns=["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_type_concept_id"])

def flush_procedure_occurrence(cursor, data):
    """
    A specialized flush function for the procedure_occurrence table, which may have specific constraints or indexes.
    For now, it behaves the same as generic_flush but can be optimized later if needed.
    """
    generic_flush(cursor, data, "omop_nlp.procedure_occurrence", columns=["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date", "procedure_type_concept_id"])

def flush_measurement(cursor, data):
    """
    A specialized flush function for the measurement table, which may have specific constraints or indexes.
    For now, it behaves the same as generic_flush but can be optimized later if needed.
    """
    generic_flush(cursor, data, "omop_nlp.measurement", columns=["measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_type_concept_id"])

def flush_condition_occurrence(cursor, data):
    """
    A specialized flush function for the condition_occurrence table, which may have specific constraints or indexes.
    For now, it behaves the same as generic_flush but can be optimized later if needed.
    """
    generic_flush(cursor, data, "omop_nlp.condition_occurrence", columns=["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date", "condition_type_concept_id"])

def flush_domain_buffer(cursor, domain_buffer):
    """
    Flushes the domain-specific buffer to the appropriate tables.
    domain_buffer is a list of tuples: (domain_table_name, row_data)
    """
    domain_data = {}
    for domain_table, row in domain_buffer:
        if domain_table not in domain_data:
            domain_data[domain_table] = []
        domain_data[domain_table].append(row)

    for domain_table, rows in domain_data.items():
        if domain_table == "condition_occurrence":
            flush_condition_occurrence(cursor, rows)
        elif domain_table == "measurement":
            flush_measurement(cursor, rows)
        elif domain_table == "procedure_occurrence":
            flush_procedure_occurrence(cursor, rows)
        elif domain_table == "drug_exposure":
            flush_drug_exposure(cursor, rows)
        elif domain_table == "observation":
            flush_observation(cursor, rows)