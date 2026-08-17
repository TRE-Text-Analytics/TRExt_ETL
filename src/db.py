"""Database access: connections, checkpointing, and bulk flush helpers.

The ETL reads from the source database and writes to the target. The checkpoint
table lives in the *target* schema so that a batch's data and its checkpoint commit
in the same transaction — a flush either lands both or neither, which is what makes
resume correct.
"""

import os
import psycopg
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / "config.env")

SOURCE_URL = os.getenv("SOURCE_DB_URL")
TARGET_URL = os.getenv("TARGET_DB_URL")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", 2000))

CHECKPOINT_TABLE = "omop_nlp.etl_checkpoint"


def get_connections():
    """Return a tuple of (source_conn, target_conn)."""
    src = psycopg.connect(SOURCE_URL)
    tgt = psycopg.connect(TARGET_URL)
    return src, tgt


# --- checkpointing (all against the TARGET connection) ---------------------


def get_checkpoint(cursor, table_name):
    cursor.execute(
        f"SELECT last_processed_id FROM {CHECKPOINT_TABLE} WHERE table_name = %s",
        (table_name,),
    )
    row = cursor.fetchone()
    return row[0] if row else 0


def create_checkpoint_table(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            table_name VARCHAR(255) PRIMARY KEY,
            last_processed_id BIGINT
        )
        """
    )
    cursor.connection.commit()


def update_checkpoint(cursor, table_name, last_id):
    cursor.execute(
        f"""
        INSERT INTO {CHECKPOINT_TABLE} (table_name, last_processed_id)
        VALUES (%s, %s)
        ON CONFLICT (table_name) DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id
        """,
        (table_name, last_id),
    )


# --- flushing --------------------------------------------------------------


def generic_flush(cursor, data, table_name, columns=None):
    """Bulk-insert ``data`` into ``table_name`` using psycopg3's executemany.

    ``table_name`` may be given with the source schema; it is rewritten to the
    target schema here. When ``columns`` is provided the INSERT names them
    explicitly; otherwise it relies on the row matching the table's column order.
    """
    table_name = table_name.replace("omop_temp", "omop_nlp")
    if not data:
        return

    placeholders = ", ".join(["%s"] * len(data[0]))
    if columns:
        cols_str = ", ".join(columns)
        statement = (
            f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )
    else:
        statement = (
            f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )

    cursor.executemany(statement, data)


# Column lists for each OMOP domain table, in insert order.
_DOMAIN_COLUMNS = {
    "condition_occurrence": [
        "condition_occurrence_id",
        "person_id",
        "condition_concept_id",
        "condition_start_date",
        "condition_type_concept_id",
    ],
    "measurement": [
        "measurement_id",
        "person_id",
        "measurement_concept_id",
        "measurement_date",
        "measurement_type_concept_id",
        "value_as_number",
        "measurement_source_value",
        "unit_concept_id",
    ],
    "procedure_occurrence": [
        "procedure_occurrence_id",
        "person_id",
        "procedure_concept_id",
        "procedure_date",
        "procedure_type_concept_id",
    ],
    "drug_exposure": [
        "drug_exposure_id",
        "person_id",
        "drug_concept_id",
        "drug_exposure_start_date",
        "drug_exposure_end_date",
        "drug_type_concept_id",
    ],
    "observation": [
        "observation_id",
        "person_id",
        "observation_concept_id",
        "observation_date",
        "observation_type_concept_id",
    ],
}


def flush_domain_buffer(cursor, domain_buffer):
    """Group a mixed domain buffer by target table and bulk-insert each group.

    ``domain_buffer`` is a list of ``(domain_table_name, row)`` tuples. Rows are
    assumed already valid — person mapping and skipping happen in the transform
    stage, so this function is a pure writer.
    """
    grouped = {}
    for domain_table, row in domain_buffer:
        grouped.setdefault(domain_table, []).append(row)

    for domain_table, rows in grouped.items():
        columns = _DOMAIN_COLUMNS.get(domain_table)
        if columns is None:
            continue  # unknown domain table; nothing to write
        generic_flush(cursor, rows, f"omop_nlp.{domain_table}", columns=columns)
