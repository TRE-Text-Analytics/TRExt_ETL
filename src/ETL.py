"""ETL runner.

Streams each configured source table through a uniform pipeline:

    retrieve  ->  clean  ->  transform  ->  insert

Cleaning and transformation are pluggable per step. Data and its checkpoint are
written to the target in a single transaction per flush, so a run resumes exactly
where the last committed batch ended.
"""

import logging
import sys

from db import (
    BATCH_SIZE,
    FETCH_SIZE,
    create_checkpoint_table,
    flush_domain_buffer,
    generic_flush,
    get_checkpoint,
    get_connections,
    update_checkpoint,
)
from transform import (
    initialize_last_ids,
    transform_note,
    transform_nlp_batch,
    transform_nlp_relationship_batch,
    transform_nlp_relationship_part_batch,
)
from clean import clean_note_nlp_row
from rejects import log_reject

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("etl_errors.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ETL_Runner")


def process_table(src_conn, tgt_conn, table_config):
    """Stream one source table through clean -> transform -> flush.

    Supports both row-by-row transforms (``transform_func(row, columns)``) and
    batch transforms (``transform_func(conn, rows, columns)``), selected by the
    step's ``is_batch`` flag.

    Returns True if the step completed, False if it aborted (bad DDL, checkpoint
    failure, or a mid-run error). The caller uses this to stop the chain: a
    downstream step must not run after an upstream one aborted, or it would load
    rows referencing parents the failed step never inserted.
    """
    name = table_config["name"]
    query = table_config["query"]
    clean_func = table_config.get("clean_func")  # optional row cleaner
    transform_func = table_config["transform_func"]
    flush_func = table_config["flush_func"]
    # Target-table columns for the INSERT — kept separate from the SELECT columns,
    # since some steps retrieve extra columns (e.g. a joined person_id) that are
    # dropped before insert.
    insert_columns = table_config.get("insert_columns")
    is_batch_mode = table_config.get("is_batch", False)

    # Optional per-step target DDL.
    table_creation_sql = table_config.get("create_table")
    if table_creation_sql:
        try:
            with tgt_conn.cursor() as tgt_cur:
                tgt_cur.execute(table_creation_sql)
                tgt_conn.commit()
                logger.info(f"[{name}] Table creation/check completed.")
        except Exception as e:
            logger.error(f"[{name}] Failed to create/check table: {e}")
            return False

    # Read the checkpoint from the target; create the checkpoint table if absent.
    try:
        with tgt_conn.cursor() as checkpoint_cur:
            last_id = get_checkpoint(checkpoint_cur, name)
    except Exception as e:
        logger.error(
            f"[{name}] Failed to fetch checkpoint, attempting to create table: {e}"
        )
        try:
            tgt_conn.rollback()
            with tgt_conn.cursor() as checkpoint_cur:
                create_checkpoint_table(checkpoint_cur)
                last_id = get_checkpoint(checkpoint_cur, name)
        except Exception as e2:
            logger.error(f"[{name}] Failed to create checkpoint table: {e2}")
            return False

    logger.info(f"--- Starting {name} from ID {last_id} ---")

    raw_buffer = []  # cleaned-but-untransformed rows (batch mode only)
    buffer = []  # transformed target rows ready to insert
    domain_buffer = []  # (domain_table_name, row) pairs for domain inserts
    current_high_id = last_id
    retrieval_columns = None
    rejected_count = 0

    def execute_flush(high_id):
        """Insert buffered rows and advance the checkpoint atomically on the target."""
        try:
            with tgt_conn.cursor() as tgt_cur:
                if buffer:
                    flush_func(tgt_cur, buffer, name, insert_columns)
                if domain_buffer:
                    flush_domain_buffer(tgt_cur, domain_buffer)
                update_checkpoint(tgt_cur, name, high_id)
            tgt_conn.commit()
            logger.info(f"[{name}] Flushed batch. Checkpoint: {high_id}")
        except Exception as e:
            tgt_conn.rollback()
            logger.error(
                f"[{name}] Flush failed at ID {high_id}; transaction rolled back."
            )
            logger.error(f"Error details: {e}")
            raise
        finally:
            buffer.clear()
            domain_buffer.clear()

    def absorb(batch_results):
        for transformed, domain_row in batch_results:
            if transformed:
                buffer.append(transformed)
            if domain_row:
                domain_buffer.append(domain_row)

    try:
        with src_conn.cursor(name=f"{name}_cursor") as src_cur:
            src_cur.itersize = FETCH_SIZE
            src_cur.execute(query, (last_id,))

            for row in src_cur:
                if retrieval_columns is None:
                    retrieval_columns = [desc[0] for desc in src_cur.description]

                current_high_id = row[0]  # checkpoint id, taken from the RAW row

                if clean_func:
                    cleaned, reason = clean_func(row, retrieval_columns)
                    if cleaned is None:
                        rejected_count += 1
                        log_reject(name, current_high_id, reason)
                        continue  # skip transform + insert; checkpoint still advances
                    row = cleaned

                try:
                    if is_batch_mode:
                        raw_buffer.append(row)
                        if len(raw_buffer) >= BATCH_SIZE:
                            absorb(
                                transform_func(tgt_conn, raw_buffer, retrieval_columns)
                            )
                            execute_flush(current_high_id)
                            raw_buffer.clear()
                    else:
                        transformed, domain_row = transform_func(row, retrieval_columns)
                        if transformed:
                            buffer.append(transformed)
                        if domain_row:
                            domain_buffer.append(domain_row)
                        if len(buffer) >= BATCH_SIZE:
                            execute_flush(current_high_id)
                except Exception as trans_err:
                    logger.error(
                        f"[{name}] Transformation error at row {current_high_id}: {trans_err}"
                    )
                    raise

        # Flush any tail rows.
        if is_batch_mode and raw_buffer:
            absorb(transform_func(tgt_conn, raw_buffer, retrieval_columns))

        if buffer or domain_buffer:
            execute_flush(current_high_id)

        if rejected_count:
            logger.warning(
                f"[{name}] Rejected {rejected_count} source row(s); see "
                f"etl_rejected_rows.log."
            )

        return True

    except Exception as main_err:
        logger.critical(f"[{name}] ETL process halted: {main_err}")
        tgt_conn.rollback()
        return False


def build_steps():
    """Return the ordered list of ETL step configs."""
    return [
        {
            "name": "omop_temp.note",
            "query": "SELECT note_id, person_id, note_date, note_datetime, "
            "note_type_concept_id, note_class_concept_id, note_title, note_text, "
            "provider_id, visit_occurrence_id, visit_detail_id, note_source_value "
            "FROM omop_temp.note "
            "WHERE note_id > %s "
            "ORDER BY note_id ASC",
            "transform_func": transform_note,
            "flush_func": generic_flush,
            "is_batch": False,
        },
        {
            "name": "omop_temp.note_nlp",
            "query": """SELECT
                        note_nlp.note_nlp_id,
                        note_nlp.note_id,
                        note_nlp.section_concept_id,
                        note_nlp.snippet,
                        note_nlp."offset",
                        note_nlp.lexical_variant,
                        note_nlp.note_nlp_concept_id,
                        note_nlp.note_nlp_source_concept_id,
                        note_nlp.nlp_system,
                        note_nlp.nlp_date,
                        note_nlp.nlp_datetime,
                        note_nlp.term_exists,
                        note_nlp.term_temporal,
                        note_nlp.term_modifiers,
                        note_nlp.value_as_number,
                        note_nlp.value_as_string,
                        note_nlp.value_as_date,
                        note_nlp.value_as_boolean,
                        note_nlp.value_as_concept,
                        note.person_id
                    FROM omop_temp.note_nlp AS note_nlp
                    INNER JOIN omop_temp.note AS note ON note_nlp.note_id = note.note_id
                    WHERE note_nlp_id > %s
                    ORDER BY note_nlp_id ASC""",
            "transform_func": transform_nlp_batch,
            "flush_func": generic_flush,
            "clean_func": clean_note_nlp_row,
            "insert_columns": [
                "note_nlp_id",
                "note_id",
                "section_concept_id",
                "snippet",
                '"offset"',
                "lexical_variant",
                "note_nlp_concept_id",
                "note_nlp_source_concept_id",
                "nlp_system",
                "nlp_date",
                "nlp_datetime",
                "term_exists",
                "term_temporal",
                "term_modifiers",
            ],
            "is_batch": True,
        },
        {
            "name": "omop_temp.note_nlp_relationship",
            "query": "SELECT nnr.relationship_id, nnr.relationship_concept_id, "
            "nnr.relationship_source_value, nnr.nlp_system "
            "FROM omop_temp.note_nlp_relationship AS nnr "
            "WHERE nnr.relationship_id > %s "
            "ORDER BY nnr.relationship_id ASC",
            "transform_func": transform_nlp_relationship_batch,
            "flush_func": generic_flush,
            "insert_columns": [
                "relationship_id",
                "relationship_concept_id",
                "relationship_source_value",
                "nlp_system",
            ],
            "is_batch": True,
            "create_table": """
                CREATE TABLE IF NOT EXISTS omop_nlp.note_nlp_relationship
                (
                    relationship_id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                    relationship_concept_id integer NOT NULL,
                    relationship_source_value character varying(100) NOT NULL,
                    nlp_system character varying(250) NOT NULL,
                    CONSTRAINT xpk_note_nlp_relationship PRIMARY KEY (relationship_id)
                )
                """,
        },
        {
            "name": "omop_temp.note_nlp_relationship_part",
            "query": "SELECT nnrp.relationship_part_id, nnrp.relationship_id, "
            "nnrp.note_nlp_id, nnrp.role_concept_id, nnrp.role_source_value "
            "FROM omop_temp.note_nlp_relationship_part AS nnrp "
            "WHERE nnrp.relationship_part_id > %s "
            "ORDER BY nnrp.relationship_part_id ASC",
            "transform_func": transform_nlp_relationship_part_batch,
            "flush_func": generic_flush,
            "insert_columns": [
                "relationship_part_id",
                "relationship_id",
                "note_nlp_id",
                "role_concept_id",
                "role_source_value",
            ],
            "is_batch": True,
            "create_table": """
                CREATE TABLE IF NOT EXISTS omop_nlp.note_nlp_relationship_part
                (
                    relationship_part_id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                    relationship_id bigint NOT NULL,
                    note_nlp_id bigint NOT NULL,
                    role_concept_id integer NOT NULL,
                    role_source_value character varying(100) NOT NULL,
                    CONSTRAINT xpk_note_nlp_relationship_part PRIMARY KEY (relationship_part_id),
                    CONSTRAINT xfk_part_to_note_nlp FOREIGN KEY (note_nlp_id)
                        REFERENCES omop_nlp.note_nlp (note_nlp_id) ON DELETE CASCADE,
                    CONSTRAINT xfk_part_to_relationship FOREIGN KEY (relationship_id)
                        REFERENCES omop_nlp.note_nlp_relationship (relationship_id) ON DELETE CASCADE
                )
            """,
        },
    ]


def run_full_etl():
    src_conn, tgt_conn = get_connections()
    try:
        # Seed domain id counters from the target's current MAX before transforming.
        initialize_last_ids(tgt_conn)

        # Steps are ordered by dependency (note -> note_nlp -> relationship ...).
        # If a step aborts, stop: running a downstream step against parents the
        # failed step never inserted would create exactly the orphans we exclude.
        for step in build_steps():
            completed = process_table(src_conn, tgt_conn, step)
            if not completed:
                logger.critical(
                    f"Step '{step['name']}' did not complete; skipping remaining "
                    f"steps to avoid orphaned downstream records."
                )
                break
    finally:
        src_conn.close()
        tgt_conn.close()


def main():
    run_full_etl()


if __name__ == "__main__":
    main()
