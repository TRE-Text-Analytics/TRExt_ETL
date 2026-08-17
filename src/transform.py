"""Transform stage: map source rows into target-schema rows.

Two kinds of output are produced:

  * the target-table row itself (e.g. a note_nlp row trimmed to the target's
    columns), and
  * an optional *domain* row (condition/measurement/procedure/drug/observation)
    derived from an NLP annotation and routed to the matching OMOP domain table.

Target ids for the domain tables are allocated in-process from the current MAX in
each table. ``initialize_last_ids`` must be called once, after connecting, before
any transform runs — this keeps the module import-time-pure (no DB connection at
import) and makes it testable. See the note at the bottom of this file for the
trade-offs versus DB sequences.
"""

from domain_concept_mapping import get_max_ids, get_routing_map_batch
from person_map import lookup_person_id
from rejects import log_reject
from domain_concepts.measurement import unit_types as measurement_unit_types

# OMOP "NLP derived" type-concept ids, one per domain table.
NLP_DERIVED_TYPE = {
    "condition_occurrence": "32424",
    "measurement": "32423",
    "procedure_occurrence": "32425",
    "drug_exposure": "32426",
    "observation": "32445",
}

VALID_DOMAINS = {"Condition", "Measurement", "Procedure", "Drug", "Observation"}

# Target note_nlp columns, in order. This list is the single source of truth for
# which retrieved columns are "compatible": anything the source SELECT adds beyond
# these (e.g. value_as_* or the joined person_id) is trimmed before insert.
TARGET_NOTE_NLP_COLUMNS = [
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
]
TARGET_NOTE_NLP_COL_COUNT = len(TARGET_NOTE_NLP_COLUMNS)

# In-process id counters, seeded by initialize_last_ids().
last_ids = {}


def initialize_last_ids(conn):
    """Seed the domain id counters from the current MAX in each target table.

    Call once, after connecting, before processing any rows. On a resumed run this
    re-reads the committed MAX, so ids continue without collision or large gaps.
    """
    global last_ids
    max_ids = get_max_ids(conn)
    last_ids = {table: max_ids[table] for table in NLP_DERIVED_TYPE}


def _next_id(table):
    last_ids[table] += 1
    return last_ids[table]


def transform_note(row, columns):
    """Map a source note row to the target note schema.

    Returns ``(row, None)``, or ``(None, None)`` if the note's person can't be
    mapped (in which case the note is logged as rejected and skipped).
    """
    new_person_id = lookup_person_id(row[1])
    if new_person_id is None:
        log_reject("omop_temp.note", row[0], "unmapped person")
        return None, None

    new_row = (
        row[0],  # note_id
        new_person_id,  # person_id (mapped)
        row[2],  # note_date
        row[3],  # note_datetime
        row[4],  # note_type_concept_id
        row[5],  # note_class_concept_id
        row[6],  # note_title
        row[7],  # note_text
        32678,  # encoding_concept_id  -> "UTF-8"
        4180186,  # language_concept_id  -> "English"
        row[8],  # provider_id
        row[9],  # visit_occurrence_id
        row[10],  # visit_detail_id
        row[11],  # note_source_value
        None,  # note_event_id
        None,  # note_event_field_concept_id
    )
    return new_row, None


def _build_measurement_row(nlp_row, person_id, concept_id, date):
    """Parse value/unit out of term_modifiers ("value=5|unit=mg") into a measurement row.

    term_modifiers may be NULL, malformed, or missing the unit segment; every parse
    failure degrades to ``None`` for that field rather than dropping the row.
    """
    modifiers = nlp_row[13]  # term_modifiers

    try:
        raw_value = modifiers.split("|")[0].replace("value=", "")
        value_as_number = (
            float(raw_value) if raw_value and raw_value != "None" else None
        )
    except (AttributeError, IndexError, ValueError):
        value_as_number = None

    measurement_source_value = (
        str(value_as_number) if value_as_number is not None else None
    )

    try:
        unit_key = modifiers.split("|")[1].replace("unit=", "").strip()
        unit_concept_id = measurement_unit_types.get(unit_key)
    except (AttributeError, IndexError):
        unit_concept_id = None

    row = (
        _next_id("measurement"),
        person_id,
        concept_id,
        date,
        NLP_DERIVED_TYPE["measurement"],
        value_as_number,
        measurement_source_value,
        unit_concept_id,
    )
    return "measurement", row


def get_domain_row(domain, nlp_row):
    """Build a domain occurrence row from a mapped note_nlp row.

    Returns ``(table_name, row)``, or ``None`` to skip the row (logging why).
    ``nlp_row[0]`` is note_nlp_id (used for traceability); ``nlp_row[-1]`` is the
    joined note.person_id; ``nlp_row[6]`` is the standard concept id set by the
    caller; ``nlp_row[9]`` is nlp_date.
    """
    note_nlp_id = nlp_row[0]
    person_id = lookup_person_id(nlp_row[-1])
    if person_id is None:
        log_reject(
            "omop_temp.note_nlp",
            note_nlp_id,
            f"unmapped person for {domain} domain row",
        )
        return None

    concept_id = nlp_row[6]
    date = nlp_row[9]

    if domain == "Condition":
        return "condition_occurrence", (
            _next_id("condition_occurrence"),
            person_id,
            concept_id,
            date,
            NLP_DERIVED_TYPE["condition_occurrence"],
        )

    if domain == "Measurement":
        return _build_measurement_row(nlp_row, person_id, concept_id, date)

    if domain == "Procedure":
        return "procedure_occurrence", (
            _next_id("procedure_occurrence"),
            person_id,
            concept_id,
            date,
            NLP_DERIVED_TYPE["procedure_occurrence"],
        )

    if domain == "Drug":
        return "drug_exposure", (
            _next_id("drug_exposure"),
            person_id,
            concept_id,
            date,
            date,  # end date = start date
            NLP_DERIVED_TYPE["drug_exposure"],
        )

    if domain == "Observation":
        return "observation", (
            _next_id("observation"),
            person_id,
            concept_id,
            date,
            NLP_DERIVED_TYPE["observation"],
        )

    return None


def transform_nlp_batch(conn, rows, columns):
    """Transform a batch of note_nlp rows, routing each to its OMOP domain table.

    Returns a list of ``(note_nlp_row, domain_row_or_None)`` pairs.
    """
    if not rows:
        return []

    # note_nlp_source_concept_id is at index 7; None values were sentinel-cleaned.
    unique_snomed_codes = {str(row[7]) for row in rows if row[7]}
    domain_lookup_map = get_routing_map_batch(conn, unique_snomed_codes)

    results = []
    for row in rows:
        row_list = list(row)

        # Linkage guard. note_nlp references a note (note_id) and, through it, a
        # person. transform_note drops any note whose person can't be mapped, so a
        # note_nlp row for that same note would be left pointing at a note that was
        # never inserted (an orphan). The joined note.person_id is the last column,
        # and it is the SAME source id transform_note keys on, so this fires exactly
        # when the parent note was dropped. Skip the row entirely — no note_nlp row
        # and no derived domain row.
        #
        # NB: this mirrors transform_note's drop condition. If a note is ever
        # dropped for an additional reason (e.g. a future clean_func on the note
        # step), that condition must be reflected here too, or orphans return.
        if lookup_person_id(row_list[-1]) is None:
            log_reject(
                "omop_temp.note_nlp",
                row_list[0],
                "skipped: parent note dropped (unmapped person)",
            )
            continue

        snomed_code = str(row_list[7])
        domain_row = None

        domain_lookup = domain_lookup_map.get(snomed_code)
        if domain_lookup and domain_lookup["domain_id"] in VALID_DOMAINS:
            row_list[6] = domain_lookup["standard_concept_id"]
            # get_domain_row needs the full (untrimmed) row; it returns None to skip.
            domain_row = get_domain_row(domain_lookup["domain_id"], row_list)

        # Trim to the columns the target note_nlp table actually has.
        row_tuple = tuple(row_list[:TARGET_NOTE_NLP_COL_COUNT])
        results.append((row_tuple, domain_row))

    return results


def transform_nlp_relationship_batch(conn, rows, columns):
    """Pass-through transform for note_nlp_relationship rows (no remapping needed)."""
    if not rows:
        return []
    return [(tuple(row), None) for row in rows]


def transform_nlp_relationship_part_batch(conn, rows, columns):
    """Pass-through transform for note_nlp_relationship_part rows (no remapping needed)."""
    if not rows:
        return []
    return [(tuple(row), None) for row in rows]
