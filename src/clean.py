"""Row-level cleaning for the ETL clean stage (retrieve -> CLEAN -> transform -> insert).

A cleaner does two jobs on a freshly retrieved source row:

  1. Normalises sentinel strings ('None', 'NULL', '', ...) to real ``None`` in a
     declared set of *structured* columns (ids, concept ids, dates). Free-text
     columns are deliberately never touched, since clinical text can legitimately
     contain words like "None".
  2. Rejects the row outright if any declared *mandatory* column is missing.

Cleaners follow a simple contract so ``process_table`` can treat them uniformly:

    clean(row, columns) -> (cleaned_tuple, None)   # keep the row
                        -> (None, reason)          # reject the row

The runner logs rejections centrally (see ``rejects.log_reject``); cleaners only
decide and explain, they don't log.
"""

SENTINELS = {"none", "null", "nan", "n/a", ""}


def is_missing(value):
    """True if ``value`` is ``None`` or a sentinel string standing in for absence."""
    return value is None or (
        isinstance(value, str) and value.strip().lower() in SENTINELS
    )


def make_row_cleaner(coercible_columns, required_columns=()):
    """Build a ``clean_func`` for a step.

    Args:
        coercible_columns: columns where sentinel strings should become ``None``.
        required_columns: columns that must be present, or the row is rejected.

    Returns:
        A ``clean(row, columns)`` callable following the clean-stage contract.
    """
    coercible = set(coercible_columns)
    required = tuple(required_columns)

    def clean(row, columns):
        row = list(row)

        for i, col in enumerate(columns):
            if col in coercible and isinstance(row[i], str):
                if row[i].strip().lower() in SENTINELS:
                    row[i] = None

        col_index = {col: i for i, col in enumerate(columns)}
        for col in required:
            if col in col_index and is_missing(row[col_index[col]]):
                return None, f"missing required field '{col}'"

        return tuple(row), None

    return clean


# Cleaner for the note_nlp step. ``person_id`` arrives via the join to note and is
# treated as mandatory because every derived domain row needs a linkable person.
clean_note_nlp_row = make_row_cleaner(
    coercible_columns={
        "note_nlp_id",
        "note_id",
        "section_concept_id",
        "note_nlp_concept_id",
        "note_nlp_source_concept_id",
        "nlp_date",
        "nlp_datetime",
        "person_id",
    },
    required_columns=("note_id", "person_id", "nlp_date"),
)
