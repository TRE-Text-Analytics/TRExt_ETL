"""Centralised logging of source rows rejected during ETL.

Every row dropped before insertion — whether for a missing mandatory field in the
clean stage or an unresolvable person mapping in the transform stage — is recorded
here with enough information to trace it back to its source table by id. The
resulting file (``etl_rejected_rows.log``) is intended to be handed to whoever
owns the upstream data-generation step.

By design we log the *source table, source id and reason* only, not the full row.
That keeps the handoff file free of clinical free-text (PHI) while still letting
the upstream owner re-query the offending rows by id. If you need the full row in
the file, pass it through ``reason`` deliberately at the call site.
"""

import logging

REJECT_LOG_FILENAME = "etl_rejected_rows.log"

_logger = logging.getLogger("ETL_Rejects")
_logger.setLevel(logging.INFO)
_handler = logging.FileHandler(REJECT_LOG_FILENAME)
_handler.setFormatter(logging.Formatter("%(asctime)s\t%(message)s"))
_logger.addHandler(_handler)
_logger.propagate = False  # keep reject records out of the main error log


def log_reject(source_table, source_id, reason):
    """Record a single rejected source row.

    Args:
        source_table: source table the row came from (e.g. ``omop_temp.note_nlp``).
        source_id: source primary key, so the row can be re-queried upstream.
        reason: short human-readable explanation of why the row was dropped.
    """
    _logger.info(f"{source_table}\t{source_id}\t{reason}")
