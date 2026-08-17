"""Source-to-target person id mapping, backed by a sorted numpy array.

The map is loaded once at import from ``output/person_ids.tsv`` and queried with a
binary search, which is fast enough to call per row without a DB round-trip.
"""

import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent.parent


def load_person_id_map(file_path):
    """Load the (source, target) person id columns as sorted 64-bit int arrays."""
    data = np.genfromtxt(file_path, delimiter="\t", skip_header=1, dtype=np.int64)
    # Sort by the source id so lookups can use binary search.
    data = data[data[:, 0].argsort()]
    return data[:, 0], data[:, 1]


SRC_PEOPLE, TGT_PEOPLE = load_person_id_map(ROOT / "output" / "person_ids.tsv")


def lookup_person_id(source_id):
    """Return the mapped target person id, or ``None`` if the source id is unknown.

    Returns a plain ``int`` (not ``numpy.int64``) so psycopg can adapt it directly
    when it lands in an INSERT.
    """
    if source_id is None:
        return None

    idx = np.searchsorted(SRC_PEOPLE, source_id)
    if idx < len(SRC_PEOPLE) and SRC_PEOPLE[idx] == source_id:
        return int(TGT_PEOPLE[idx])
    return None
