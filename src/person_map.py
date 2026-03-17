import numpy as np
from pathlib import Path
ROOT = Path(__file__).parent.parent

def load_person_id_map(file_path):
    # Load columns directly into specialized 64-bit integer arrays
    data = np.genfromtxt(file_path, delimiter='\t', skip_header=1, dtype=np.int64)
    
    # Sort by the 'SOURCE_SUBJECT' column for binary search lookups
    data = data[data[:, 0].argsort()]
    # return source, target
    return data[:, 0], data[:, 1]


SRC_PEOPLE, TGT_PEOPLE = load_person_id_map(ROOT / "output" / "person_ids.tsv")


def lookup_person_id(source_id):
    # Find the index where target_id would be inserted to maintain order
    idx = np.searchsorted(SRC_PEOPLE, source_id)
    # Check if the ID at that index is actually our target
    if idx < len(SRC_PEOPLE) and SRC_PEOPLE[idx] == source_id:
        return TGT_PEOPLE[idx]
    return None