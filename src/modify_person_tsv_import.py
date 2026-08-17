"""
Populate the `person_source_value` column in person.tsv using the
SOURCE_SUBJECT -> TARGET_SUBJECT mapping in person_ids.tsv.

person.tsv already stores the TARGET_SUBJECT id in its `person_id` column.
For each row we look that id up in the mapping and write back the original
SOURCE_SUBJECT value into `person_source_value`.

Run standalone:
    python populate_person_source_value.py
    python populate_person_source_value.py --person output/person.tsv \
        --mapping output/person_ids.tsv
"""

import argparse
import csv
import os
import sys
import tempfile

# Let long/wide id fields through without the default 128 KB field-size cap.
csv.field_size_limit(sys.maxsize)


def build_mapping(mapping_path):
    """Return a dict of TARGET_SUBJECT -> SOURCE_SUBJECT from the mapping log."""
    mapping = {}
    duplicates = 0

    with open(mapping_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        _require_columns(
            reader.fieldnames, ["SOURCE_SUBJECT", "TARGET_SUBJECT"], mapping_path
        )

        for row in reader:
            target = row["TARGET_SUBJECT"]
            source = row["SOURCE_SUBJECT"]
            if target in mapping and mapping[target] != source:
                duplicates += 1
                print(
                    f"  warning: TARGET_SUBJECT {target!r} maps to multiple "
                    f"SOURCE_SUBJECT values; keeping the last one seen ({source!r})",
                    file=sys.stderr,
                )
            mapping[target] = source

    if duplicates:
        print(
            f"  note: {duplicates} duplicate target id(s) in the mapping.",
            file=sys.stderr,
        )
    return mapping


def _require_columns(fieldnames, required, path):
    fieldnames = fieldnames or []
    missing = [c for c in required if c not in fieldnames]
    if missing:
        sys.exit(f"Error: {path} is missing required column(s): {', '.join(missing)}")


def update_person_file(person_path, mapping):
    """Rewrite person.tsv with person_source_value backfilled from the mapping."""
    dir_name = os.path.dirname(os.path.abspath(person_path))
    matched = 0
    unmatched = 0

    with open(person_path, newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in, delimiter="\t")
        _require_columns(
            reader.fieldnames,
            ["person_id", "person_source_value"],
            person_path,
        )
        fieldnames = reader.fieldnames

        # Write to a temp file in the same directory, then atomically replace.
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(
                    f_out,
                    fieldnames=fieldnames,
                    delimiter="\t",
                    lineterminator="\n",
                )
                writer.writeheader()

                for row in reader:
                    person_id = row["person_id"]
                    if person_id in mapping:
                        row["person_source_value"] = mapping[person_id]
                        matched += 1
                    else:
                        unmatched += 1
                        print(
                            f"  warning: no mapping for person_id {person_id!r}; "
                            f"person_source_value left unchanged",
                            file=sys.stderr,
                        )
                    writer.writerow(row)

            os.replace(tmp_path, person_path)
        except BaseException:
            # Don't leave a stray temp file behind on failure.
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    return matched, unmatched


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--person",
        default="output/person.tsv",
        help="Path to person.tsv (modified in place).",
    )
    parser.add_argument(
        "--mapping",
        default="output/person_ids.tsv",
        help="Path to person_ids.tsv (read only).",
    )
    args = parser.parse_args()

    for path in (args.person, args.mapping):
        if not os.path.isfile(path):
            sys.exit(f"Error: file not found: {path}")

    print(f"Reading mapping from {args.mapping} ...")
    mapping = build_mapping(args.mapping)
    print(f"  loaded {len(mapping)} id mapping(s).")

    print(f"Updating {args.person} ...")
    matched, unmatched = update_person_file(args.person, mapping)

    print("Done.")
    print(f"  rows matched:   {matched}")
    print(f"  rows unmatched: {unmatched}")


if __name__ == "__main__":
    main()
