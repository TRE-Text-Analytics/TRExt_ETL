#!/usr/bin/env python3
"""
Update person.person_source_value in a PostgreSQL database from a CSV file.

CSV format (no header required, but supported):
    person_id,target_id
    <new person_source_value>,<person.person_id to match>

Usage:
    python update_person_source_value.py --csv data.csv \
        --host localhost --port 5432 --dbname mydb \
        --user myuser --password mypassword

    # Or use a connection string:
    python update_person_source_value.py --csv data.csv \
        --dsn "postgresql://myuser:mypassword@localhost:5432/mydb"
"""

import argparse
import csv
import sys
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit(
        "ERROR: psycopg2 is not installed.\n"
        "Install it with:  pip install psycopg2-binary"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Update person.person_source_value from a CSV file."
    )

    parser.add_argument("--csv", required=True, metavar="FILE",
                        help="Path to the input CSV file.")

    parser.add_argument("--has-header", action="store_true", default=False,
                        help="Pass this flag if the CSV has a header row to skip.")

    # Connection — either a DSN or individual params
    conn_group = parser.add_mutually_exclusive_group(required=True)
    conn_group.add_argument("--dsn", metavar="DSN",
                            help="Full PostgreSQL DSN, e.g. postgresql://user:pass@host/db")
    conn_group.add_argument("--host", metavar="HOST",
                            help="Database host (use with --dbname, --user, etc.)")

    parser.add_argument("--port",     default=5432,  type=int, metavar="PORT")
    parser.add_argument("--dbname",   metavar="DBNAME")
    parser.add_argument("--user",     metavar="USER")
    parser.add_argument("--password", metavar="PASSWORD", default="")
    parser.add_argument("--schema",   metavar="SCHEMA", default="public",
                        help="Schema that contains the person table (default: public).")

    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Parse the CSV and print updates without touching the database.")

    return parser.parse_args()


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_csv(path: str, has_header: bool) -> list[tuple[str, str]]:
    """
    Returns a list of (person_source_value, person_id) tuples.
    Column order in the CSV: source_subject (source value), target_subject (db person_id).
    """
    rows = []
    csv_path = Path(path)

    if not csv_path.exists():
        sys.exit(f"ERROR: CSV file not found: {path}")

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")

        if has_header:
            next(reader, None)  # skip header

        for line_no, row in enumerate(reader, start=2 if has_header else 1):
            if not row:
                continue  # skip blank lines

            if len(row) < 2:
                print(f"  WARNING: line {line_no} has fewer than 2 columns — skipped: {row}")
                continue

            source_value = row[0].strip()
            target_id    = row[1].strip()

            if not source_value or not target_id:
                print(f"  WARNING: line {line_no} has empty values — skipped: {row}")
                continue

            rows.append((source_value, target_id))

    return rows


# ---------------------------------------------------------------------------
# Database update
# ---------------------------------------------------------------------------

def build_connection(args) -> "psycopg2.connection":
    if args.dsn:
        return psycopg2.connect(args.dsn)

    if not args.dbname:
        sys.exit("ERROR: --dbname is required when not using --dsn.")

    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


UPDATE_SQL = """
    UPDATE {schema}.person
       SET person_source_value = %s
     WHERE person_id = %s
"""


def run_updates(conn, rows: list[tuple[str, str]], schema: str) -> None:
    sql = UPDATE_SQL.format(schema=schema)
    updated = 0
    not_found = 0

    with conn:                          # transaction
        with conn.cursor() as cur:
            for source_value, target_id in rows:
                cur.execute(sql, (source_value, target_id))
                if cur.rowcount == 0:
                    print(f"  WARNING: person_id={target_id!r} not found — no row updated.")
                    not_found += 1
                else:
                    updated += cur.rowcount

    print(f"\nDone. {updated} row(s) updated, {not_found} person_id(s) not found.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    print(f"Loading CSV: {args.csv}")
    rows = load_csv(args.csv, args.has_header)
    print(f"  {len(rows)} valid row(s) found.")

    if not rows:
        print("Nothing to do.")
        return

    if args.dry_run:
        print("\n--- DRY RUN (no database changes) ---")
        print(f"{'person_source_value':<30} {'person_id (target)'}")
        print("-" * 55)
        for source_value, target_id in rows:
            print(f"{source_value:<30} {target_id}")
        return

    print(f"\nConnecting to database …")
    try:
        conn = build_connection(args)
    except psycopg2.OperationalError as exc:
        sys.exit(f"ERROR: Could not connect to database:\n  {exc}")

    print(f"Connected. Running updates on {args.schema}.person …")
    try:
        run_updates(conn, rows, args.schema)
    finally:
        conn.close()


if __name__ == "__main__":
    main()