import pandas as pd
import psycopg
from dotenv import load_dotenv
import os
import sys
import io

load_dotenv('config.env')
TARGET_URL = os.getenv('TARGET_DB_URL')

def import_tsv_to_person(tsv_file):
    print(f"Reading {tsv_file}...")
    df = pd.read_csv(tsv_file, sep='\t', dtype=str)  # Read everything as string from the start
    print(f"  {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")
 
    print("Connecting to PostgreSQL...")
    with psycopg.connect(TARGET_URL) as conn:
        with conn.cursor() as cursor:
 
            # Fetch the actual columns in omop_nlp.person
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'omop' AND table_name = 'person'
                ORDER BY ordinal_position;
            """)
            db_columns = [row[0] for row in cursor.fetchall()]
 
            # Only keep TSV columns that exist in the DB table
            matched_columns = [col for col in df.columns if col in db_columns]
            missing_in_db   = [col for col in df.columns if col not in db_columns]
            missing_in_tsv  = [col for col in db_columns if col not in df.columns]
 
            if not matched_columns:
                print("Error: No TSV columns match any columns in omop_nlp.person. Aborting.")
                sys.exit(1)
 
            if missing_in_db:
                print(f"  Warning: These TSV columns don't exist in omop_nlp.person and will be skipped: {missing_in_db}")
            if missing_in_tsv:
                print(f"  Note: These omop_nlp.person columns are not in the TSV (DB defaults will apply): {missing_in_tsv}")
 
            # Use COPY for bulk insert — sends raw text, Postgres handles casting
            df_to_insert = df[matched_columns]
            col_list = ", ".join(matched_columns)
 
            # Write dataframe to an in-memory TSV buffer
            buffer = io.StringIO()
            df_to_insert.to_csv(buffer, sep='\t', index=False, header=False, na_rep='\\N')
            buffer.seek(0)
 
            print(f"Inserting {len(df_to_insert)} rows into omop_nlp.person via COPY...")
            with cursor.copy(f"COPY omop_nlp.person ({col_list}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')") as copy:
                copy.write(buffer.read())
 
            conn.commit()
 
    print(f"Done! {len(df_to_insert)} rows successfully appended to omop_nlp.person.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_to_omop_nlp.py <file.tsv>")
        sys.exit(1)

    import_tsv_to_person(sys.argv[1])