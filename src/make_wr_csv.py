import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / "config.env")

SOURCE_URL = os.getenv("SOURCE_DB_URL")


def sql_query_to_csv(query, output_file):
    try:
        with psycopg.connect(SOURCE_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

                # Fetch column names from the cursor description.
                colnames = [desc[0] for desc in cursor.description]

                with open(output_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f, delimiter=",")
                    writer.writerow(colnames)
                    writer.writerows(cursor.fetchall())

        print(f"Success! Data exported to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")


def dump_person():
    """Generate a dump of person data for only essential fields."""
    sql_query_to_csv(
        """
        SELECT DISTINCT ON (ad.subject_id)
            ad.subject_id AS person_id,
            ad.race,
            p.gender,
            (MAKE_TIMESTAMP(p.anchor_year, 1, 1, 0, 0, 0) - INTERVAL '200 years') AS anchor_year
        FROM mimiciv_hosp.admissions AS ad
        INNER JOIN omop_temp.note AS n ON n.person_id = ad.subject_id
        INNER JOIN mimiciv_hosp.patients AS p ON p.subject_id = ad.subject_id;
        """,
        "person.csv",
    )


def dump_note():
    sql_query_to_csv(
        """
        SELECT note_id, person_id, note_date, note_datetime, note_type_concept_id,
               note_class_concept_id, note_title, encoding_concept_id,
               language_concept_id, visit_occurrence_id
        FROM omop_temp.note
        LIMIT 100000;
        """,
        "note.csv",
    )


def dump_note_nlp():
    sql_query_to_csv(
        """
        SELECT *
        FROM omop_temp.note_nlp
        LIMIT 100000;
        """,
        "note_nlp.csv",
    )


if __name__ == "__main__":
    dump_person()
    # dump_note()
    # dump_note_nlp()