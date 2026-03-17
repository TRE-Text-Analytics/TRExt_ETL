import pandas as pd
import sys
import os

def update_ethnicity_concept_id(input_file, output_file=None):
    # Generate output filename if not provided
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_updated{ext}"

    # Read the TSV file
    df = pd.read_csv(input_file, sep='\t')

    if 'ethnicity_concept_id' not in df.columns:
        print(f"Error: Column 'ethnicity_concept_id' not found.")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    # Update the column
    original_count = df['ethnicity_concept_id'].nunique()
    df['ethnicity_concept_id'] = 759814

    # Save to new file
    df.to_csv(output_file, sep='\t', index=False)

    print(f"Done! Updated {len(df)} rows (was {original_count} unique value(s)).")
    print(f"Saved to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_ethnicity.py <input.tsv> [output.tsv]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    update_ethnicity_concept_id(input_file, output_file)