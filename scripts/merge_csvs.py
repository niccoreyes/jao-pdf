import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, List


def natural_key(p: Path) -> int:
    """
    Sort by trailing number in filenames like 'chunk_12_table_3.csv'.
    Falls back to 0 when not found.
    """
    m = re.search(r"(\d+)(?=\.csv$)", p.name)
    return int(m.group(1)) if m else 0


def iter_csv_rows(csv_path: Path) -> Iterable[List[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            yield [cell for cell in row]


def is_empty_row(row: List[str]) -> bool:
    return all((str(c).strip() == "" for c in row))


def pad_row(row: List[str], width: int) -> List[str]:
    if len(row) < width:
        return row + [""] * (width - len(row))
    return row[:width]


def merge_csvs(
    csv_dir: Path,
    out_path: Path,
    add_source_column: bool = True,
    drop_empty_rows: bool = True,
) -> int:
    """
    Merge all CSV files in csv_dir into a single CSV.
    - Normalizes rows to the max column count across all files.
    - Optionally adds a 'source_file' column as the first column.
    - Optionally drops rows that are entirely empty (after trimming).
    Returns: number of rows written (excluding header if none is used).
    """
    csv_files = sorted((p for p in csv_dir.glob("*.csv")), key=natural_key)
    if not csv_files:
        print(f"[info] No CSVs found in: {csv_dir}")
        return 0

    # Determine max width across all CSVs
    max_cols = 0
    for csv_file in csv_files:
        for row in iter_csv_rows(csv_file):
            max_cols = max(max_cols, len(row))

    print(f"[plan] {len(csv_files)} CSV(s) -> merging to {out_path} with width={max_cols}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    wrote = 0
    with out_path.open("w", encoding="utf-8-sig", newline="") as f_out:
        writer = csv.writer(f_out)

        for csv_file in csv_files:
            for row in iter_csv_rows(csv_file):
                if drop_empty_rows and is_empty_row(row):
                    continue
                normalized = pad_row(row, max_cols)
                if add_source_column:
                    writer.writerow([csv_file.name] + normalized)
                else:
                    writer.writerow(normalized)
                wrote += 1

    print(f"[done] Wrote {wrote} merged row(s) to {out_path}")
    return wrote


def main() -> None:
    """
    Merge utility for CSVs produced by analyze_chunks_to_csv.py.

    Examples (Windows):
      # Merge default folder 'csv_chunks' into 'merged.csv'
      python scripts\merge_csvs.py

      # Custom input/output, keep empty rows, do not add source col
      python scripts\merge_csvs.py --csv-dir csv_chunks --out merged.csv --no-add-source --keep-empty
    """
    parser = argparse.ArgumentParser(description="Merge per-chunk CSVs into a single CSV.")
    parser.add_argument("--csv-dir", default="csv_chunks", help="Directory containing input CSV files.")
    parser.add_argument("--out", default="merged.csv", help="Output CSV filename.")
    parser.add_argument("--no-add-source", action="store_true", help="Do not add a 'source_file' column.")
    parser.add_argument("--keep-empty", action="store_true", help="Keep rows that are entirely empty.")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    out_path = Path(args.out)

    merge_csvs(
        csv_dir=csv_dir,
        out_path=out_path,
        add_source_column=not args.no_add_source,
        drop_empty_rows=not args.keep_empty,
    )


if __name__ == "__main__":
    main()
