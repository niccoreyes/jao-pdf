import argparse
import csv
import os
import re
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential


def natural_chunk_sort_key(p: Path) -> int:
    """
    Extract numeric suffix from filenames like 'chunk_12.pdf' for proper sorting.
    Falls back to 0 if no match.
    """
    m = re.search(r"chunk_(\d+)\.pdf$", p.name, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def build_grid_from_table(table) -> List[List[str]]:
    """
    Construct a dense 2D grid (rows x cols) from the Azure table cells.
    Cells may have row_span/column_span; we ignore spans and place content at (row_index, column_index).
    """
    row_count = table.row_count or 0
    col_count = table.column_count or 0
    grid: List[List[str]] = [[""] * col_count for _ in range(row_count)]
    for cell in table.cells:
        r = getattr(cell, "row_index", None)
        c = getattr(cell, "column_index", None)
        content = getattr(cell, "content", "") or ""
        if r is not None and c is not None and 0 <= r < row_count and 0 <= c < col_count:
            grid[r][c] = content.strip()
    return grid


def write_csv(path: Path, rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig BOM helps Excel auto-detect UTF-8
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def analyze_pdf_to_csvs(
    client: DocumentIntelligenceClient,
    pdf_path: Path,
    output_dir: Path,
    model_id: str = "prebuilt-layout",
) -> int:
    """
    Analyze a single PDF and emit one CSV per detected table.
    Returns number of tables extracted.
    """
    with pdf_path.open("rb") as f:
        request = AnalyzeDocumentRequest(bytes_source=f.read())

    poller = client.begin_analyze_document(model_id=model_id, body=request)
    result = poller.result()

    total_tables = 0
    for i, table in enumerate(result.tables or [], start=1):
        grid = build_grid_from_table(table)
        csv_name = f"{pdf_path.stem}_table_{i}.csv"
        out_csv = output_dir / csv_name
        write_csv(out_csv, grid)
        print(f"[csv] {out_csv} ({table.row_count}x{table.column_count})")
        total_tables += 1

    if total_tables == 0:
        print("[warn] No tables detected in this chunk.")
    return total_tables


def main() -> None:
    """
    Analyze all chunk_*.pdf files and export detected tables to CSV.

    Prereqs (Windows):
      - Activate venv first:
          PowerShell: .\\.venv\\Scripts\\Activate.ps1
          CMD:        .venv\\Scripts\\activate
      - Ensure .env has:
          DOCUMENTINTELLIGENCE_ENDPOINT=...
          DOCUMENTINTELLIGENCE_API_KEY=...

    Examples:
      python scripts\analyze_chunks_to_csv.py
      python scripts\analyze_chunks_to_csv.py --chunks-dir pdf_chunks --out-dir csv_chunks --delay 1
      python scripts\analyze_chunks_to_csv.py --start-from 21 --max-chunks 10
    """
    parser = argparse.ArgumentParser(description="Analyze PDF chunks with Azure Document Intelligence and export tables to CSV.")
    parser.add_argument("--chunks-dir", default="pdf_chunks", help="Directory containing chunk_*.pdf files.")
    parser.add_argument("--out-dir", default="csv_chunks", help="Directory to write CSV files.")
    parser.add_argument("--model-id", default="prebuilt-layout", help="Azure DI model ID (default: prebuilt-layout).")
    parser.add_argument("--start-from", type=int, default=1, help="Start from chunk number N (default: 1).")
    parser.add_argument("--max-chunks", type=int, default=0, help="Process at most K chunks (0 = no limit).")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay (seconds) between requests to respect rate limits.")
    args = parser.parse_args()

    # Load .env for endpoint and api key
    load_dotenv()
    endpoint = os.getenv("DOCUMENTINTELLIGENCE_ENDPOINT")
    api_key = os.getenv("DOCUMENTINTELLIGENCE_API_KEY")

    if not endpoint or not api_key:
        raise RuntimeError(
            "Missing Azure credentials. Ensure .env contains DOCUMENTINTELLIGENCE_ENDPOINT and DOCUMENTINTELLIGENCE_API_KEY."
        )

    client = DocumentIntelligenceClient(endpoint, AzureKeyCredential(api_key))

    chunks_dir = Path(args.chunks_dir)
    out_dir = Path(args.out_dir)

    if not chunks_dir.exists():
        raise FileNotFoundError(f"Chunks directory not found: {chunks_dir}")

    chunk_files = sorted((p for p in chunks_dir.glob("chunk_*.pdf")), key=natural_chunk_sort_key)
    # Filter by start-from chunk number
    chunk_files = [p for p in chunk_files if natural_chunk_sort_key(p) >= int(args.start_from)]

    if not chunk_files:
        print(f"[info] No chunks found in {chunks_dir} (start-from={args.start_from})")
        return

    if args.max_chunks and args.max_chunks > 0:
        chunk_files = chunk_files[: int(args.max_chunks)]

    print(f"[plan] Processing {len(chunk_files)} chunk(s) from: {chunks_dir}")
    print(f"[out]  CSV output directory: {out_dir}\n")

    grand_total = 0
    for idx, pdf_path in enumerate(chunk_files, start=1):
        print(f"[chunk] {idx}/{len(chunk_files)} -> {pdf_path.name}")
        try:
            total_tables = analyze_pdf_to_csvs(
                client=client,
                pdf_path=pdf_path,
                output_dir=out_dir,
                model_id=args.model_id,
            )
            grand_total += total_tables
        except Exception as ex:
            print(f"[error] Failed to process {pdf_path.name}: {ex}")
        time.sleep(max(0.0, float(args.delay)))

    print(f"\n[done] Extracted {grand_total} table(s) from {len(chunk_files)} chunk(s).")


if __name__ == "__main__":
    main()
