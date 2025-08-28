import argparse
import os
import shutil
from pathlib import Path
from typing import List, Tuple

from PyPDF2 import PdfReader, PdfWriter


def generate_pdf_chunks(
    pdf_path: Path,
    chunk_folder: Path,
    legacy_folder: Path,
    pages_per_chunk: int = 2,
    max_chunk_size_bytes: int = 4 * 1024 * 1024,
) -> Tuple[List[Path], List[Path]]:
    """
    Split a PDF into N-page chunks (default 2). If a chunk exceeds max size,
    move it to 'legacy_folder' and create single-page subchunks instead.

    Returns:
      - chunks: list of chunk file paths created in chunk_folder
      - legacy_chunks: list of oversized original chunk file paths moved to legacy_folder
    """
    chunk_folder.mkdir(parents=True, exist_ok=True)
    legacy_folder.mkdir(parents=True, exist_ok=True)

    chunks: List[Path] = []
    legacy_chunks: List[Path] = []

    with pdf_path.open("rb") as f:
        reader = PdfReader(f)
        total_pages = len(reader.pages)

        current_pages = []
        chunk_counter = 1

        for page_num in range(total_pages):
            current_pages.append(reader.pages[page_num])

            # Emit a chunk when reaching pages_per_chunk or end of document
            if len(current_pages) == pages_per_chunk or page_num == total_pages - 1:
                chunk_path = chunk_folder / f"chunk_{chunk_counter}.pdf"

                # Write the tentative chunk
                with PdfWriter() as writer:
                    for p in current_pages:
                        writer.add_page(p)
                    with chunk_path.open("wb") as out_f:
                        writer.write(out_f)

                chunk_size = chunk_path.stat().st_size

                if chunk_size > max_chunk_size_bytes:
                    # Move the oversized chunk to legacy and split into single pages
                    legacy_path = legacy_folder / f"chunk_{chunk_counter}.pdf"
                    shutil.move(str(chunk_path), str(legacy_path))
                    legacy_chunks.append(legacy_path)

                    # Single-page subchunks replacing this oversized one
                    for i, page in enumerate(current_pages, start=0):
                        subchunk_num = chunk_counter + i
                        subchunk_path = chunk_folder / f"chunk_{subchunk_num}.pdf"
                        with PdfWriter() as sub_writer:
                            sub_writer.add_page(page)
                            with subchunk_path.open("wb") as sub_f:
                                sub_writer.write(sub_f)
                        print(
                            f"[subchunk] Created {subchunk_path.name} "
                            f"(1 page, {subchunk_path.stat().st_size/1024:.1f} KB)"
                        )
                        chunks.append(subchunk_path)

                    # Advance counter beyond the number of pages we just split
                    chunk_counter += len(current_pages)
                else:
                    print(
                        f"[chunk] Created {chunk_path.name} "
                        f"({len(current_pages)} pages, {chunk_size/1024:.1f} KB)"
                    )
                    chunks.append(chunk_path)
                    chunk_counter += 1

                # Reset accumulator
                current_pages = []

    print(f"\nSummary: {len(chunks)} chunks, {len(legacy_chunks)} legacy oversized chunks")
    return chunks, legacy_chunks


def main() -> None:
    """
    CLI Usage (Windows PowerShell/CMD examples):
      - Activate venv first:
          PowerShell: .\.venv\Scripts\Activate.ps1
          CMD:        .venv\Scripts\activate

      - Chunk a PDF into 2-page parts:
          python scripts\chunk_pdf.py --pdf "DOH-PHIC-JAO-No-2021-002 - Dictionary.pdf"

      - Customize output folders and thresholds:
          python scripts\chunk_pdf.py --pdf input.pdf --chunk-folder pdf_chunks --legacy-folder oversized_chunks ^
            --pages-per-chunk 2 --max-chunk-size-mb 4
    """
    parser = argparse.ArgumentParser(description="Split a PDF into 2-page chunks with size validation.")
    parser.add_argument("--pdf", required=True, help="Path to the input PDF file.")
    parser.add_argument("--chunk-folder", default="pdf_chunks", help="Folder to write generated chunks.")
    parser.add_argument("--legacy-folder", default="oversized_chunks", help="Folder to store oversized original chunks.")
    parser.add_argument("--pages-per-chunk", type=int, default=2, help="Pages per chunk (default: 2).")
    parser.add_argument("--max-chunk-size-mb", type=int, default=4, help="Max chunk size in MB (default: 4).")

    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {pdf_path}")

    chunk_folder = Path(args.chunk_folder)
    legacy_folder = Path(args.legacy_folder)
    pages_per_chunk = max(1, int(args.pages_per_chunk))
    max_chunk_size_bytes = int(args.max_chunk_size_mb) * 1024 * 1024

    print(f"Input: {pdf_path}")
    print(f"Output chunks: {chunk_folder} | Legacy (oversized): {legacy_folder}")
    print(f"Pages per chunk: {pages_per_chunk} | Max size: {args.max_chunk_size_mb} MB\n")

    generate_pdf_chunks(
        pdf_path=pdf_path,
        chunk_folder=chunk_folder,
        legacy_folder=legacy_folder,
        pages_per_chunk=pages_per_chunk,
        max_chunk_size_bytes=max_chunk_size_bytes,
    )


if __name__ == "__main__":
    main()
