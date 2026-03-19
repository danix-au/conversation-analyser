#!/usr/bin/env python3
"""
Extract JSON payloads from a CSV or JSONL content field.

Usage:
    python scripts/extract_content_json.py
    python scripts/extract_content_json.py output/conversationtranscripts_changes_20260319T001616Z.jsonl
    python scripts/extract_content_json.py output
    python scripts/extract_content_json.py "output/*_changes_*.jsonl" --latest-only
    python scripts/extract_content_json.py output --output-format files
    python scripts/extract_content_json.py --output-dir output/extracted_content.jsonl
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import re
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path


DEFAULT_INPUT = Path("output/conversationtranscripts_changes_latest.jsonl")


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "row"


def normalize_header(value: str) -> str:
    return value.strip().lstrip("\ufeff").strip().strip('"').lower()


def choose_file_stem(row: dict[str, str], row_index: int) -> str:
    transcript_id = (row.get("conversationtranscriptid") or "").strip()
    name = (row.get("name") or "").strip()
    if transcript_id:
        return safe_name(transcript_id)
    if name:
        return safe_name(name)
    return f"row_{row_index:05d}"


def iter_csv_rows(input_path: Path) -> Iterator[dict[str, str]]:
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        csv.field_size_limit(10**7)

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        header_map = {normalize_header(name): name for name in fieldnames}
        content_key = header_map.get("content")

        if not content_key:
            raise ValueError("'content' column not found in CSV header.")

        for row in reader:
            yield {normalize_header(key): value for key, value in row.items() if key}


def iter_jsonl_rows(input_path: Path) -> Iterator[dict[str, object]]:
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL at line {line_number}: {exc.msg} "
                    f"(line {exc.lineno}, col {exc.colno})"
                ) from exc

            if not isinstance(parsed, dict):
                raise ValueError(f"JSONL line {line_number} is not an object.")

            yield {normalize_header(str(key)): value for key, value in parsed.items()}


def iter_input_rows(input_path: Path) -> Iterable[dict[str, object]]:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return iter_csv_rows(input_path)
    if suffix == ".jsonl":
        return iter_jsonl_rows(input_path)
    raise ValueError("Unsupported input format. Use a .csv or .jsonl file.")


def resolve_input_files(input_spec: str) -> list[Path]:
    candidate = Path(input_spec).expanduser()

    if candidate.exists():
        resolved = candidate.resolve()
        if resolved.is_file():
            return [resolved]
        if resolved.is_dir():
            files = sorted(
                [
                    *resolved.glob("*.jsonl"),
                    *resolved.glob("*.csv"),
                ]
            )
            return [file.resolve() for file in files if file.is_file()]

    if any(char in input_spec for char in "*?[]"):
        matched = [Path(path).resolve() for path in glob.glob(input_spec) if Path(path).is_file()]
        return sorted(matched)

    return []


def choose_record_key(row: dict[str, str], row_index: int) -> str:
    transcript_id = (row.get("conversationtranscriptid") or "").strip()
    if transcript_id:
        return f"conversationtranscriptid:{safe_name(transcript_id)}"

    name = (row.get("name") or "").strip()
    if name:
        return f"name:{safe_name(name)}"

    return f"row:{row_index:08d}"


def record_sort_token(row: dict[str, str], row_index: int) -> tuple[str, int]:
    modifiedon = (row.get("modifiedon") or "").strip()
    return (modifiedon, row_index)


def choose_jsonl_output_path(output_arg: str | None, input_files: list[Path], default_base: Path) -> Path:
    if output_arg:
        candidate = Path(output_arg).expanduser().resolve()
        if candidate.suffix.lower() == ".jsonl":
            candidate.parent.mkdir(parents=True, exist_ok=True)
            return candidate
        candidate.mkdir(parents=True, exist_ok=True)
        if len(input_files) == 1:
            return candidate / f"{input_files[0].stem}_content.jsonl"
        return candidate / "batch_content.jsonl"

    if len(input_files) == 1:
        return default_base / f"{input_files[0].stem}_content.jsonl"
    return default_base / "batch_content.jsonl"


def row_to_export_record(
    row: dict[str, str],
    parsed_content: object,
    row_index: int,
    source_file: Path,
) -> dict[str, object]:
    record: dict[str, object] = {key: value for key, value in row.items() if key != "content"}
    record["source_input_file"] = source_file.name
    record["source_row_index"] = row_index
    record["content_json"] = parsed_content
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract JSON from the input 'content' field."
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default=str(DEFAULT_INPUT),
        help="Path to source file, directory, or glob pattern for .csv/.jsonl files.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Output destination. For --output-format jsonl, this can be a .jsonl file path "
            "or a directory. For --output-format files, this must be a directory."
        ),
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Write compact JSON for --output-format files.",
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help=(
            "Keep only the latest record per transcript key across all inputs "
            "(uses modifiedon when available, otherwise last occurrence)."
        ),
    )
    parser.add_argument(
        "--output-format",
        choices=["jsonl", "files"],
        default="jsonl",
        help="Output format: jsonl (default) or per-record JSON files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_files = resolve_input_files(args.input_path)
    if not input_files:
        print(
            "ERROR: No input files found. Provide a .csv/.jsonl file, directory, or glob pattern.",
            file=sys.stderr,
        )
        return 1

    input_label = args.input_path
    default_output_base = input_files[0].parent

    output_path: Path | None = None
    output_dir: Path | None = None
    if args.output_format == "jsonl":
        output_path = choose_jsonl_output_path(args.output_dir, input_files, default_output_base)
    else:
        if args.output_dir:
            output_dir = Path(args.output_dir).expanduser().resolve()
        elif len(input_files) == 1:
            output_dir = default_output_base / f"{input_files[0].stem}_content_json"
        else:
            output_dir = default_output_base / "batch_content_json"
        output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped_empty = 0
    parse_errors = 0
    used_names: dict[str, int] = {}
    latest_rows: dict[str, tuple[tuple[str, int], dict[str, str], object, int, Path]] = {}
    global_row_index = 0
    writer = None

    try:
        if args.output_format == "jsonl" and not args.latest_only:
            assert output_path is not None
            output_path.parent.mkdir(parents=True, exist_ok=True)
            writer = output_path.open("w", encoding="utf-8")

        for input_file in input_files:
            rows = iter_input_rows(input_file)
            for raw_row in rows:
                global_row_index += 1
                row = {key: "" if value is None else str(value) for key, value in raw_row.items()}
                raw_content = row.get("content", "")
                if not raw_content.strip():
                    skipped_empty += 1
                    continue

                try:
                    parsed = json.loads(raw_content)
                except json.JSONDecodeError as exc:
                    parse_errors += 1
                    print(
                        f"WARN: Row {global_row_index} content JSON parse failed: {exc.msg} "
                        f"(line {exc.lineno}, col {exc.colno})",
                        file=sys.stderr,
                    )
                    continue

                if args.latest_only:
                    record_key = choose_record_key(row, global_row_index)
                    token = record_sort_token(row, global_row_index)
                    current = latest_rows.get(record_key)
                    if current is None or token >= current[0]:
                        latest_rows[record_key] = (token, row, parsed, global_row_index, input_file)
                    continue

                if args.output_format == "jsonl":
                    assert writer is not None
                    export_record = row_to_export_record(row, parsed, global_row_index, input_file)
                    writer.write(json.dumps(export_record, ensure_ascii=False) + "\n")
                    written += 1
                    continue

                assert output_dir is not None
                stem = choose_file_stem(row, global_row_index)
                count = used_names.get(stem, 0) + 1
                used_names[stem] = count
                if count > 1:
                    stem = f"{stem}_{count}"

                output_file = output_dir / f"{stem}.json"
                with output_file.open("w", encoding="utf-8") as out:
                    if args.compact:
                        json.dump(parsed, out, ensure_ascii=False, separators=(",", ":"))
                    else:
                        json.dump(parsed, out, ensure_ascii=False, indent=2)
                written += 1

        if args.latest_only:
            if args.output_format == "jsonl":
                assert output_path is not None
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("w", encoding="utf-8") as out:
                    for _, row, parsed, original_row_index, source_file in latest_rows.values():
                        export_record = row_to_export_record(row, parsed, original_row_index, source_file)
                        out.write(json.dumps(export_record, ensure_ascii=False) + "\n")
                        written += 1
            else:
                assert output_dir is not None
                for _, row, parsed, original_row_index, _ in latest_rows.values():
                    stem = choose_file_stem(row, original_row_index)
                    output_file = output_dir / f"{stem}.json"
                    with output_file.open("w", encoding="utf-8") as out:
                        if args.compact:
                            json.dump(parsed, out, ensure_ascii=False, separators=(",", ":"))
                        else:
                            json.dump(parsed, out, ensure_ascii=False, indent=2)
                    written += 1
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if writer is not None:
            writer.close()

    print(f"Source input spec: {input_label}")
    print(f"Input files processed: {len(input_files)}")
    if args.output_format == "jsonl":
        print(f"Output jsonl: {output_path}")
    else:
        print(f"Output dir: {output_dir}")
    print(f"Output format: {args.output_format}")
    print(f"Files written: {written}")
    print(f"Rows skipped (empty content): {skipped_empty}")
    print(f"Rows skipped (invalid JSON): {parse_errors}")

    return 0 if written > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
