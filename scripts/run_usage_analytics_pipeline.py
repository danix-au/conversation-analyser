#!/usr/bin/env python3
"""
Run the end-to-end usage analytics pipeline:
1) Pull Dataverse incremental extract (JSONL)
2) Extract transcript content JSONL
3) Run transcript analytics (with Entra enrichment via existing az login) and export Parquet
4) Remove temporary files and archive processed JSONL extracts

Usage:
    python scripts/run_usage_analytics_pipeline.py
    python scripts/run_usage_analytics_pipeline.py --skip-dataverse-extract
    python scripts/run_usage_analytics_pipeline.py --process-all
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dataverse extract -> content extraction -> analytics parquet pipeline."
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory containing Dataverse JSONL extracts (default: output)",
    )
    parser.add_argument(
        "--archive-dir",
        default=None,
        help="Directory to move processed JSONL files into (default: <output-dir>/archive)",
    )
    parser.add_argument(
        "--skip-dataverse-extract",
        action="store_true",
        help="Skip running dataverse_incremental_extract.py and process existing JSONL files.",
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help="Process all matching JSONL extracts in output-dir (default: newest only).",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary extracted-content JSONL files (default: remove after each run).",
    )
    return parser.parse_args()


def run_cmd(cmd: list[str], cwd: Path) -> None:
    print(f"\n>> Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(cmd)}")


def next_available_path(path: Path) -> Path:
    if not path.exists():
        return path

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    candidate = path.with_name(f"{path.stem}_{timestamp}{path.suffix}")
    index = 2
    while candidate.exists():
        candidate = path.with_name(f"{path.stem}_{timestamp}_{index}{path.suffix}")
        index += 1
    return candidate


def find_extract_files(output_dir: Path) -> list[Path]:
    return sorted(output_dir.glob("*_changes_*.jsonl"), key=lambda p: p.stat().st_mtime)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    output_dir = (repo_root / args.output_dir).resolve()
    archive_dir = (
        (repo_root / args.archive_dir).resolve()
        if args.archive_dir
        else (output_dir / "archive").resolve()
    )
    temp_root = output_dir / "_tmp"

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    temp_root.mkdir(parents=True, exist_ok=True)

    dataverse_script = repo_root / "scripts" / "dataverse_incremental_extract.py"
    extract_content_script = repo_root / "scripts" / "extract_content_json.py"
    analytics_script = repo_root / "scripts" / "transcript_analytics.py"

    if not args.skip_dataverse_extract:
        run_cmd([sys.executable, str(dataverse_script)], cwd=repo_root)

    extract_files = find_extract_files(output_dir)
    if not extract_files:
        print("No Dataverse extract files found to process.")
        return 0

    if not args.process_all:
        extract_files = [extract_files[-1]]

    processed = 0
    skipped_empty = 0
    failed = 0

    for extract_file in extract_files:
        stem = extract_file.stem
        temp_jsonl = temp_root / f"{stem}_content.jsonl"
        parquet_file = output_dir / f"usage_analytics_{stem}.parquet"

        print("\n" + "=" * 72)
        print(f"Processing extract: {extract_file.name}")
        print(f"Parquet output: {parquet_file.name}")
        print("=" * 72)

        try:
            if extract_file.stat().st_size == 0:
                archived_path = next_available_path(archive_dir / extract_file.name)
                shutil.move(str(extract_file), str(archived_path))
                print(f"No changes in extract, archived without analytics: {archived_path}")
                skipped_empty += 1
                continue

            run_cmd(
                [
                    sys.executable,
                    str(extract_content_script),
                    str(extract_file),
                    "--latest-only",
                    "--output-format",
                    "jsonl",
                    "--output-dir",
                    str(temp_jsonl),
                ],
                cwd=repo_root,
            )

            run_cmd(
                [
                    sys.executable,
                    str(analytics_script),
                    str(temp_jsonl),
                    "--parquet",
                    str(parquet_file),
                    "--enrich-entra",
                ],
                cwd=repo_root,
            )

            archived_path = next_available_path(archive_dir / extract_file.name)
            shutil.move(str(extract_file), str(archived_path))
            print(f"Archived extract: {archived_path}")
            processed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR processing {extract_file.name}: {exc}", file=sys.stderr)
            failed += 1
        finally:
            if not args.keep_temp and temp_jsonl.exists():
                temp_jsonl.unlink(missing_ok=True)

    if not args.keep_temp and temp_root.exists() and not any(temp_root.iterdir()):
        temp_root.rmdir()

    print("\nPipeline summary")
    print(f"Processed successfully: {processed}")
    print(f"Skipped (empty extract): {skipped_empty}")
    print(f"Failed: {failed}")
    print(f"Archive dir: {archive_dir}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
