"""Upload Freddie Mac sample files into the Bronze layer.

Freddie Mac's dataset requires a login, so the zip files are downloaded
by hand once (see README, Step 1). This script then handles everything
repeatable: validation, checksums, and idempotent upload to S3.

Files are unzipped and stored unchanged, partitioned by vintage year:
    s3://<bucket>/bronze/freddie_mac/origination/vintage=<YYYY>/sample_orig_<YYYY>.txt
    s3://<bucket>/bronze/freddie_mac/performance/vintage=<YYYY>/sample_svcg_<YYYY>.txt

Re-running is safe: a file already in S3 with the same checksum is skipped.

Usage:
    python -m ingestion.freddie_mac_ingestion
    python -m ingestion.freddie_mac_ingestion --dry-run
"""

from __future__ import annotations

import argparse
import re
import tempfile
import zipfile
from pathlib import Path

from ingestion.common import (
    PROJECT_ROOT,
    get_logger,
    get_s3_client,
    load_config,
    md5_of_file,
    require_env,
    s3_object_exists,
)

log = get_logger("freddie_mac_ingest")

ZIP_PATTERN = re.compile(r"^sample_(\d{4})\.zip$")
MEMBER_PATTERNS = {
    "origination": re.compile(r"^sample_orig_(\d{4})\.txt$"),
    "performance": re.compile(r"^sample_svcg_(\d{4})\.txt$"),
}


def classify_member(name: str) -> tuple[str, str] | None:
    """Return (dataset, vintage) for a file inside a sample zip, or None."""
    base = Path(name).name
    for dataset, pattern in MEMBER_PATTERNS.items():
        m = pattern.match(base)
        if m:
            return dataset, m.group(1)
    return None


def find_zips(local_dir: Path, start: int, end: int) -> list[tuple[int, Path]]:
    found = []
    for p in sorted(local_dir.glob("sample_*.zip")):
        m = ZIP_PATTERN.match(p.name)
        if m and start <= int(m.group(1)) <= end:
            found.append((int(m.group(1)), p))
    return found


def validate_pipe_file(path: Path) -> int:
    """Light sanity check: pipe-delimited, non-empty. Returns line count."""
    with open(path, encoding="utf-8", errors="replace") as f:
        first = f.readline()
        if "|" not in first:
            raise ValueError(f"{path.name} does not look pipe-delimited")
        return 1 + sum(1 for _ in f)


def upload_if_changed(s3, bucket: str, key: str, path: Path, dry_run: bool) -> str:
    checksum = md5_of_file(path)
    if s3_object_exists(s3, bucket, key):
        remote = s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("md5")
        if remote == checksum:
            return "skipped (unchanged)"
        # Bronze is immutable: never overwrite silently.
        raise RuntimeError(
            f"{key} already exists with different contents. "
            "Investigate before replacing raw data."
        )
    if dry_run:
        return "would upload"
    s3.upload_file(
        str(path), bucket, key,
        ExtraArgs={"Metadata": {"source": "freddie_mac_sample", "md5": checksum}},
    )
    return "uploaded"


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload Freddie Mac sample files to Bronze.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report without uploading")
    args = parser.parse_args()

    cfg = load_config()
    local_dir = PROJECT_ROOT / cfg["freddie_mac"]["local_dir"]
    start, end = cfg["project"]["vintage_start"], cfg["project"]["vintage_end"]
    bucket = require_env("S3_BUCKET")
    prefix = cfg["s3"]["bronze_prefix"]
    s3 = get_s3_client()

    zips = find_zips(local_dir, start, end)
    missing = sorted(set(range(start, end + 1)) - {y for y, _ in zips})
    if missing:
        log.warning("No zip found for vintages: %s", missing)
    if not zips:
        raise SystemExit(f"No sample_YYYY.zip files found in {local_dir}")

    for year, zpath in zips:
        with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(zpath) as zf:
            for member in zf.namelist():
                info = classify_member(member)
                if not info:
                    continue
                dataset, vintage = info
                extracted = Path(zf.extract(member, tmp))
                rows = validate_pipe_file(extracted)
                key = f"{prefix}/freddie_mac/{dataset}/vintage={vintage}/{extracted.name}"
                status = upload_if_changed(s3, bucket, key, extracted, args.dry_run)
                log.info("%s %-11s %9d rows  %s", vintage, dataset, rows, status)

    log.info("Freddie Mac Bronze ingestion complete.")


if __name__ == "__main__":
    main()
