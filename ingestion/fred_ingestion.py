"""Pull macroeconomic series from the FRED API into the Bronze layer.

Each series is saved as the raw JSON response, unchanged, at:
    s3://<bucket>/bronze/fred/series_id=<ID>/ingest_date=<YYYY-MM-DD>/observations.json

Usage:
    python -m ingestion.fred_ingestion                 # national + state series to S3
    python -m ingestion.fred_ingestion --national-only # skip the ~100 state series
    python -m ingestion.fred_ingestion --local         # write to data/bronze/fred instead of S3
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.common import (
    PROJECT_ROOT,
    get_logger,
    get_s3_client,
    ingest_date,
    load_config,
    md5_of_bytes,
    require_env,
)

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
log = get_logger("fred_ingest")


def build_session() -> requests.Session:
    """HTTP session with automatic retries for transient errors and rate limits."""
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "mortgage-delinquency-capstone/1.0"
    return session


def series_list(cfg: dict, national_only: bool) -> list[str]:
    fred_cfg = cfg["fred"]
    ids = list(fred_cfg["national_series"].keys())
    if not national_only:
        for template in fred_cfg["state_series_templates"]:
            ids += [template.format(state=s) for s in fred_cfg["states"]]
    return ids


def fetch_series(session: requests.Session, series_id: str, api_key: str, start: str) -> bytes:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    resp = session.get(FRED_URL, params=params, timeout=30)
    if resp.status_code == 400:
        # FRED returns 400 for unknown series IDs; log and skip rather than crash.
        raise ValueError(f"FRED rejected series {series_id}: {resp.text[:200]}")
    resp.raise_for_status()
    payload = resp.json()
    if "observations" not in payload:
        raise ValueError(f"Unexpected response for {series_id}: missing 'observations'")
    return resp.content  # raw bytes, stored exactly as received


def write_local(series_id: str, raw: bytes, run_date: str) -> Path:
    out = PROJECT_ROOT / "data" / "bronze" / "fred" / f"series_id={series_id}" / f"ingest_date={run_date}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "observations.json"
    path.write_bytes(raw)
    return path


def write_s3(s3, bucket: str, prefix: str, series_id: str, raw: bytes, run_date: str) -> str:
    key = f"{prefix}/fred/series_id={series_id}/ingest_date={run_date}/observations.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw,
        ContentType="application/json",
        Metadata={"source": "fred", "series_id": series_id, "md5": md5_of_bytes(raw)},
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest FRED series into Bronze.")
    parser.add_argument("--local", action="store_true", help="Write to data/bronze instead of S3")
    parser.add_argument("--national-only", action="store_true", help="Skip state-level series")
    args = parser.parse_args()

    cfg = load_config()
    api_key = require_env("FRED_API_KEY")
    start = cfg["fred"]["observation_start"]
    run_date = ingest_date()
    session = build_session()

    s3 = bucket = None
    if not args.local:
        bucket = require_env("S3_BUCKET")
        s3 = get_s3_client()

    ids = series_list(cfg, args.national_only)
    log.info("Ingesting %d FRED series (start=%s, run_date=%s)", len(ids), start, run_date)

    ok, failed = 0, []
    for sid in ids:
        try:
            raw = fetch_series(session, sid, api_key, start)
            n_obs = len(json.loads(raw)["observations"])
            dest = (
                write_local(sid, raw, run_date)
                if args.local
                else write_s3(s3, bucket, cfg["s3"]["bronze_prefix"], sid, raw, run_date)
            )
            log.info("OK   %-12s %5d obs -> %s", sid, n_obs, dest)
            ok += 1
        except Exception as e:  # keep going; report failures at the end
            log.warning("FAIL %-12s %s", sid, e)
            failed.append(sid)
        time.sleep(0.6)  # stay well under FRED's rate limit (120 requests/minute)

    log.info("Done. %d succeeded, %d failed.", ok, len(failed))
    if failed:
        log.warning("Failed series: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
