#!/usr/bin/env python3
"""
scripts/extract.py

Fetches OpenF1 data for 2025:
  - meetings
  - drivers
  - sessions
  - session_result (saved as meeting_key_session_key)
  - starting_grid (by meeting)

Saves raw JSON under local_data/raw/{endpoint}/ and uploads to S3.
"""

import os
import boto3
import time
import json
import requests
from typing import Dict, List, Tuple

# ─── Configuration ──────────────────────────────────────────────────────────────
BASE_URL       = "https://api.openf1.org/v1"
YEAR           = 2025
RAW_ROOT       = os.path.join("local_data", "raw")
RETRY_COUNT    = 3
RETRY_WAIT     = 3     # seconds if no Retry-After header
THROTTLE_PAUSE = 0.5   # seconds between every request

# S3 configuration
RAW_BUCKET = os.getenv("RAW_BUCKET", "etl-f1-data")
s3 = boto3.client("s3")

# ─── Helper Functions ────────────────────────────────────────────────────────────
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def fetch_json(endpoint: str, params: Dict = None) -> Dict:
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, params=params or {}, timeout=10)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra is not None else RETRY_WAIT
                print(f"⏳  [{endpoint}] rate limited; retry after {wait:.1f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️  [{endpoint}] attempt {attempt} failed: {e}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
            else:
                print(f"❌  [{endpoint}] giving up after {RETRY_COUNT} attempts")
                return {}
    return {}


def save_json(data: Dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def upload_to_s3(local_path: str, s3_key: str):
    try:
        s3.upload_file(local_path, RAW_BUCKET, s3_key)
        print(f"☁️  Uploaded to s3://{RAW_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"⚠️  Failed to upload {local_path} to S3: {e}")

# ─── Extraction Steps ───────────────────────────────────────────────────────────
def extract_meetings() -> List[int]:
    out_dir = os.path.join(RAW_ROOT, "meetings")
    ensure_dir(out_dir)

    data = fetch_json("meetings", {"year": YEAR})
    filename = f"meetings_{YEAR}.json"
    path = os.path.join(out_dir, filename)
    save_json(data, path)
    upload_to_s3(path, f"meetings/{filename}")

    keys = [m["meeting_key"] for m in data] if isinstance(data, list) else []
    print(f"✔ Saved {len(keys)} meetings to {path}")
    time.sleep(THROTTLE_PAUSE)
    return keys


def extract_drivers():
    out_dir = os.path.join(RAW_ROOT, "drivers")
    ensure_dir(out_dir)

    data = fetch_json("drivers")
    filename = "drivers.json"
    path = os.path.join(out_dir, filename)
    save_json(data, path)
    upload_to_s3(path, f"drivers/{filename}")

    count = len(data) if isinstance(data, list) else 0
    print(f"✔ Saved {count} drivers to {path}")
    time.sleep(THROTTLE_PAUSE)


def extract_sessions(meeting_keys: List[int]) -> List[Tuple[int, int]]:
    out_dir = os.path.join(RAW_ROOT, "sessions")
    ensure_dir(out_dir)

    pairs: List[Tuple[int, int]] = []
    for mk in meeting_keys:
        data = fetch_json("sessions", {"meeting_key": mk})
        filename = f"{mk}_sessions.json"
        path = os.path.join(out_dir, filename)
        save_json(data, path)
        upload_to_s3(path, f"sessions/{filename}")

        session_keys = [s["session_key"] for s in data] if isinstance(data, list) else []
        print(f"✔ [{mk}] Saved {len(session_keys)} sessions to {path}")
        for sk in session_keys:
            pairs.append((mk, sk))
        time.sleep(THROTTLE_PAUSE)
    return pairs


def extract_session_results(session_pairs: List[Tuple[int, int]]):
    out_dir = os.path.join(RAW_ROOT, "session_results")
    ensure_dir(out_dir)

    for mk, sk in session_pairs:
        data = fetch_json("session_result", {"session_key": sk})
        filename = f"{mk}_{sk}.json"
        path = os.path.join(out_dir, filename)
        save_json(data, path)
        upload_to_s3(path, f"session_results/{filename}")
        print(f"✔ [{mk}+{sk}] Saved session_result to {path}")
        time.sleep(THROTTLE_PAUSE)


def extract_starting_grids(meeting_keys: List[int]):
    out_dir = os.path.join(RAW_ROOT, "starting_grids")
    ensure_dir(out_dir)

    for mk in meeting_keys:
        data = fetch_json("starting_grid", {"meeting_key": mk})
        filename = f"{mk}_starting_grid.json"
        path = os.path.join(out_dir, filename)
        save_json(data, path)
        upload_to_s3(path, f"starting_grids/{filename}")
        print(f"✔ [{mk}] Saved starting_grid to {path}")
        time.sleep(THROTTLE_PAUSE)

# ─── Main Execution ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 Starting OpenF1 {YEAR} data extraction…")
    meeting_keys = extract_meetings()
    extract_drivers()
    session_pairs = extract_sessions(meeting_keys)
    extract_session_results(session_pairs)
    extract_starting_grids(meeting_keys)
    print("🎉 Extraction complete!")
