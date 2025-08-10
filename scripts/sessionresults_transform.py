#!/usr/bin/env python3
# scripts/sessionresults_transform.py

import os
import json
import boto3
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    Boolean,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ─── CONFIG ───
RAW_BUCKET       = os.getenv("RAW_BUCKET", "etl-f1-data")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "etl-f1-processed")
DATABASE_URL     = os.getenv("DATABASE_URL")
PREFIX           = "session_results/"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# ─── CLIENTS ───
s3     = boto3.client("s3")
engine = create_engine(DATABASE_URL, echo=False, future=True)
metadata = MetaData()

# ─── SCHEMA ───
session_results = Table(
    "session_results",
    metadata,
    Column("meeting_key", Integer, primary_key=True),
    Column("session_key", Integer, primary_key=True),
    Column("driver_number", Integer, primary_key=True),
    Column("position", Integer),
    Column("number_of_laps", Integer),
    Column("dnf", Boolean),
    Column("dns", Boolean),
    Column("dsq", Boolean),
)

metadata.create_all(engine)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# ─── LOAD & UPSERT ───
paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=RAW_BUCKET, Prefix=PREFIX)

with engine.begin() as conn:
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]  # e.g. "session_results/1253_9683.json"
            resp = s3.get_object(Bucket=RAW_BUCKET, Key=key)
            records = json.loads(resp["Body"].read())

            if not isinstance(records, list):
                continue

            for rec in records:
                row = {
                    "meeting_key":     rec.get("meeting_key"),
                    "session_key":     rec.get("session_key"),
                    "driver_number":   rec.get("driver_number"),
                    "position":        rec.get("position"),
                    "number_of_laps":  rec.get("number_of_laps"),
                    "dnf":             rec.get("dnf"),
                    "dns":             rec.get("dns"),
                    "dsq":             rec.get("dsq"),
                }

                if None in (row["meeting_key"], row["session_key"], row["driver_number"]):
                    print("⚠️ Skipping malformed row:", rec)
                    continue

                stmt = pg_insert(session_results).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["meeting_key", "session_key", "driver_number"],
                    set_={
                        "position":        stmt.excluded.position,
                        "number_of_laps":  stmt.excluded.number_of_laps,
                        "dnf":             stmt.excluded.dnf,
                        "dns":             stmt.excluded.dns,
                        "dsq":             stmt.excluded.dsq,
                    },
                )
                conn.execute(stmt)

    print("✅ Upserted session_results into Postgres")

# ─── DUMP & UPLOAD ───
processed_dir  = os.path.join("local_data", "processed", "session_results")
processed_file = os.path.join(processed_dir, "session_results.json")
ensure_dir(processed_dir)

with engine.connect() as conn:
    rows = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM session_results"))]

with open(processed_file, "w", encoding="utf-8") as pf:
    json.dump(rows, pf, indent=2, ensure_ascii=False)

try:
    s3.upload_file(
        processed_file,
        PROCESSED_BUCKET,
        "session_results/session_results.json"
    )
    print(f"☁️  Uploaded processed session_results to s3://{PROCESSED_BUCKET}/session_results/session_results.json")
except Exception as e:
    print(f"⚠️  Failed to upload processed session_results: {e}")
