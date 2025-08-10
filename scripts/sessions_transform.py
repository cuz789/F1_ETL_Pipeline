#!/usr/bin/env python3
# scripts/sessions_transform.py

import os
import json
import boto3
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ─── CONFIG ───
RAW_BUCKET       = os.getenv("RAW_BUCKET", "etl-f1-data")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "etl-f1-processed")
DATABASE_URL     = os.getenv("DATABASE_URL")
PREFIX           = "sessions/"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# ─── CLIENTS ───
s3     = boto3.client("s3")
engine = create_engine(DATABASE_URL, echo=False, future=True)
metadata = MetaData()

# ─── SCHEMA ───
sessions = Table(
    "sessions",
    metadata,
    Column("session_key", Integer, primary_key=True),
    Column("meeting_key", Integer),
    Column("session_type", String),
    Column("session_name", String),
    Column("location", String),
    Column("country_code", String),
    Column("country_name", String),
    Column("circuit_key", Integer),
    Column("circuit_short_name", String),
    Column("gmt_offset", String),
    Column("date_start", String),
    Column("date_end", String),
    Column("year", Integer),
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
            key = obj["Key"]  # e.g. "sessions/1253_sessions.json"
            resp = s3.get_object(Bucket=RAW_BUCKET, Key=key)
            recs = json.loads(resp["Body"].read())
            if not isinstance(recs, list):
                continue

            for rec in recs:
                row = {
                    "session_key": rec.get("session_key"),
                    "meeting_key": rec.get("meeting_key"),
                    "session_type": rec.get("session_type"),
                    "session_name": rec.get("session_name"),
                    "location": rec.get("location"),
                    "country_code": rec.get("country_code"),
                    "country_name": rec.get("country_name"),
                    "circuit_key": rec.get("circuit_key"),
                    "circuit_short_name": rec.get("circuit_short_name"),
                    "gmt_offset": rec.get("gmt_offset"),
                    "date_start": rec.get("date_start"),
                    "date_end": rec.get("date_end"),
                    "year": rec.get("year"),
                }

                if not row["session_key"]:
                    print("⚠️ Skipping malformed session row:", rec)
                    continue

                stmt = pg_insert(sessions).values(**row)
                do_update = stmt.on_conflict_do_update(
                    index_elements=["session_key"],
                    set_={col.name: getattr(stmt.excluded, col.name)
                          for col in sessions.columns
                          if col.name != "session_key"}
                )
                conn.execute(do_update)

    print("✅ Upserted all sessions into Postgres")

# ─── DUMP & UPLOAD ───
processed_dir  = os.path.join("local_data", "processed", "sessions")
processed_file = os.path.join(processed_dir, "sessions.json")
ensure_dir(processed_dir)

with engine.connect() as conn:
    rows = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM sessions"))]

with open(processed_file, "w", encoding="utf-8") as pf:
    json.dump(rows, pf, indent=2, ensure_ascii=False)

try:
    s3.upload_file(processed_file, PROCESSED_BUCKET, "sessions/sessions.json")
    print(f"☁️  Uploaded processed sessions to s3://{PROCESSED_BUCKET}/sessions/sessions.json")
except Exception as e:
    print(f"⚠️  Failed to upload processed sessions: {e}")
