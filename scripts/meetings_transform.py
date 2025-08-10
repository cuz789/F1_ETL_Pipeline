#!/usr/bin/env python3
# scripts/meetings_transform.py

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

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# ─── CLIENTS ───
s3     = boto3.client("s3")
engine = create_engine(DATABASE_URL, echo=False, future=True)
metadata = MetaData()

# ─── SCHEMA ───
meetings = Table(
    "meetings",
    metadata,
    Column("meeting_key", Integer, primary_key=True),
    Column("circuit_key", Integer),
    Column("circuit_short_name", String),
    Column("meeting_code", String),
    Column("location", String),
    Column("country_key", Integer),
    Column("country_code", String),
    Column("country_name", String),
    Column("meeting_name", String),
    Column("meeting_official_name", String),
    Column("gmt_offset", String),
    Column("date_start", String),
    Column("year", Integer),
)

metadata.create_all(engine)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# ─── LOAD & UPSERT ───
resp    = s3.get_object(Bucket=RAW_BUCKET, Key="meetings/meetings_2025.json")
records = json.loads(resp["Body"].read())

with engine.begin() as conn:
    for rec in records:
        row = {
            "meeting_key":            rec.get("meeting_key"),
            "circuit_key":            rec.get("circuit_key"),
            "circuit_short_name":     rec.get("circuit_short_name"),
            "meeting_code":           rec.get("meeting_code"),
            "location":               rec.get("location"),
            "country_key":            rec.get("country_key"),
            "country_code":           rec.get("country_code"),
            "country_name":           rec.get("country_name"),
            "meeting_name":           rec.get("meeting_name"),
            "meeting_official_name":  rec.get("meeting_official_name"),
            "gmt_offset":             rec.get("gmt_offset"),
            "date_start":             rec.get("date_start"),
            "year":                   rec.get("year"),
        }

        if not row["meeting_key"]:
            print("⚠️ Skipping invalid record:", rec)
            continue

        stmt = pg_insert(meetings).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["meeting_key"],
            set_={col.name: getattr(stmt.excluded, col.name)
                  for col in meetings.columns
                  if col.name != "meeting_key"}
        )
        conn.execute(stmt)

print(f"✅ Upserted {len(records)} meeting rows into Postgres")

# ─── DUMP & UPLOAD ───
processed_dir  = os.path.join("local_data", "processed", "meetings")
processed_file = os.path.join(processed_dir, "meetings.json")
ensure_dir(processed_dir)

with engine.connect() as conn:
    rows = [dict(row._mapping) for row in conn.execute(text("SELECT * FROM meetings"))]

with open(processed_file, "w", encoding="utf-8") as pf:
    json.dump(rows, pf, indent=2, ensure_ascii=False)

try:
    s3.upload_file(processed_file, PROCESSED_BUCKET, "meetings/meetings.json")
    print(f"☁️  Uploaded processed meetings to s3://{PROCESSED_BUCKET}/meetings/meetings.json")
except Exception as e:
    print(f"⚠️  Failed to upload processed meetings: {e}")
