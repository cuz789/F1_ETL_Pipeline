#!/usr/bin/env python3
# scripts/drivers_transform.py

import os
import json
import boto3
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ── CONFIG ──
RAW_BUCKET       = os.getenv("RAW_BUCKET", "etl-f1-data")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "etl-f1-processed")
DATABASE_URL     = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set")

s3 = boto3.client("s3")
engine = create_engine(DATABASE_URL, echo=False, future=True)
metadata = MetaData()

# ── SCHEMA ──
drivers = Table(
    "drivers", metadata,
    Column("meeting_key", Integer, primary_key=True),
    Column("session_key", Integer, primary_key=True),
    Column("driver_number", Integer, primary_key=True),
    Column("full_name", String),
    Column("first_name", String),
    Column("last_name", String),
    Column("team_name", String),
)

metadata.create_all(engine)

# ── FETCH RAW JSON FROM S3 ──
resp    = s3.get_object(Bucket=RAW_BUCKET, Key="drivers/drivers.json")
records = json.loads(resp["Body"].read())

# ── UPSERT ──
with engine.begin() as conn:
    for rec in records:
        row = {
            "meeting_key":    rec.get("meeting_key"),
            "session_key":    rec.get("session_key"),
            "driver_number":  rec.get("driver_number"),
            "full_name":      rec.get("full_name"),
            "first_name":     rec.get("first_name"),
            "last_name":      rec.get("last_name"),
            "team_name":      rec.get("team_name"),
        }

        if not row["meeting_key"] or not row["session_key"] or not row["driver_number"]:
            print("⚠️ Skipping invalid record:", rec)
            continue

        stmt = pg_insert(drivers).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["meeting_key", "session_key", "driver_number"],
            set_={col.name: getattr(stmt.excluded, col.name)
                  for col in drivers.columns
                  if col.name not in drivers.primary_key}
        )
        conn.execute(stmt)

print(f"✅ Upserted {len(records)} driver rows into Postgres")

# ── DUMP PROCESSED JSON LOCALLY ──
processed_dir  = os.path.join("local_data", "processed", "drivers")
processed_file = os.path.join(processed_dir, "drivers.json")
os.makedirs(processed_dir, exist_ok=True)

with engine.connect() as conn:
    rows = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM drivers"))]

with open(processed_file, "w", encoding="utf-8") as f:
    json.dump(rows, f, indent=2)

# ── UPLOAD TO S3 ──
try:
    s3.upload_file(processed_file, PROCESSED_BUCKET, "drivers/drivers.json")
    print(f"☁️ Uploaded cleaned drivers JSON to s3://{PROCESSED_BUCKET}/drivers/drivers.json")
except Exception as e:
    print(f"⚠️ Failed to upload to S3: {e}")
