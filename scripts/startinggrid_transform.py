#!/usr/bin/env python3
# scripts/startinggrid_transform.py

import os
import json
import boto3
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ─── CONFIG ───
RAW_BUCKET       = os.getenv("RAW_BUCKET", "etl-f1-data")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "etl-f1-processed")
DATABASE_URL     = os.getenv("DATABASE_URL")
PREFIX           = "starting_grids/"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# ─── CLIENTS ───
s3     = boto3.client("s3")
engine = create_engine(DATABASE_URL, echo=False, future=True)
metadata = MetaData()

# ─── SCHEMA ───
starting_grid = Table(
    "starting_grid",
    metadata,
    Column("meeting_key", Integer, primary_key=True),
    Column("session_key", Integer, primary_key=True),
    Column("driver_number", Integer, primary_key=True),
    Column("position", Integer),
    Column("lap_duration", Float),
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
            key = obj["Key"]  # e.g. "starting_grids/1254_starting_grid.json"
            resp = s3.get_object(Bucket=RAW_BUCKET, Key=key)
            records = json.loads(resp["Body"].read())

            if not isinstance(records, list):
                continue

            for rec in records:
                row = {
                    "meeting_key":    rec.get("meeting_key"),
                    "session_key":    rec.get("session_key"),
                    "driver_number":  rec.get("driver_number"),
                    "position":       rec.get("position"),
                    "lap_duration":   rec.get("lap_duration"),
                }

                if None in (row["meeting_key"], row["session_key"], row["driver_number"]):
                    print("⚠️ Skipping malformed row:", rec)
                    continue

                stmt = pg_insert(starting_grid).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["meeting_key", "session_key", "driver_number"],
                    set_={
                        "position":     stmt.excluded.position,
                        "lap_duration": stmt.excluded.lap_duration,
                    },
                )
                conn.execute(stmt)

    print("✅ Upserted starting_grid into Postgres")

# ─── DUMP & UPLOAD ───
processed_dir  = os.path.join("local_data", "processed", "starting_grids")
processed_file = os.path.join(processed_dir, "starting_grids.json")
ensure_dir(processed_dir)

with engine.connect() as conn:
    rows = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM starting_grid"))]

with open(processed_file, "w", encoding="utf-8") as pf:
    json.dump(rows, pf, indent=2, ensure_ascii=False)

try:
    s3.upload_file(
        processed_file,
        PROCESSED_BUCKET,
        "starting_grids/starting_grids.json"
    )
    print(f"☁️  Uploaded processed starting_grid to s3://{PROCESSED_BUCKET}/starting_grids/starting_grids.json")
except Exception as e:
    print(f"⚠️  Failed to upload processed starting_grid: {e}")
