# load.py
import os, json
from glob import glob
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# 1) Connect to your “Supabase” Postgres
DB_URL = os.getenv("DATABASE_URL",
                   "postgresql://postgres:examplepass@supabase:5432/postgres")
engine = create_engine(DB_URL, future=True)
metadata = MetaData()

# 2) Reflect your tables (they were created in transform scripts)
meetings         = Table("meetings", metadata, autoload_with=engine)
drivers          = Table("drivers", metadata, autoload_with=engine)
sessions         = Table("sessions", metadata, autoload_with=engine)
session_results  = Table("session_results", metadata, autoload_with=engine)
starting_grids   = Table("starting_grid", metadata, autoload_with=engine)

def load_entity(entity_name, table):
    files = glob(f"/app/local_data/processed/{entity_name}/*.json")
    with engine.begin() as conn:
        for path in files:
            records = json.load(open(path))
            stmt = pg_insert(table).values(records)
            # upsert: if PK conflict, do nothing
            stmt = stmt.on_conflict_do_nothing(index_elements=table.primary_key.columns)
            conn.execute(stmt)
    print(f"✅ Loaded {entity_name} ({len(files)} files)")

if __name__ == "__main__":
    load_entity("meetings", meetings)
    load_entity("drivers", drivers)
    load_entity("sessions", sessions)
    load_entity("session_results", session_results)
    load_entity("starting_grids", starting_grids)
