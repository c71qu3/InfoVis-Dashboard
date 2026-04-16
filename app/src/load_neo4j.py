from neo4j import GraphDatabase
import os
import sys
import pandas as pd


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")
ADDRESS_PATH = "/app/data/Address.csv"
ENTITY_PATH = "/app/data/Entity.csv"


def load_address_csv(driver, path: str=ADDRESS_PATH, batch: int=5000) -> int:

    dtype = {
        "node_id": "Int64",
        "address": str,
        "name": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str}
    df = pd.read_csv(path, dtype=dtype, encoding="utf-8", low_memory=False)
    df = df.where(pd.notnull(df), None)

    def _ensure_constraints(tx):
        tx.run(
            """
            CREATE CONSTRAINT address_node_id_unique IF NOT EXISTS
            FOR (a:Address)
            REQUIRE a.node_id IS UNIQUE
            """)

    def _upsert_batch(tx, rows):
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (a:Address {node_id: row.node_id})
            SET a.address = row.address,
                a.name = row.name,
                a.countries = row.countries,
                a.country_codes = row.country_codes,
                a.sourceID = row.sourceID,
                a.valid_until = row.valid_until,
                a.note = row.note
            """,
            rows=rows)

    rows = df.to_dict("records")
    with driver.session() as session:
        session.execute_write(_ensure_constraints)
        for i in range(0, len(rows), batch):
            session.execute_write(_upsert_batch, rows[i : i + batch])
    return len(rows)


def load_entity_csv(driver, path: str = ENTITY_PATH, batch: int = 5000) -> int:

    dtype = {
        "node_id": "Int64",
        "name": str,
        "original_name": str,
        "former_name": str,
        "jurisdiction": str,
        "jurisdiction_description": str,
        "company_type": str,
        "address": str,
        "internal_id": str,
        "status": str,
        "service_provider": str,
        "ibcRUC": str,
        "country_codes": str,
        "countries": str,
        "sourceID": str,
        "valid_until": str,
        "note": str}
    df = pd.read_csv(path, dtype=dtype, encoding="utf-8", low_memory=False)
    df = df.where(pd.notnull(df), None)

    for col in ["incorporation_date", "inactivation_date", "struck_off_date", "dorm_date"]:
        if col in df.columns:
            dt = pd.to_datetime(df[col], format="%d-%b-%Y", errors="coerce", dayfirst=True)
            df[col] = dt.dt.date

    def _ensure_constraints(tx):
        tx.run(
            """
            CREATE CONSTRAINT entity_node_id_unique IF NOT EXISTS
            FOR (e:Entity)
            REQUIRE e.node_id IS UNIQUE
            """)

    def _upsert_batch(tx, rows):
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (e:Entity {node_id: row.node_id})
            SET e.name = row.name,
                e.original_name = row.original_name,
                e.former_name = row.former_name,
                e.jurisdiction = row.jurisdiction,
                e.jurisdiction_description = row.jurisdiction_description,
                e.company_type = row.company_type,
                e.address = row.address,
                e.internal_id = row.internal_id,
                e.status = row.status,
                e.service_provider = row.service_provider,
                e.ibcRUC = row.ibcRUC,
                e.country_codes = row.country_codes,
                e.countries = row.countries,
                e.sourceID = row.sourceID,
                e.valid_until = row.valid_until,
                e.note = row.note,
                e.incorporation_date = row.incorporation_date,
                e.inactivation_date = row.inactivation_date,
                e.struck_off_date = row.struck_off_date,
                e.dorm_date = row.dorm_date
            """,
            rows=rows)

    rows = df.to_dict("records")
    with driver.session() as session:
        session.execute_write(_ensure_constraints)
        for i in range(0, len(rows), batch):
            session.execute_write(_upsert_batch, rows[i : i + batch])
    return len(rows)


try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    print(f"Adresses: {load_address_csv(driver)}")
    print(f"Entities: {load_entity_csv(driver)}")

except Exception as e:
    print(f"Failed to connect to Neo4j at {NEO4J_URI}:\n{e}")
    sys.exit(1)
finally:
    if 'driver' in locals():
        driver.close()