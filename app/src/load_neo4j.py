from neo4j import GraphDatabase
import os
import sys
import pandas as pd


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

DATAPATH = "/app/data/{}.csv"
ADDRESS_PATH = DATAPATH.format("Address")
ENTITY_PATH = DATAPATH.format("Entity")
INTERMEDIARY_PATH = DATAPATH.format("Intermediary")
OFFICER_PATH = DATAPATH.format("Officer")


def load_csv_upsert(
    driver, label: str, path: str,
    dtype: dict, unique_key: str="node_id",
    batch: int=5000, date_cols: list=[]
) -> int:
    df = pd.read_csv(path, dtype=dtype, encoding="utf-8", low_memory=False)
    df = df.where(pd.notnull(df), None)

    if unique_key in df.columns:
        df = df[df[unique_key].notnull()]

    if date_cols:
        for col in date_cols:
            if col in df.columns:
                dt = pd.to_datetime(df[col], errors="coerce")
                df[col] = dt.dt.date

    rows = df.to_dict("records")
    constraint_name = f"{label.lower()}_{unique_key}_unique"

    def _ensure_constraints(tx):
        tx.run(
            f"""
            CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
            FOR (n:`{label}`)
            REQUIRE n.{unique_key} IS UNIQUE
            """)

    def _upsert_batch(tx, batch_rows):
        tx.run(
            f"""
            UNWIND $rows AS row
            MERGE (n:`{label}` {{{unique_key}: row.{unique_key}}})
            SET n += row
            """,
            rows=batch_rows)

    with driver.session() as session:
        session.execute_write(_ensure_constraints)
        for i in range(0, len(rows), batch):
            session.execute_write(_upsert_batch, rows[i : i + batch])
    return len(rows)


def load_address_csv(driver, path: str=ADDRESS_PATH, batch: int=5000) -> int:
    return load_csv_upsert(
        driver,
        label="Address",
        path=path,
        dtype={
            "node_id": "Int64",
            "address": str,
            "name": str,
            "countries": str,
            "country_codes": str,
            "sourceID": str,
            "valid_until": str,
            "note": str
        }
    )


def load_entity_csv(driver, path: str = ENTITY_PATH, batch: int = 5000) -> int:
    return load_csv_upsert(
        driver,
        label="Entity",
        path=path,
        dtype={
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
            "note": str,
            "incorporation_date": str,
            "inactivation_date": str,
            "struck_off_date": str,
            "dorm_date": str
        },
        date_cols=[
            "incorporation_date",
            "inactivation_date",
            "struck_off_date",
            "dorm_date"
        ]
    )


def load_intermediary_csv(driver, path: str = INTERMEDIARY_PATH, batch: int = 5000) -> int:
    dtype = {
        "node_id": "Int64",
        "name": str,
        "status": str,
        "internal_id": str,
        "address": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str,
    }
    return load_csv_upsert(
        driver,
        label="Intermediary",
        path=path,
        dtype={
            "node_id": "Int64",
            "name": str,
            "status": str,
            "internal_id": str,
            "address": str,
            "countries": str,
            "country_codes": str,
            "sourceID": str,
            "valid_until": str,
            "note": str,
        }
    )


def load_officer_csv(driver, path: str=OFFICER_PATH, batch: int=5000) -> int:
    return load_csv_upsert(
        driver,
        label="Officer",
        path=path,
        dtype={
            "node_id": "Int64",
            "name": str,
            "countries": str,
            "country_codes": str,
            "sourceID": str,
            "valid_until": str,
            "note": str,
        }
    )





try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    print(f"Adresses added: {load_address_csv(driver)}")
    print(f"Entities added: {load_entity_csv(driver)}")
    print(f"Intermediaries added: {load_intermediary_csv(driver)}")
    print(f"Officers added: {load_officer_csv(driver)}")

except Exception as e:
    print(f"Failed to connect to Neo4j at {NEO4J_URI}:\n{e}")
    sys.exit(1)
finally:
    if 'driver' in locals():
        driver.close()