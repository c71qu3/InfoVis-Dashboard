from neo4j import GraphDatabase
import os
import sys
import pandas as pd
import hashlib


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

DATAPATH = "/app/data/{}.csv"
ADDRESS_PATH = DATAPATH.format("Address")
ENTITY_PATH = DATAPATH.format("Entity")
INTERMEDIARY_PATH = DATAPATH.format("Intermediary")
OFFICER_PATH = DATAPATH.format("Officer")
EDGES_PATH = DATAPATH.format("Edges")


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
            FOR (n:Node)
            REQUIRE n.{unique_key} IS UNIQUE
            """)

    def _upsert_batch(tx, batch_rows):
        tx.run(
            f"""
            UNWIND $rows AS row
            MERGE (n:Node:`{label}` {{{unique_key}: row.{unique_key}}})
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


def load_edges_csv(driver, path: str = EDGES_PATH, batch: int = 5000) -> int:
    df = pd.read_csv(
        path,
        dtype={
            "node_id_start": "Int64",
            "node_id_end": "Int64",
            "rel_type": str,
            "link": str,
            "status": str,
            "sourceID": str,
            "start_date": str,
            "end_date": str,
        },
        encoding="utf-8",
        low_memory=False)
    df = df.where(pd.notnull(df), None)
    df = df[df["node_id_start"].notnull() & df["node_id_end"].notnull() & df["rel_type"].notnull()]

    for c in ["start_date", "end_date"]:
        if c in df.columns:
            dt = pd.to_datetime(df[c], errors="coerce")
            df[c] = dt.dt.date

    def _edge_id(row) -> str:
        raw = "|".join([
            str(row.get("node_id_start") or ""),
            str(row.get("rel_type") or ""),
            str(row.get("node_id_end") or ""),
            str(row.get("link") or ""),
            str(row.get("start_date") or ""),
            str(row.get("end_date") or ""),
            str(row.get("sourceID") or ""),
        ])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    df["edge_id"] = df.apply(_edge_id, axis=1)

    total = 0
    with driver.session() as session:
        for rel_type, grp in df.groupby("rel_type", dropna=True):
            rel_type_str = str(rel_type)

            safe_name = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in rel_type_str).lower()
            constraint_name = f"rel_{safe_name}_edge_id_unique"

            def _ensure_rel_constraint(tx):
                tx.run(
                    f"""
                    CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                    FOR ()-[r:`{rel_type_str}`]-()
                    REQUIRE r.edge_id IS UNIQUE
                    """)

            def _merge_batch(tx, batch_rows) -> int:
                res = tx.run(
                    f"""
                    UNWIND $rows AS row
                    MATCH (a:Node {{node_id: row.node_id_start}})
                    MATCH (b:Node {{node_id: row.node_id_end}})
                    MERGE (a)-[r:`{rel_type_str}` {{edge_id: row.edge_id}}]->(b)
                    SET r += row
                    """,
                    rows=batch_rows)
                return res.consume().counters.relationships_created

            session.execute_write(_ensure_rel_constraint)
            rows = grp.to_dict("records")
            for i in range(0, len(rows), batch):
                total += session.execute_write(_merge_batch, rows[i : i + batch])
    return total


try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    print(f"Adresses added: {load_address_csv(driver)}")
    print(f"Entities added: {load_entity_csv(driver)}")
    print(f"Intermediaries added: {load_intermediary_csv(driver)}")
    print(f"Officers added: {load_officer_csv(driver)}")
    print(f"Edges added: {load_edges_csv(driver)}")

except Exception as e:
    print(f"Failed to connect to Neo4j at {NEO4J_URI}:\n{e}")
    sys.exit(1)
finally:
    if 'driver' in locals():
        driver.close()