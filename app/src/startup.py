import os
import sys
import time
import subprocess
import glob
from neo4j import GraphDatabase


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

WAIT_SECONDS = int(os.environ.get("NEO4J_WAIT_SECONDS", "90"))

DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
FILTER_SCRIPT = os.environ.get("FILTER_SCRIPT", "/app/filter_data.py")

AUTO_PREPARE_DATA = os.environ.get("AUTO_PREPARE_DATA", "1") == "1"
FORCE_PREPARE_DATA = os.environ.get("FORCE_PREPARE_DATA", "0") == "1"


def wait_for_neo4j():
    deadline = time.time() + WAIT_SECONDS
    last_error = None

    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
            with driver.session() as s:
                s.run("RETURN 1").consume()
            driver.close()
            return
        except Exception as e:
            last_error = e
            time.sleep(2)

    raise RuntimeError(f"Neo4j not ready after {WAIT_SECONDS}s")


def _data_ready() -> bool:
    required = [
        "Address.csv",
        "Entity.csv",
        "Intermediary.csv",
        "Officer.csv",
        "Edges.csv",
    ]
    for fn in required:
        p = os.path.join(DATA_DIR, fn)
        if not (os.path.exists(p) and os.path.getsize(p) > 0):
            return False
    return True


def _prepare_data_if_needed() -> None:  # AI assisted 
    """Generate the filtered CSVs in DATA_DIR from the downloaded ZIP.

    This makes `podman-compose up --build` a single-step startup:
    if the CSVs already exist we do nothing; otherwise we run filter_data.py.
    """

    if not (AUTO_PREPARE_DATA or FORCE_PREPARE_DATA):
        return

    if _data_ready() and not FORCE_PREPARE_DATA:
        return

    if not os.path.exists(FILTER_SCRIPT):
        raise RuntimeError(
            f"Data-prep requested but {FILTER_SCRIPT} not found. "
            "(Is it mounted into the container?)"
        )

    zips = glob.glob(os.path.join(DATA_DIR, "*.zip"))
    if not zips:
        raise RuntimeError(
            f"Data-prep requested but no .zip file found in {DATA_DIR}. "
            "Download the ICIJ ZIP into ./data/ first."
        )

    print("Filtered CSVs not found (or FORCE_PREPARE_DATA=1). Running filter_data.py...")
    subprocess.run([sys.executable, FILTER_SCRIPT], check=True, cwd="/app")


def main():
    _prepare_data_if_needed()
    wait_for_neo4j()

    subprocess.run([sys.executable, "src/upsert.py"], check=True)
    os.execvp(sys.executable, [sys.executable, "app.py"])


if __name__ == "__main__":
    main()