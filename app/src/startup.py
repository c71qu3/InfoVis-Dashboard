import os
import sys
import time
import subprocess
from neo4j import GraphDatabase


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

WAIT_SECONDS = int(os.environ.get("NEO4J_WAIT_SECONDS", "90"))


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


def main():
    wait_for_neo4j()

    subprocess.run([sys.executable, "src/upsert.py"], check=True)
    os.execvp(sys.executable, [sys.executable, "app.py"])


if __name__ == "__main__":
    main()