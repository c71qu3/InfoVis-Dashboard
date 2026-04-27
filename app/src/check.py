from neo4j import GraphDatabase
import os
import sys


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")


try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        result = session.run("RETURN 1 AS test")
        if result.single()["test"] != 1:
            print("Query did not return expected result.")
            sys.exit(2)

        print("Successfully connected to Neo4j at", NEO4J_URI)

        address_count = session.run(
            "MATCH (a:Address) RETURN count(a) AS address_count"
        ).single()["address_count"]
        print(f"Address nodes found: {address_count}")

        entity_count = session.run(
            "MATCH (e:Entity) RETURN count(e) AS entity_count"
        ).single()["entity_count"]
        print(f"Entity nodes found: {entity_count}")

        intermediary_count = session.run(
            "MATCH (e:Intermediary) RETURN count(e) AS intermediary_count"
        ).single()["intermediary_count"]
        print(f"Intermediary nodes found: {intermediary_count}")

        officer_count = session.run(
            "MATCH (e:Officer) RETURN count(e) AS officer_count"
        ).single()["officer_count"]
        print(f"Officer nodes found: {officer_count}")

        rel_count = session.run(
            "MATCH ()-[r]->() RETURN count(r) AS rel_count"
        ).single()["rel_count"]
        print(f"Relationships found: {rel_count}")

        rel_types = session.run(
            """
            MATCH ()-[r]->()
            RETURN type(r) AS rel_type, count(r) AS cnt
            ORDER BY cnt DESC
            LIMIT 20
            """
        ).data()
        if rel_types:
            print("Top relationship types:")
            for row in rel_types:
                print(f"  {row['rel_type']}: {row['cnt']}")

        sys.exit(0)
except Exception as e:
    print(f"Failed to connect to Neo4j at {NEO4J_URI}:\n{e}")
    sys.exit(1)
finally:
    if 'driver' in locals():
        driver.close()