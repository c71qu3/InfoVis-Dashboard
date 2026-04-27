from flask import Flask, render_template
import os
import re
from neo4j import GraphDatabase


app = Flask(__name__)


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")


driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def get_entity_countries():
    """
    Returns a sorted list of unique country names derived from Entity.countries.
    """
    query = """
    MATCH (e:Entity)
    WITH toString(e.countries) AS countries
    WHERE countries IS NOT NULL
        AND trim(countries) <> ""
        AND countries <> "NaN"
    RETURN DISTINCT countries AS countries
    """

    countries_set = set()
    with driver.session() as session:
        rows = session.run(query)
        for r in rows:
            raw = r["countries"] or ""
            parts = re.split(r"\s*[;,]\s*", raw.strip())
            for p in parts:
                p = (p or "").strip()
                if p:
                    countries_set.add(p)

    return sorted(countries_set, key=str.casefold)


@app.route('/')
def index():
    countries = get_entity_countries()
    return render_template(
        "index.html",
        countries=countries)


def main():
    app.run(
        host="0.0.0.0",
        debug=True,
        port=5000)


if __name__ == "__main__":
    main()
