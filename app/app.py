from flask import Flask, render_template, jsonify, request
import requests
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
app = Flask(__name__, static_folder='static', static_url_path='/static')

# World Bank indicators
WB_INDICATORS = {
    "GDP (current US$)": "NY.GDP.MKTP.CD",
    "GDP per capita (US$)": "NY.GDP.PCAP.CD",
    "Population": "SP.POP.TOTL",
    "Life expectancy (years)": "SP.DYN.LE00.IN",
    "Control of Corruption": "GOV_WGI_CC.EST",
    "Rule of Law": "GOV_WGI_RL.EST",
    "Unemployment (%)": "SL.UEM.TOTL.ZS",
    "Inflation (annual %)": "FP.CPI.TOTL.ZG",
}

latest_cache = {}
year_cache = {}

def get_all_countries():
    url = "https://api.worldbank.org/v2/country?format=json&per_page=300"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        countries = []
        if isinstance(data, list) and len(data) > 1:
            for c in data[1]:
                iso2 = c.get("iso2Code")
                if iso2 and re.match(r'^[A-Z]{2}$', iso2) and c.get("capitalCity"):
                    countries.append({"name": c["name"], "iso2": iso2})
        return sorted(countries, key=lambda x: x["name"])
    except Exception as e:
        print(f"Error fetching countries: {e}")
        return []

def fetch_latest_for_indicator(iso2, label, code):
    url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json&per_page=100"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if isinstance(data, list) and len(data) > 1 and data[1]:
            for entry in data[1]:
                val = entry.get("value")
                year = entry.get("date")
                if val is not None and str(val).strip() not in ("", "null"):
                    return {label: {"value": val, "year": year}}
        return {label: {"value": None, "year": None}}
    except Exception as e:
        print(f"Error for {label} {iso2}: {e}")
        return {label: {"value": None, "year": None}}

def fetch_latest_all(iso2):
    if iso2 in latest_cache:
        return latest_cache[iso2]
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_latest_for_indicator, iso2, label, code): label 
                   for label, code in WB_INDICATORS.items()}
        for future in as_completed(futures):
            results.update(future.result())
    latest_cache[iso2] = results
    return results

def fetch_year_for_indicator(iso2, year, label, code):
    url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json&per_page=100"
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        if isinstance(data, list) and len(data) > 1:
            for entry in data[1]:
                if entry.get("date") == str(year):
                    val = entry.get("value")
                    return {label: {"value": val, "year": year}}
        return {label: {"value": None, "year": year}}
    except Exception:
        return {label: {"value": None, "year": year}}

def fetch_year_all(iso2, year):
    if iso2 in year_cache and year in year_cache[iso2]:
        return year_cache[iso2][year]
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_year_for_indicator, iso2, year, label, code): label
                   for label, code in WB_INDICATORS.items()}
        for future in as_completed(futures):
            results.update(future.result())
    if iso2 not in year_cache:
        year_cache[iso2] = {}
    year_cache[iso2][year] = results
    return results

def get_available_years(iso2):
    code = "NY.GDP.MKTP.CD"
    url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json&per_page=100"
    years = set()
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        if isinstance(data, list) and len(data) > 1:
            for entry in data[1]:
                y = entry.get("date")
                if y and y.isdigit():
                    years.add(int(y))
    except Exception:
        pass
    return sorted(years, reverse=True)

# after get_all_countries, create iso3_to_iso2 mapping
iso3_to_iso2 = {}
iso3_to_name = {}
try:
    resp = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=300")
    data = resp.json()
    if isinstance(data, list) and len(data) > 1:
        for c in data[1]:
            iso2 = c.get("iso2Code")
            iso3 = c.get("id")  # World Bank uses 3-letter code as id
            if iso2 and iso3 and re.match(r'^[A-Z]{2}$', iso2) and c.get("capitalCity"):
                iso3_to_iso2[iso3] = iso2
            if iso3 and c.get("name"):
                iso3_to_name[iso3] = c["name"]
except Exception:
    pass

# Neo4j driver for the knowledge-graph panel
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")
try:
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
except Exception as e:
    print(f"Could not init Neo4j driver: {e}")
    neo4j_driver = None

NODE_TYPES = ("Entity", "Officer", "Intermediary", "Address")

def node_type(labels):
    for lbl in labels:
        if lbl in NODE_TYPES:
            return lbl
    return "Node"

DATA_DIR = Path("static/data")

def count_connections():
    counts = {}
    csv_files = ["Address.csv", "Entity.csv", "Intermediary.csv", "Officer.csv"]
    
    for fname in csv_files:
        filepath = DATA_DIR / fname
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping")
            continue
        df = pd.read_csv(filepath, low_memory=False)
        if 'country_codes' in df.columns:
            col = 'country_codes'
        elif 'countries' in df.columns:
            col = 'countries'
        else:
            continue
        
        for codes in df[col].dropna():
            if isinstance(codes, str):
                for code in re.split(r'[;,]\s*', codes.strip()):
                    code = code.strip().upper()
                    # Convert ISO3 → ISO2 if possible
                    if len(code) == 3 and code in iso3_to_iso2:
                        code = iso3_to_iso2[code]
                    # Only count if it's a 2-letter ISO2 code
                    if len(code) == 2:
                        counts[code] = counts.get(code, 0) + 1
    return counts

# pre‑compute counts
connection_counts = count_connections()
print(f"Loaded {len(connection_counts)} country codes with connection counts")


@app.route("/api/connections")
def api_connections():
    return jsonify(connection_counts)

# Agent-centred: anchor on the intermediaries that set up the most of a country's
# companies, pull those companies and a few of their officers, then return all edges
# among the node set. Companies sharing an agent become connected through it.
GRAPH_QUERY = """
MATCH (i:Intermediary)-[:intermediary_of]-(e0:Entity)
WHERE e0.country_codes CONTAINS $iso3
WITH i, count(DISTINCT e0) AS ents
ORDER BY ents DESC
LIMIT $agents
CALL {
    WITH i
    MATCH (i)-[:intermediary_of]-(e:Entity)
    WHERE e.country_codes CONTAINS $iso3
    RETURN e LIMIT $per_agent
}
CALL {
    WITH e
    OPTIONAL MATCH (e)-[:officer_of]-(o:Officer)
    RETURN o LIMIT $per_company
}
WITH collect(DISTINCT i) + collect(DISTINCT e) + collect(DISTINCT o) AS ns
UNWIND ns AS n
WITH n WHERE n IS NOT NULL
WITH collect(DISTINCT n) AS nodes
UNWIND nodes AS x
MATCH (x)-[r]-(y:Node)
WHERE y IN nodes
RETURN DISTINCT elementId(r) AS eid, type(r) AS rel,
       startNode(r).node_id AS r_start, endNode(r).node_id AS r_end,
       startNode(r).node_id AS s_id, labels(startNode(r)) AS s_labels, startNode(r).name AS s_name,
       endNode(r).node_id AS t_id, labels(endNode(r)) AS t_labels, endNode(r).name AS t_name
"""

def clamp_arg(name, default, lo, hi):
    try:
        return max(lo, min(hi, int(request.args.get(name, default))))
    except (TypeError, ValueError):
        return default

@app.route("/api/graph/<iso3>")
def api_graph(iso3):
    iso3 = iso3.upper()
    if neo4j_driver is None or not re.match(r'^[A-Z]{3}$', iso3):
        return jsonify({"nodes": [], "links": []})

    agents = clamp_arg("agents", 4, 1, 10)
    per_agent = clamp_arg("per", 9, 2, 25)
    per_company = clamp_arg("officers", 2, 1, 6)

    nodes, links, seen = {}, [], set()
    try:
        with neo4j_driver.session() as session:
            for rec in session.run(GRAPH_QUERY, iso3=iso3, agents=agents,
                                    per_agent=per_agent, per_company=per_company):
                for nid, labels, name in (
                    (rec["s_id"], rec["s_labels"], rec["s_name"]),
                    (rec["t_id"], rec["t_labels"], rec["t_name"]),
                ):
                    if nid not in nodes:
                        nodes[nid] = {"id": nid, "label": name or str(nid),
                                      "type": node_type(labels), "degree": 0}
                if rec["eid"] not in seen:
                    seen.add(rec["eid"])
                    links.append({"source": rec["r_start"], "target": rec["r_end"], "rel": rec["rel"]})
    except Exception as e:
        print(f"Graph query failed for {iso3}: {e}")
        return jsonify({"nodes": [], "links": [], "error": "graph_unavailable"})

    for l in links:
        for end in ("source", "target"):
            if l[end] in nodes:
                nodes[l[end]]["degree"] += 1

    return jsonify({"nodes": list(nodes.values()), "links": links})

# Jurisdiction-flow view: countries as nodes, cross-border relationships as edges.
JURIS_QUERY = """
MATCH (a:Node)-[r]->(b:Node)
WHERE a.country_codes IS NOT NULL AND b.country_codes IS NOT NULL
RETURN a.country_codes AS ca, b.country_codes AS cb, count(*) AS w
"""

juris_cache = None

def primary_code(codes):
    for part in re.split(r'[;,]\s*', codes.strip()):
        part = part.strip().upper()
        if re.match(r'^[A-Z]{3}$', part):
            return part
    return None

def compute_jurisdiction_flows():
    pairs, totals = {}, {}
    with neo4j_driver.session() as session:
        for rec in session.run(JURIS_QUERY):
            x, y = primary_code(rec["ca"]), primary_code(rec["cb"])
            if not x or not y or x == y:
                continue
            w = rec["w"]
            key = tuple(sorted((x, y)))
            pairs[key] = pairs.get(key, 0) + w
            totals[x] = totals.get(x, 0) + w
            totals[y] = totals.get(y, 0) + w
    return pairs, totals

@app.route("/api/jurisdictions")
def api_jurisdictions():
    global juris_cache
    if neo4j_driver is None:
        return jsonify({"nodes": [], "links": []})
    focus = request.args.get("focus", "").strip().upper()
    focus = focus if re.match(r'^[A-Z]{3}$', focus) else None
    try:
        if juris_cache is None:
            juris_cache = compute_jurisdiction_flows()
    except Exception as e:
        print(f"Jurisdiction query failed: {e}")
        return jsonify({"nodes": [], "links": [], "error": "graph_unavailable"})

    pairs, totals = juris_cache
    if focus:
        chosen = sorted(((k, w) for k, w in pairs.items() if focus in k), key=lambda kv: -kv[1])
        chosen = chosen[:clamp_arg("limit", 14, 5, 40)]
    else:
        chosen = sorted(pairs.items(), key=lambda kv: -kv[1])[:clamp_arg("limit", 40, 5, 120)]

    used = set()
    links = []
    for (x, y), w in chosen:
        links.append({"source": x, "target": y, "weight": w})
        used.update((x, y))
    nodes = [{"id": c, "label": iso3_to_name.get(c, c), "degree": totals.get(c, 0),
              "focus": c == focus} for c in used]
    return jsonify({"nodes": nodes, "links": links, "focus": focus})

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/countries")
def api_countries():
    return jsonify(get_all_countries())

@app.route("/api/indicators/<iso2>")
def api_indicators(iso2):
    mode = request.args.get("mode", "latest")
    if mode == "latest":
        data = fetch_latest_all(iso2)
    else:
        try:
            year = int(mode)
            data = fetch_year_all(iso2, year)
        except ValueError:
            return jsonify({"error": "Invalid mode"}), 400
    return jsonify(data)

@app.route("/api/years/<iso2>")
def api_years(iso2):
    years = get_available_years(iso2)
    return jsonify(years)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)