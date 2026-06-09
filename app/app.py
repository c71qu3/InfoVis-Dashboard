from flask import Flask, render_template, jsonify, request
import json
import re
import urllib.parse
import urllib.request
from pathlib import Path

from src.app_utils import (
    count_connections_iso3,
    count_outgoing_crossborder_edges_iso3,
    clamp_arg
)

from src.world_bank import (
    get_all_countries,
    fetch_latest_all,
    fetch_year_all,
    get_available_years,
)

from src.graph_layer import (
    get_country_graph,
    get_jurisdictions,
    get_country_entities,
    get_country_intermediaries,
    get_country_nodes,
    get_entity_graph,
    get_intermediary_graph,
    get_officer_graph,
)


DATA_DIR = Path("static/data")


app = Flask(__name__, static_folder="static", static_url_path="/static")


# pre-compute:
# - raw appearance counts (ISO3 -> count)
# - outgoing cross-border edges (ISO3 -> count) for clickability
connection_counts = count_connections_iso3(DATA_DIR)
outgoing_counts = count_outgoing_crossborder_edges_iso3(DATA_DIR)
print(f"Loaded {len(connection_counts)} ISO3 codes with connection counts")
print(f"Loaded {len(outgoing_counts)} ISO3 codes with intermediary-linked entity edges")


@app.route("/api/connections")
def api_connections():
    return jsonify(connection_counts)


@app.route("/api/outgoing_connections")
def api_outgoing_connections():
    return jsonify(outgoing_counts)


@app.route("/api/graph/<iso3>")
def api_graph(iso3):
    iso3 = iso3.upper()

    agents = clamp_arg(request.args, "agents", 4, 1, 10)
    per_agent = clamp_arg(request.args, "per", 9, 2, 25)
    per_company = clamp_arg(request.args, "officers", 2, 1, 6)

    result = get_country_graph(
        iso3=iso3,
        agents=agents,
        per_agent=per_agent,
        per_company=per_company,
    )
    return jsonify(result)


@app.route("/api/jurisdictions")
def api_jurisdictions():
    focus = request.args.get("focus", "").strip().upper()
    focus = focus if re.match(r"^[A-Z]{3}$", focus) else None
    limit = clamp_arg(request.args, "limit", 40 if not focus else 14, 5, 120 if not focus else 40)

    result = get_jurisdictions(focus=focus, limit=limit)
    return jsonify(result)


@app.route("/api/entity_graph/<entity_id>")
def api_entity_graph(entity_id):
    # Per-node fan-out limits
    intermediaries = clamp_arg(request.args, "intermediaries", 6, 1, 25)
    per_i = clamp_arg(request.args, "per_i", 6, 1, 25)
    officers = clamp_arg(request.args, "officers", 2, 0, 10)

    # Subgraph traversal limits
    depth = clamp_arg(request.args, "depth", 4, 0, 4)
    max_nodes = clamp_arg(request.args, "max_nodes", 5000, 1, 20000)
    max_rels = clamp_arg(request.args, "max_rels", 20000, 0, 60000)

    result = get_entity_graph(
        entity_id=entity_id,
        max_depth=depth,
        max_nodes=max_nodes,
        max_rels=max_rels,
        intermediaries=intermediaries,
        entities_per_intermediary=per_i,
        officers_per_entity=officers,
    )
    return jsonify(result)


@app.route("/api/intermediary_graph/<intermediary_id>")
def api_intermediary_graph(intermediary_id):
    # Per-node fan-out limits
    iso3 = (request.args.get("iso3") or "").strip().upper() or None
    entities = clamp_arg(request.args, "entities", 12, 1, 80)
    officers = clamp_arg(request.args, "officers", 2, 0, 10)

    # Subgraph traversal limits
    depth = clamp_arg(request.args, "depth", 4, 0, 4)
    max_nodes = clamp_arg(request.args, "max_nodes", 5000, 1, 20000)
    max_rels = clamp_arg(request.args, "max_rels", 20000, 0, 60000)

    result = get_intermediary_graph(
        intermediary_id=intermediary_id,
        max_depth=depth,
        max_nodes=max_nodes,
        max_rels=max_rels,
        iso3=iso3,
        entities=entities,
        officers_per_entity=officers,
    )
    return jsonify(result)


@app.route("/api/officer_graph/<officer_id>")
def api_officer_graph(officer_id):
    # Per-node fan-out limits
    entities = clamp_arg(request.args, "entities", 12, 1, 80)
    intermediaries = clamp_arg(request.args, "intermediaries", 6, 0, 25)

    # Subgraph traversal limits
    depth = clamp_arg(request.args, "depth", 4, 0, 4)
    max_nodes = clamp_arg(request.args, "max_nodes", 5000, 1, 20000)
    max_rels = clamp_arg(request.args, "max_rels", 20000, 0, 60000)

    result = get_officer_graph(
        officer_id=officer_id,
        max_depth=depth,
        max_nodes=max_nodes,
        max_rels=max_rels,
        entities=entities,
        intermediaries_per_entity=intermediaries,
    )
    return jsonify(result)


@app.route("/api/entities/<iso3>")
def api_entities(iso3):
    iso3 = (iso3 or "").strip().upper()

    limit = clamp_arg(request.args, "limit", 200, 1, 500)
    offset = clamp_arg(request.args, "offset", 0, 0, 20000)
    q = (request.args.get("q") or "").strip() or None

    result = get_country_entities(iso3=iso3, q=q, limit=limit, offset=offset)
    return jsonify(result)


@app.route("/api/intermediaries/<iso3>")
def api_intermediaries(iso3):
    iso3 = (iso3 or "").strip().upper()

    limit = clamp_arg(request.args, "limit", 200, 1, 500)
    offset = clamp_arg(request.args, "offset", 0, 0, 20000)
    q = (request.args.get("q") or "").strip() or None
    node_type = (request.args.get("type") or "all").strip().lower()

    result = get_country_nodes(iso3=iso3, q=q, node_type=node_type, limit=limit, offset=offset)
    return jsonify(result)



@app.route("/api/offshore_node/<node_id>")
def api_offshore_node(node_id):
    """Fetch a tiny bit of public metadata about a node from ICIJ Offshore Leaks.

    Uses the Reconciliation "extend" API (proxied server-side to avoid browser CORS
    issues):
      https://offshoreleaks.icij.org/api/v1/reconcile?extend=...

    We currently request:
      - country_codes
      - jurisdiction

    Note: the response also typically includes a "schema" field.
    """

    node_id = (node_id or "").strip()
    if not re.match(r"^\d+$", node_id):
        return jsonify({"error": "Invalid node id"}), 400

    extend_obj = {
        "ids": [int(node_id)],
        "properties": [
            {"id": "country_codes"},
            {"id": "jurisdiction"},
        ],
    }

    extend_qs = urllib.parse.quote(json.dumps(extend_obj, separators=(",", ":")))
    url = f"https://offshoreleaks.icij.org/api/v1/reconcile?extend={extend_qs}"

    try:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "InfoVis-Dashboard/0.1",
            },
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload) if payload else {}
    except Exception as e:
        return jsonify({"error": "Failed to fetch Offshore Leaks details", "detail": str(e)}), 502

    row = (data.get("rows") or {}).get(node_id) or {}
    meta = data.get("meta") or []
    return jsonify({"id": node_id, "row": row, "meta": meta})


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