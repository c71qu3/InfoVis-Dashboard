from flask import Flask, render_template, jsonify, request
import re
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
    get_jurisdictions
)


DATA_DIR = Path("static/data")


app = Flask(__name__, static_folder="static", static_url_path="/static")


# Pre-compute:
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