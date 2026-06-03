from flask import Flask, render_template, jsonify, request
import re
import os
import pandas as pd
from pathlib import Path

from src.world_bank import (
    get_all_countries,
    fetch_latest_all,
    fetch_year_all,
    get_available_years,
    iso3_to_iso2,
    iso3_to_name,
)

from src.graph_layer import (
    get_country_graph,
    get_jurisdictions
)


DATA_DIR = Path("static/data")


app = Flask(__name__, static_folder="static", static_url_path="/static")


def count_connections():
    counts = {}
    csv_files = ["Address.csv", "Entity.csv", "Intermediary.csv", "Officer.csv"]

    for fname in csv_files:
        filepath = DATA_DIR / fname
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping")
            continue
        df = pd.read_csv(filepath, low_memory=False)
        if "country_codes" in df.columns:
            col = "country_codes"
        elif "countries" in df.columns:
            col = "countries"
        else:
            continue

        for codes in df[col].dropna():
            if isinstance(codes, str):
                for code in re.split(r"[;,]\s*", codes.strip()):
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


def clamp_arg(name, default, lo, hi):
    try:
        return max(lo, min(hi, int(request.args.get(name, default))))
    except (TypeError, ValueError):
        return default


@app.route("/api/graph/<iso3>")
def api_graph(iso3):
    iso3 = iso3.upper()

    agents = clamp_arg("agents", 4, 1, 10)
    per_agent = clamp_arg("per", 9, 2, 25)
    per_company = clamp_arg("officers", 2, 1, 6)

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
    limit = clamp_arg("limit", 40 if not focus else 14, 5, 120 if not focus else 40)

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