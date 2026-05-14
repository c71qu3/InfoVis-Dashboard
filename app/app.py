from flask import Flask, render_template, jsonify, request
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from pathlib import Path
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
try:
    resp = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=300")
    data = resp.json()
    if isinstance(data, list) and len(data) > 1:
        for c in data[1]:
            iso2 = c.get("iso2Code")
            iso3 = c.get("id")  # World Bank uses 3-letter code as id
            if iso2 and iso3 and re.match(r'^[A-Z]{2}$', iso2) and c.get("capitalCity"):
                iso3_to_iso2[iso3] = iso2
except Exception:
    pass

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