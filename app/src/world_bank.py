import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


_session = requests.Session()
_executor = ThreadPoolExecutor(max_workers=8)

# map the indicator names
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


# the try/except structure referenced to AI

latest_cache = {}
year_cache = {}


def get_all_countries():
    url = "https://api.worldbank.org/v2/country?format=json&per_page=300"
    try:
        resp = _session.get(url, timeout=10)
        data = resp.json()
        countries = []
        if isinstance(data, list) and len(data) > 1:
            for c in data[1]:
                iso2 = c.get("iso2Code")
                if iso2 and re.match(r"^[A-Z]{2}$", iso2) and c.get("capitalCity"):
                    countries.append({"name": c["name"], "iso2": iso2})
        return sorted(countries, key=lambda x: x["name"])
    except Exception as e:
        print(f"Error fetching countries: {e}")
        return []


def fetch_latest_for_indicator(iso2, label, code): # to reduce nan values on front end
    url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json&per_page=100"
    try:
        r = _session.get(url, timeout=10)
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
    futures = {
        _executor.submit(fetch_latest_for_indicator, iso2, label, code): label
        for label, code in WB_INDICATORS.items()
    }
    for future in as_completed(futures):
        results.update(future.result())
    latest_cache[iso2] = results
    return results


def fetch_year_for_indicator(iso2, year, label, code):
    url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json&per_page=100"
    try:
        r = _session.get(url, timeout=5)
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
    futures = {
        _executor.submit(fetch_year_for_indicator, iso2, year, label, code): label
        for label, code in WB_INDICATORS.items()
    }
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
        r = _session.get(url, timeout=5)
        data = r.json()
        if isinstance(data, list) and len(data) > 1:
            for entry in data[1]:
                y = entry.get("date")
                if y and y.isdigit():
                    years.add(int(y))
    except Exception:
        pass
    return sorted(years, reverse=True)


# ISO mappings (World Bank "id" is ISO3)
iso3_to_iso2 = {}
iso3_to_name = {}


try:
    resp = _session.get("https://api.worldbank.org/v2/country?format=json&per_page=300", timeout=10)
    data = resp.json()
    if isinstance(data, list) and len(data) > 1:
        for c in data[1]:
            iso2 = c.get("iso2Code")
            iso3 = c.get("id")
            if iso2 and iso3 and re.match(r"^[A-Z]{2}$", iso2) and c.get("capitalCity"):
                iso3_to_iso2[iso3] = iso2
            if iso3 and c.get("name"):
                iso3_to_name[iso3] = c["name"]
except Exception:
    pass