#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import glob
import zipfile
import os

ZIPFILE = "data/*.zip"
DATAPATH = "data/raw/"
SELECTED_LEAKS = ["Panama Papers"]

os.makedirs(DATAPATH, exist_ok=True)


# load csv data
for zf_path in glob.glob(ZIPFILE):
    with zipfile.ZipFile(zf_path, 'r') as zf:
        for member in zf.namelist():
            if member.lower().endswith(".csv"):
                zf.extract(member, DATAPATH)

nodes = {}

# address nodes
nodes["Address"] = pd.read_csv(
    DATAPATH + "nodes-addresses.csv",
    dtype={
        "node_id": "Int64",
        "address": str,
        "name": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str
    },
    encoding="utf-8",
    low_memory=False
)

# entity nodes
nodes["Entity"] = pd.read_csv(
    DATAPATH + "nodes-entities.csv",
    dtype={
        "node_id": "Int64",
        "name": str,
        "original_name": str,
        "former_name": str,
        "jurisdiction": str,
        "jurisdiction_description": str,
        "company_type": str,
        "address": str,
        "internal_id": str,
        "status": str,
        "service_provider": str,
        "ibcRUC": str,
        "country_codes": str,
        "countries": str,
        "sourceID": str,
        "valid_until": str,
        "note": str,
    },
    parse_dates=[
        "incorporation_date", 
        "inactivation_date", 
        "struck_off_date", 
        "dorm_date"
    ],
    date_format=r"%d-%b-%Y",
    dayfirst=True,
    encoding="utf-8",
    low_memory=False
)

# intermediary nodes
nodes["Intermediary"] = pd.read_csv(
    DATAPATH + "nodes-intermediaries.csv",
    dtype={
        "node_id": "Int64",
        "name": str,
        "status": str,
        "internal_id": str,
        "address": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str
    },
    encoding="utf-8",
    low_memory=False
)

# officer nodes
nodes["Officer"] = pd.read_csv(
    DATAPATH + "nodes-officers.csv",
    dtype={
        "node_id": "Int64",
        "name": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str,
    },
    encoding="utf-8",
    low_memory=False
)

# other nodes
nodes["Other"] = pd.read_csv(
    DATAPATH + "nodes-others.csv",
    dtype={
        "node_id": "Int64",
        "name": str,
        "type": str,
        "jurisdiction": str,
        "jurisdiction_description": str,
        "countries": str,
        "country_codes": str,
        "sourceID": str,
        "valid_until": str,
        "note": str,
    },
    parse_dates=[
        "incorporation_date",
        "struck_off_date",
        "closed_date"
    ],
    date_format=r"%d-%b-%Y",
    dayfirst=True,
    encoding="utf-8",
    low_memory=False
)

# edges between nodes
edges = pd.read_csv(
    DATAPATH + "relationships.csv",
    dtype={
        "node_id_start": "Int64",
        "node_id_end": "Int64",
        "rel_type": str,
        "link": str,
        "status": str,
        "sourceID": str,
    },
    parse_dates=[
        "start_date",
        "end_date"
    ],
    date_format=r"%d-%b-%Y",
    dayfirst=True,
    encoding="utf-8"
)


# filter for `SELECTED_LEAKS` and save results
def filter_by_leaks(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["sourceID"].isin(SELECTED_LEAKS)]


date_columns = {
    "Entity": ["incorporation_date", "inactivation_date", "struck_off_date", "dorm_date"],
    "Other": ["incorporation_date", "struck_off_date", "closed_date"],
    "Edges": ["start_date", "end_date"]}


def format_dates_iso(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            dt = pd.to_datetime(df[c], format="%d-%b-%Y", errors="coerce", dayfirst=True)
            df[c] = dt.dt.strftime("%Y-%m-%d")
    return df


for n in nodes:
    nodes[n] = filter_by_leaks(nodes[n])
    nodes[n] = format_dates_iso(nodes[n], date_columns.get(n, []))
    if nodes[n].shape[0] > 0:
        nodes[n].to_csv(f"data/{n}.csv", index=False)

edges = filter_by_leaks(edges)
edges = format_dates_iso(edges, date_columns["Edges"])
edges.to_csv(f"data/Edges.csv", index=False)


# remove row csv files
for f in glob.glob("data/raw/*"):
    if os.path.isfile(f):
        os.remove(f)