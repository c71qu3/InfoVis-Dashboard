import os
import re
from neo4j import GraphDatabase

from src.world_bank import iso3_to_name


# Neo4j connection config
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

NODE_TYPES = ("Entity", "Officer", "Intermediary", "Address")


# Lazy, module-level driver
_driver = None
_driver_attempted = False


def _get_driver():
    global _driver, _driver_attempted
    if _driver_attempted:
        return _driver

    _driver_attempted = True
    try:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    except Exception as e:
        print(f"Could not init Neo4j driver: {e}")
        _driver = None
    return _driver


def node_type(labels):
    for lbl in labels:
        if lbl in NODE_TYPES:
            return lbl
    return "Node"


# Agent-centred: anchor on intermediaries; return all edges among the selected node set.
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


def get_country_graph(iso3: str, agents: int = 4, per_agent: int = 9, per_company: int = 2):
    iso3 = (iso3 or "").strip().upper()
    driver = _get_driver()

    if driver is None or not re.match(r"^[A-Z]{3}$", iso3):
        return {"nodes": [], "links": []}

    nodes, links, seen = {}, [], set()
    try:
        with driver.session() as session:
            for rec in session.run(
                GRAPH_QUERY,
                iso3=iso3,
                agents=agents,
                per_agent=per_agent,
                per_company=per_company,
            ):
                for nid, labels, name in (
                    (rec["s_id"], rec["s_labels"], rec["s_name"]),
                    (rec["t_id"], rec["t_labels"], rec["t_name"]),
                ):
                    if nid not in nodes:
                        nodes[nid] = {
                            "id": nid,
                            "label": name or str(nid),
                            "type": node_type(labels),
                            "degree": 0,
                        }

                if rec["eid"] not in seen:
                    seen.add(rec["eid"])
                    links.append(
                        {"source": rec["r_start"], "target": rec["r_end"], "rel": rec["rel"]}
                    )
    except Exception as e:
        print(f"Graph query failed for {iso3}: {e}")
        return {"nodes": [], "links": [], "error": "graph_unavailable"}

    for l in links:
        for end in ("source", "target"):
            if l[end] in nodes:
                nodes[l[end]]["degree"] += 1

    return {"nodes": list(nodes.values()), "links": links}


# Jurisdiction-flow view
JURIS_QUERY = """
MATCH (a:Node)-[r]->(b:Node)
WHERE a.country_codes IS NOT NULL AND b.country_codes IS NOT NULL
RETURN a.country_codes AS ca, b.country_codes AS cb, count(*) AS w
"""

_juris_cache = None  # (pairs, totals)


def primary_code(codes: str):
    for part in re.split(r"[;,]\s*", (codes or "").strip()):
        part = part.strip().upper()
        if re.match(r"^[A-Z]{3}$", part):
            return part
    return None


def compute_jurisdiction_flows(driver):
    pairs, totals = {}, {}
    with driver.session() as session:
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


def get_jurisdictions(focus=None, limit: int = 40):
    """
    Returns:
      { nodes: [{id,label,degree,focus}], links: [{source,target,weight}], focus }
    """
    global _juris_cache

    driver = _get_driver()
    if driver is None:
        return {"nodes": [], "links": []}

    focus = (focus or "").strip().upper()
    focus = focus if re.match(r"^[A-Z]{3}$", focus) else None

    try:
        if _juris_cache is None:
            _juris_cache = compute_jurisdiction_flows(driver)
    except Exception as e:
        print(f"Jurisdiction query failed: {e}")
        return {"nodes": [], "links": [], "error": "graph_unavailable"}

    pairs, totals = _juris_cache

    if focus:
        chosen = sorted(
            ((k, w) for k, w in pairs.items() if focus in k),
            key=lambda kv: -kv[1],
        )[: max(5, int(limit))]
    else:
        chosen = sorted(pairs.items(), key=lambda kv: -kv[1])[: max(5, int(limit))]

    used = set()
    links = []
    for (x, y), w in chosen:
        links.append({"source": x, "target": y, "weight": w})
        used.update((x, y))

    nodes = [
        {
            "id": c,
            "label": iso3_to_name.get(c, c),
            "degree": totals.get(c, 0),
            "focus": c == focus,
        }
        for c in used
    ]
    return {"nodes": nodes, "links": links, "focus": focus}