import os
import re
from neo4j import GraphDatabase

from src.world_bank import iso3_to_name


# Neo4j connection config
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password")

NODE_TYPES = ("Entity", "Officer", "Intermediary", "Address")

# Entity-list view (for the right-side "Entities List" panel)
ENTITIES_QUERY = """
MATCH (e:Entity)
WHERE e.country_codes CONTAINS $iso3
  AND ($q IS NULL OR $q = '' OR toLower(coalesce(e.name, '')) CONTAINS toLower($q))
RETURN e.node_id AS id, e.name AS name
ORDER BY toLower(coalesce(e.name, '')) ASC, id ASC
SKIP $offset
LIMIT $limit
"""

ENTITIES_COUNT_QUERY = """
MATCH (e:Entity)
WHERE e.country_codes CONTAINS $iso3
  AND ($q IS NULL OR $q = '' OR toLower(coalesce(e.name, '')) CONTAINS toLower($q))
RETURN count(e) AS total
"""

# Intermediary-list view (for the right-side list panel)
INTERMEDIARIES_QUERY = """
MATCH (i:Intermediary)-[:intermediary_of]-(e:Entity)
WHERE e.country_codes CONTAINS $iso3
  AND ($q IS NULL OR $q = '' OR toLower(coalesce(i.name, '')) CONTAINS toLower($q))
RETURN DISTINCT i.node_id AS id, i.name AS name
ORDER BY toLower(coalesce(name, '')) ASC, id ASC
SKIP $offset
LIMIT $limit
"""

INTERMEDIARIES_COUNT_QUERY = """
MATCH (i:Intermediary)-[:intermediary_of]-(e:Entity)
WHERE e.country_codes CONTAINS $iso3
  AND ($q IS NULL OR $q = '' OR toLower(coalesce(i.name, '')) CONTAINS toLower($q))
RETURN count(DISTINCT i) AS total
"""

# Unified country node list (search + type filter for the right-side list panel).
# `$type` is one of 'all' | 'entity' | 'officer' | 'intermediary'.
# Per-type MATCH clauses; combined with UNION for 'all'.
_NODE_MATCH_CLAUSES = {
    "entity": "MATCH (n:Entity) WHERE n.country_codes CONTAINS $iso3",
    "intermediary": "MATCH (n:Intermediary)-[:intermediary_of]-(e:Entity) WHERE e.country_codes CONTAINS $iso3",
    "officer": "MATCH (n:Officer)-[:officer_of]-(e:Entity) WHERE e.country_codes CONTAINS $iso3",
}

_NODE_TYPE_EXPR = "head([l IN labels(n) WHERE l <> 'Node']) AS type"


def _build_node_match(node_type: str) -> str:
    """Cypher that binds `n` to the nodes of the requested type for a country.

    Always ends with `WITH DISTINCT n` so a name-filter WHERE can be chained after it.
    """
    if node_type in _NODE_MATCH_CLAUSES:
        return f"{_NODE_MATCH_CLAUSES[node_type]} WITH DISTINCT n"
    # 'all' — union of every supported type
    parts = [f"{clause} RETURN n" for clause in _NODE_MATCH_CLAUSES.values()]
    return "CALL {\n" + "\nUNION\n".join(parts) + "\n} WITH DISTINCT n"

# Entity-focused graph view (when clicking an entity in the list)
ENTITY_EXISTS_QUERY = """
MATCH (e:Entity)
WHERE toString(e.node_id) = toString($entity_id)
RETURN e.node_id AS id, e.name AS name
LIMIT 1
"""

ENTITY_FOCUS_GRAPH_QUERY = """
MATCH (e:Entity)
WHERE toString(e.node_id) = toString($entity_id)
CALL {
    WITH e
    OPTIONAL MATCH (e)-[:intermediary_of]-(i:Intermediary)
    RETURN i
    ORDER BY toLower(coalesce(i.name,'')) ASC
    LIMIT $intermediaries
}
CALL {
    WITH i
    OPTIONAL MATCH (i)-[:intermediary_of]-(e2:Entity)
    RETURN e2
    LIMIT $entities_per_intermediary
}
CALL {
    WITH e2
    OPTIONAL MATCH (e2)-[:officer_of]-(o:Officer)
    RETURN o
    LIMIT $officers_per_entity
}
WITH collect(DISTINCT e) + collect(DISTINCT i) + collect(DISTINCT e2) + collect(DISTINCT o) AS ns
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

# Intermediary-focused graph view (when clicking an intermediary in the list)
INTERMEDIARY_EXISTS_QUERY = """
MATCH (i:Intermediary)
WHERE toString(i.node_id) = toString($intermediary_id)
RETURN i.node_id AS id, i.name AS name
LIMIT 1
"""

INTERMEDIARY_FOCUS_GRAPH_QUERY = """
MATCH (i:Intermediary)
WHERE toString(i.node_id) = toString($intermediary_id)
CALL {
    WITH i
    MATCH (i)-[:intermediary_of]-(e:Entity)
    WHERE ($iso3 IS NULL OR $iso3 = '' OR e.country_codes CONTAINS $iso3)
    RETURN e
    ORDER BY toLower(coalesce(e.name,'')) ASC
    LIMIT $entities
}
CALL {
    WITH e
    OPTIONAL MATCH (e)-[:officer_of]-(o:Officer)
    RETURN o
    LIMIT $officers_per_entity
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


def get_country_entities(iso3: str, q: str | None = None, limit: int = 200, offset: int = 0):
    """List Entity nodes for a country (by ISO3 substring match).

    Returns:
      { items: [{id, name}], total, limit, offset }

    If Neo4j is unavailable or iso3 invalid, returns empty items.
    """

    iso3 = (iso3 or "").strip().upper()
    driver = _get_driver()

    if driver is None or not re.match(r"^[A-Z]{3}$", iso3):
        return {"items": [], "total": 0, "limit": limit, "offset": offset}

    q = (q or "").strip()

    try:
        with driver.session() as session:
            total_rec = session.run(ENTITIES_COUNT_QUERY, iso3=iso3, q=q).single()
            total = int(total_rec["total"]) if total_rec and total_rec["total"] is not None else 0

            items = []
            for rec in session.run(ENTITIES_QUERY, iso3=iso3, q=q, limit=int(limit), offset=int(offset)):
                items.append({"id": rec.get("id"), "name": rec.get("name") or str(rec.get("id"))})

        return {"items": items, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        print(f"Entity list query failed for {iso3}: {e}")
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": "graph_unavailable"}


def get_country_intermediaries(iso3: str, q: str | None = None, limit: int = 200, offset: int = 0):
    """List Intermediary nodes connected to a country's entities (by ISO3 substring match).

    Returns:
      { items: [{id, name}], total, limit, offset }

    If Neo4j is unavailable or iso3 invalid, returns empty items.
    """

    iso3 = (iso3 or "").strip().upper()
    driver = _get_driver()

    if driver is None or not re.match(r"^[A-Z]{3}$", iso3):
        return {"items": [], "total": 0, "limit": limit, "offset": offset}

    q = (q or "").strip()

    try:
        with driver.session() as session:
            total_rec = session.run(INTERMEDIARIES_COUNT_QUERY, iso3=iso3, q=q).single()
            total = int(total_rec["total"]) if total_rec and total_rec["total"] is not None else 0

            items = []
            for rec in session.run(INTERMEDIARIES_QUERY, iso3=iso3, q=q, limit=int(limit), offset=int(offset)):
                items.append({"id": rec.get("id"), "name": rec.get("name") or str(rec.get("id"))})

        return {"items": items, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        print(f"Intermediary list query failed for {iso3}: {e}")
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": "graph_unavailable"}


def get_country_nodes(iso3: str, q: str | None = None, node_type: str = "all", limit: int = 200, offset: int = 0):
    """List nodes for a country, searchable by name and filtered by type.

    `node_type` is one of 'all' | 'entity' | 'officer' | 'intermediary'.
    Returns: { items: [{id, name, type}], total, limit, offset }.
    Degrades to empty items if Neo4j is unavailable or iso3 is invalid.
    """

    iso3 = (iso3 or "").strip().upper()
    node_type = (node_type or "all").strip().lower()
    if node_type not in _NODE_MATCH_CLAUSES and node_type != "all":
        node_type = "all"

    driver = _get_driver()
    if driver is None or not re.match(r"^[A-Z]{3}$", iso3):
        return {"items": [], "total": 0, "limit": limit, "offset": offset}

    q = (q or "").strip()

    match = _build_node_match(node_type)
    name_filter = "WHERE ($q = '' OR toLower(coalesce(n.name, '')) CONTAINS toLower($q))"
    list_query = (
        f"{match}\n{name_filter}\n"
        f"RETURN n.node_id AS id, n.name AS name, {_NODE_TYPE_EXPR}\n"
        "ORDER BY toLower(coalesce(name, '')) ASC, id ASC\n"
        "SKIP $offset LIMIT $limit"
    )
    count_query = f"{match}\n{name_filter}\nRETURN count(n) AS total"

    try:
        with driver.session() as session:
            total_rec = session.run(count_query, iso3=iso3, q=q).single()
            total = int(total_rec["total"]) if total_rec and total_rec["total"] is not None else 0

            items = []
            for rec in session.run(list_query, iso3=iso3, q=q, limit=int(limit), offset=int(offset)):
                items.append({
                    "id": rec.get("id"),
                    "name": rec.get("name") or str(rec.get("id")),
                    "type": rec.get("type"),
                })

        return {"items": items, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        print(f"Country node list query failed for {iso3} ({node_type}): {e}")
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": "graph_unavailable"}


def get_entity_graph(entity_id: str, intermediaries: int = 6, entities_per_intermediary: int = 6, officers_per_entity: int = 2):
    """Return a small graph centered on a single Entity (node_id).

    Returns the same shape as get_country_graph: {nodes: [...], links: [...]}.
    """

    driver = _get_driver()
    if driver is None:
        return {"nodes": [], "links": []}

    entity_id = (entity_id or "").strip()
    if not entity_id:
        return {"nodes": [], "links": []}

    # Ensure the entity exists (and get a label) even if it has no relationships.
    try:
        with driver.session() as session:
            base = session.run(ENTITY_EXISTS_QUERY, entity_id=entity_id).single()
            if not base:
                return {"nodes": [], "links": []}

            base_node = {
                "id": base["id"],
                "label": base["name"] or str(base["id"]),
                "type": "Entity",
                "degree": 0,
            }

            nodes, links, seen = {}, [], set()
            rows = list(
                session.run(
                    ENTITY_FOCUS_GRAPH_QUERY,
                    entity_id=entity_id,
                    intermediaries=int(intermediaries),
                    entities_per_intermediary=int(entities_per_intermediary),
                    officers_per_entity=int(officers_per_entity),
                )
            )

            if not rows:
                return {"nodes": [base_node], "links": []}

            for rec in rows:
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
                    links.append({"source": rec["r_start"], "target": rec["r_end"], "rel": rec["rel"]})

    except Exception as e:
        print(f"Entity-focused graph query failed for {entity_id}: {e}")
        return {"nodes": [], "links": [], "error": "graph_unavailable"}

    for l in links:
        for end in ("source", "target"):
            if l[end] in nodes:
                nodes[l[end]]["degree"] += 1

    # Ensure the focused entity is present in nodes.
    if base_node["id"] not in nodes:
        nodes[base_node["id"]] = base_node

    return {"nodes": list(nodes.values()), "links": links}


def get_intermediary_graph(intermediary_id: str, iso3: str | None = None, entities: int = 12, officers_per_entity: int = 2):
    """Return a small graph centered on a single Intermediary (node_id).

    If iso3 is provided (AAA), the Entity expansion is filtered to that country's entities.

    Returns the same shape as get_country_graph: {nodes: [...], links: [...]}.
    """

    driver = _get_driver()
    if driver is None:
        return {"nodes": [], "links": []}

    intermediary_id = (intermediary_id or "").strip()
    if not intermediary_id:
        return {"nodes": [], "links": []}

    iso3 = (iso3 or "").strip().upper()
    iso3 = iso3 if re.match(r"^[A-Z]{3}$", iso3) else None

    # Ensure the intermediary exists (and get a label) even if it has no relationships.
    try:
        with driver.session() as session:
            base = session.run(INTERMEDIARY_EXISTS_QUERY, intermediary_id=intermediary_id).single()
            if not base:
                return {"nodes": [], "links": []}

            base_node = {
                "id": base["id"],
                "label": base["name"] or str(base["id"]),
                "type": "Intermediary",
                "degree": 0,
            }

            nodes, links, seen = {}, [], set()
            rows = list(
                session.run(
                    INTERMEDIARY_FOCUS_GRAPH_QUERY,
                    intermediary_id=intermediary_id,
                    iso3=iso3,
                    entities=int(entities),
                    officers_per_entity=int(officers_per_entity),
                )
            )

            if not rows:
                return {"nodes": [base_node], "links": []}

            for rec in rows:
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
                    links.append({"source": rec["r_start"], "target": rec["r_end"], "rel": rec["rel"]})

    except Exception as e:
        print(f"Intermediary-focused graph query failed for {intermediary_id}: {e}")
        return {"nodes": [], "links": [], "error": "graph_unavailable"}

    for l in links:
        for end in ("source", "target"):
            if l[end] in nodes:
                nodes[l[end]]["degree"] += 1

    # Ensure the focused intermediary is present in nodes.
    if base_node["id"] not in nodes:
        nodes[base_node["id"]] = base_node

    return {"nodes": list(nodes.values()), "links": links}



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