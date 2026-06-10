import csv
import re
from pathlib import Path


_ISO3_RE = re.compile(r"^[A-Z]{3}$")


def _primary_iso3(codes: str) -> str | None:
    """Extract the first ISO3 code from a semicolon/comma-separated country_codes field."""
    if not codes or not isinstance(codes, str):
        return None
    for part in re.split(r"[;,]\s*", codes.strip()):
        part = (part or "").strip().upper()
        if _ISO3_RE.match(part):
            return part
    return None


def count_connections_iso3(data_dir: Path, csv_files=None) -> dict:
    """Count occurrences of ISO3 country codes in the source CSVs.

    This is *not* the same as "has an offshore network"; it's simply how often
    a code appears in the raw tables.

    Returns: dict like {"USA": 123, "GBR": 456, ...}
    """

    counts: dict[str, int] = {}
    csv_files = csv_files or ["Address.csv", "Entity.csv", "Intermediary.csv", "Officer.csv"]

    for fname in csv_files:
        filepath = data_dir / fname
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping")
            continue

        with filepath.open(newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)

            col = None
            if reader.fieldnames:
                if "country_codes" in reader.fieldnames:
                    col = "country_codes"
                elif "countries" in reader.fieldnames:
                    col = "countries"

            if not col:
                continue

            for row in reader:
                code = _primary_iso3(row.get(col))
                if not code:
                    continue
                counts[code] = counts.get(code, 0) + 1

    return counts


def count_outgoing_crossborder_edges_iso3(data_dir: Path) -> dict:  # AI assisted 
    """Count a *country-level offshore network signal* (ISO3 -> edge count).

    The UI notion of "clickable" should match what the entity-network query can
    actually return. The Neo4j entity-network view is anchored on:

      (Intermediary)-[:intermediary_of]-(Entity)

    and filtered by the *Entity*'s country code.

    So here we count, per ISO3, how many `intermediary_of` edges touch an Entity
    whose primary ISO3 is that country.

    Returns: dict like {"USA": 3066, "GBR": 9619, ...}
    """

    # Build node_id -> (type, iso3)
    node_id_to_type: dict[str, str] = {}
    node_id_to_iso3: dict[str, str] = {}

    node_tables = [
        ("Entity.csv", "Entity"),
        ("Intermediary.csv", "Intermediary"),
        ("Officer.csv", "Officer"),
        ("Address.csv", "Address"),
    ]

    for fname, ntype in node_tables:
        filepath = data_dir / fname
        if not filepath.exists():
            continue

        with filepath.open(newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "node_id" not in reader.fieldnames:
                continue

            col = "country_codes" if "country_codes" in reader.fieldnames else ("countries" if "countries" in reader.fieldnames else None)

            for row in reader:
                nid = row.get("node_id")
                if nid is None:
                    continue

                nid = str(nid)
                node_id_to_type[nid] = ntype

                if col:
                    code = _primary_iso3(row.get(col))
                    if code:
                        node_id_to_iso3.setdefault(nid, code)

    counts: dict[str, int] = {}

    edges_path = data_dir / "Edges.csv"
    if not edges_path.exists():
        return counts

    with edges_path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return counts

        start_col = "node_id_start" if "node_id_start" in reader.fieldnames else None
        end_col = "node_id_end" if "node_id_end" in reader.fieldnames else None
        rel_col = "rel_type" if "rel_type" in reader.fieldnames else None
        if not start_col or not end_col or not rel_col:
            return counts

        for row in reader:
            if row.get(rel_col) != "intermediary_of":
                continue

            s = row.get(start_col)
            t = row.get(end_col)
            if not s or not t:
                continue

            s = str(s)
            t = str(t)

            # Identify which end is the Entity.
            entity_nid = None
            if node_id_to_type.get(s) == "Entity" and node_id_to_type.get(t) == "Intermediary":
                entity_nid = s
            elif node_id_to_type.get(t) == "Entity" and node_id_to_type.get(s) == "Intermediary":
                entity_nid = t
            else:
                continue

            iso3 = node_id_to_iso3.get(entity_nid)
            if not iso3:
                continue

            counts[iso3] = counts.get(iso3, 0) + 1

    return counts


def clamp_arg(args, name: str, default: int, lo: int, hi: int) -> int:  # AI assisted 
    """
    Clamp an integer query parameter from a Flask request.args-like mapping.
    """
    try:
        return max(lo, min(hi, int(args.get(name, default))))
    except (TypeError, ValueError):
        return default