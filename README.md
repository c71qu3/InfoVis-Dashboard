# InfoVis Dashboard (Neo4j + Flask)

This project builds an interactive dashboard on top of the ICIJ *Offshore Leaks* dataset.
For local development and reproducibility it runs as two containers:

- **Neo4j** graph database
- **Flask** web app that prepares the data (optional) and loads it into Neo4j

The repository contains a small data-prep pipeline that filters the raw ICIJ export to only **Panama Papers** entries to keep the dataset manageable.

---

## Prerequisites

- Either **Podman + podman-compose** *or* **Docker + Docker Compose**
- (Optional, for running scripts locally) **uv**: https://docs.astral.sh/uv/

---
## Quickstart

### Data download (mandatory)

To ensure proper data usage, user must download the data first.

1. Download the *Offshore Leaks* database ZIP from:
   https://offshoreleaks.icij.org/pages/database
2. Place the downloaded `.zip` into:

   ```text
   ./data/
   ```

The containers will generate the filtered CSVs automatically on first run (see below).

---
### Build App

From the repository root:

#### Using Podman (Linux)

```bash
podman-compose up --build
```

#### Using Podman (macOS/Windows)

```bash
podman machine init
podman machine start
podman compose up --build
```
> **macOS only:** If `podman compose` is not found, try `podman-compose up --build` with hythen instead.

#### Using Docker

```bash
docker compose up --build
```

When the import has finished, the main app is available at **http://localhost:5001** (not 5000)— that is our project product. 

Building is finished when you see `app-1 Running on all addresses (0.0.0.0)....`

> **First build may take 5–10 minutes**

Neo4j Browser (http://localhost:7474, user: `neo4j`, password: `password`) is exposed for inspection only; you do not need to use it directly.

To stop everything:

```bash
podman compose down # or podman-compose down
# or
docker compose down
```

---

## What happens on first startup?

The `app` container runs `app/src/startup.py`, which:

1. **Prepares data** (only if needed)
   - If the filtered CSVs do **not** exist in `./data/`, it runs `filter_data.py` inside the container.
2. **Waits for Neo4j** to become reachable.
3. **Upserts** the CSVs into Neo4j (`app/src/upsert.py`).
4. Starts the Flask server.

### Forcing a fresh data preparation

If you want to re-generate the filtered CSVs even if they already exist, set:

- `FORCE_PREPARE_DATA=1`

in `docker-compose.yml` (service `app`) or via your compose overrides.

---

## Project structure

```text
.
├── docker-compose.yml            # Neo4j + app services
├── filter_data.ipynb             # Notebook version of the data filter
├── filter_data.py                # Generates filtered CSVs from the downloaded ZIP
├── data/                         # Input ZIP + generated CSVs
│   ├── *.zip                     # Downloaded ICIJ export
│   ├── Address.csv               # Generated (filtered)
│   ├── Edges.csv                 # Generated (filtered)
│   ├── Entity.csv                # Generated (filtered)
│   ├── Intermediary.csv          # Generated (filtered)
│   └── Officer.csv               # Generated (filtered)
├── neo4j/                        # Created/used by the Neo4j container (mounted volume)
└── app/
    ├── Dockerfile
    ├── app.py                    # Flask entrypoint
    ├── templates/
    │   └── index.html            # Single-page UI (loads /static/main.js)
    ├── static/
    │   ├── main.js               # Frontend entrypoint (ES module)
    │   ├── js/                   # Frontend modules
    │   └── data/                 # Map/lookup assets (topojson, ISO mappings, ...)
    └── src/
        ├── startup.py            # Prepare data → wait for Neo4j → import → start app
        ├── upsert.py             # CSV → Neo4j import logic
        └── check.py              # Helper checks
```

---

## Frontend JavaScript files

The UI is a single page (`app/templates/index.html`) that loads `app/static/main.js` as an **ES module**.
Most functionality is split into small modules under `app/static/js/`.

> Note: **D3 v7** and **topojson-client** are loaded via CDN in `index.html` and used as globals (`d3`, `topojson`).

### Entry point

- `app/static/main.js`
  - Bootstraps the dashboard.
  - Loads lookup tables (ISO code maps), connection counts, and outgoing-edge counts.
  - Initializes the three main UI components (map, info panel, graph panel, list panel) and wires up selection callbacks.

### API client

- `app/static/js/api.js`
  - Thin wrapper around backend endpoints (`/api/...`).
  - Used by all panels to fetch graphs, lists, World Bank indicators/years, and jurisdiction connection data.

### UI components (panels)

- `app/static/js/map.js`
  - Renders the world map choropleth (D3 + TopoJSON), including zoom/pan.
  - Handles country hover/selection state.
  - Draws *jurisdiction connection arcs* for the selected country and renders a small side list of connected jurisdictions.

- `app/static/js/infoPanel.js`
  - Manages the “Country Info” panel.
  - Fetches World Bank indicators and renders them as cards.
  - Builds the year selector (latest vs. a specific year).

- `app/static/js/graphPanel.js`
  - Manages the “Knowledge Graph” panel.
  - Renders force-directed graphs for:
    - country-level entity networks (`/api/graph/<ISO3>`)
    - focused subgraphs for a selected entity/intermediary/officer
  - Handles node click → “Node Details” overlay, including optional metadata fetched via `/api/offshore_node/<id>`.

- `app/static/js/listPanel.js`
  - Manages the “Node List” panel.
  - Fetches paginated node lists for the selected country, supports search + type filter.
  - Clicking a list item loads a focused graph in the graph panel.

### Small helpers

- `app/static/js/countrySearch.js`
  - Implements the country typeahead search box (useful for small/hard-to-click countries).

- `app/static/js/tooltip.js`
  - Simple tooltip helper used by the map and graph.

- `app/static/js/utils.js`
  - Shared utilities (e.g., compact number formatting, “info” popovers).

### ISO / country-code helpers

- `app/static/js/countryCodes.js`
  - Builds/resolves *country name → ISO2* (for World Bank indicators) with a small alias set.

- `app/static/js/isoNumeric.js`
  - Builds *TopoJSON numeric id → ISO3* mapping using `iso_numeric.json`.
  - Needed because the map geometry uses numeric ids while the backend uses ISO3.

### Static frontend data assets

- `app/static/data/world-topo.json` — world geometry (TopoJSON)
- `app/static/data/iso_numeric.json` — mapping `{ ISO3 -> numeric }` used to connect map shapes to backend ISO3 codes

---

## Running the data filter locally (optional)

If you prefer to generate the filtered CSVs on the host (instead of inside the container):

```bash
uv venv
uv sync
uv run python filter_data.py
```

---

## Configuration

The compose file sets the following defaults:

- Neo4j auth: `NEO4J_AUTH=neo4j/password`
- App → Neo4j connection:
  - `NEO4J_URI=bolt://neo4j:7687`
  - `NEO4J_USER=neo4j`
  - `NEO4J_PASSWORD=password`
- Startup behavior:
  - `AUTO_PREPARE_DATA=1` (generate filtered CSVs if missing)
  - `FORCE_PREPARE_DATA=0` (override to regenerate)
  - `NEO4J_WAIT_SECONDS=90` (timeout while waiting for Neo4j)


## Troubleshooting

- **Neo4j is empty / app can’t find data**
  - Ensure a `.zip` file exists in `./data/`.
  - Check container logs:

    ```bash
    podman-compose logs -f app
    podman-compose logs -f neo4j
    ```

- **You want a clean database**
  - Stop containers and remove the mounted Neo4j data directory:

    ```bash
    podman-compose down
    rm -rf ./neo4j/data
    ```

  Then start again.
- **Windows:** If you see `unauthorized: incorrect username or password` during build, Docker and Podman may be conflicting. Run `podman pull python:3.13-slim` first, then retry.

---

## AI Usage

In line with the [TU Wien guidelines on AI in education](https://www.tuwien.at/en/studies/teaching-at-tu-wien/digitally-supported-teaching/artificial-intelligence-in-education),
we disclose that generative AI tools were used as coding assistants during this
project. All AI-assisted output was reviewed, adapted, and integrated by us; the
design, interaction, and data-query decisions are our own.

- **Tools used:** AI models: Claude, ChatGPT

We did not track AI assistance line-by-line, so we document it at the component
level for the parts we built:

| Component | Files | AI involvement |
| --- | --- | --- |
| Knowledge-Graph panel | `app/static/js/graphPanel.js`, graph endpoints in `app/app.py` / `app/src/graph_layer.py` | It is AI-assisted. The force-directed rendering, node-details overlay, and Neo4j query/endpoint code were drafted with AI, then reviewed and adapted. The panel concept (country click → offshore network) and interaction design are ours. |
| Node List panel | `app/static/js/listPanel.js`, search query in `app/src/graph_layer.py` | AI-assisted. Pagination and the search + type-filter UI were drafted with AI; the filtering logic and backend query were designed and reviewed by us. |
| Frontend | `templates/index.html`, `app/static/js/map.js`, stable call in `app/src/workd_bank.py` | AI-assisted. The map color scaling, arc paths, rendered list, and world bank api call error handling were drafted with AI, also some styling details such as pulsing dot top left. The panel layout, map structure, world bank info and display, aesthetic design, color palette, and interaction model are designed and reviewed by us. |

### Reflection: using AI in the data-analysis process

AI support was most useful for drafting repetitive implementation details, such
as D3/DOM wiring, Flask endpoint structure, and Cypher query variants. It was
less reliable for dataset-specific issues, especially ICIJ data conventions,
Neo4j import behavior, and deciding whether a graph result was meaningful. Those
parts required manual inspection, debugging, and validation against the actual
data. Overall, AI sped up implementation, but the interpretation, interaction
design, and final checks remained team decisions.

---
