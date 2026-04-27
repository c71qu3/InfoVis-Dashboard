# Information Visualization Project


## Project Structure

    project/
    ├── README.md
    ├── docker-compose.yml
    │
    ├── filter_data.ipynb
    ├── .python-version
    ├── pyproject.toml
    ├── uv.lock
    │
    ├── data/
    │   ├── raw/
    │   ├── full-oldb.LATEST.zip
    │   ├── Address.csv
    │   ├── Edges.csv
    │   ├── Entity.csv
    │   ├── Intermediary.csv
    │   └── Officer.csv
    │
    ├── neo4j/
    │   ├── logs/
    │   ├── config/
    │   ├── data/
    │   └── plugins/
    │
    └── app/
        ├── Dockerfile
        ├── app.py
        └── src/
            ├── upsert.py
            ├── check.py
            └── plugins

The CSV files in `data/` are extracted by `filter_data.ipynb`.
The `neo4j/` directory and its sub-directories are created by the Neo4j container.


## Data

In `/filter_data.ipynb` only _Panama Papers_ entries are kept in order to make the data more manageable.


## Setup

1. Clone the repo:

```{bash}
git clone git@github.com:c71qu3/InfoVis-Dashboard.git
```

2. Download the ZIP file and save it in the `InfoVis-Dashboard/data/` directory. The raw data can be found [here](https://offshoreleaks.icij.org/pages/database).

3. Prepare the workspace by running:

```{bash}
uv venv
uv sync
```

4. Run `filter_data.ipynb` notebook to unpack ZIP file.

**Note:** If you use _Docker_ instead of _Podman_ just replace the commands (`docker-compose` in place of `podman-compose`).

5. Build all containers from the `InfoVis-Dashboard/` directory:

```{bash}
podman-compose up --build
```

If the Neo4j database is empty it will take a moment to load the data.
When ready, it will show the Flask app [http://localhost:5000](http://localhost:5000) in the host machine.

To stop the container:

```{bash}
podman-compose down
```
