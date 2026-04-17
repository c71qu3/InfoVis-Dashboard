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
            ├── load_neo4j.py
            ├── check_neo4j.py
            └── plugins


## Data

In `/filter_data.ipynb` only _Panama Papers_ entries are kept in order to make the data more manageable.


## Setup

1. Download the ZIP file and save it in the `project/data/` directory. The raw data can be found [here](https://offshoreleaks.icij.org/pages/database).

2. Prepare the workspace by running `uv venv`.

3. Run `filter_data.ipynb` notebook to unpack ZIP file.

**Note:** If you use _Docker_ instead of _Podman_ just replace the commands (`docker-compose` in place of `podman-compose`).

4. Build all containers:

```{bash}
podman-compose up --build -d
```

5. To enter the _app_ container:

```{bash}
podman run --rm -it \
    -v "$(pwd)/app:/app:Z" \
    -v "$(pwd)/data:/app/data:Z" \
    --network project_default \
    -w /app \
    localhost/project_app:latest bash
```