FROM python:3.13-slim

WORKDIR /app

RUN pip install flask neo4j pandas requests

COPY . .

CMD ["python", "app.py"]