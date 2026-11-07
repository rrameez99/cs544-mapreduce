# Single image for the boss and the workers; docker-compose.yml picks the
# command for each container.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python scripts/gen_protos.py

# Print log lines immediately so `docker compose logs` is live.
ENV PYTHONUNBUFFERED=1
