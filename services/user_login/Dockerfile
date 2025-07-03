FROM python:3.11-slim

WORKDIR /app

# system deps for psycopg2, pymodbus, etc.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Environment variables (override in your compose or k8s)
ENV DB_URL="postgresql://edge:edgepass@postgres:5432/edgedb" \
    KAFKA_BROKER="kafka:9092" \
    PLC_WRITE_COMMANDS_TOPIC="plc_write_commands"

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
