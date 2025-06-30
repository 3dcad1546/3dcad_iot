FROM python:3.11-slim

# install OS deps for postgres & kafka clients
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libpq-dev \
      librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy & install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy your service
COPY . .

# start the service
CMD ["python", "main.py"]
