
FROM python:3.11-slim
WORKDIR /app
COPY oee_influx_writer.py .
RUN pip install aiokafka influxdb-client
CMD ["python", "main.py"]

