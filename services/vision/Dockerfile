# vision/Dockerfile
FROM python:3.9-slim

# system deps (if you need CSV, base64, etc. all stdlib)
WORKDIR /app

# install FastAPI & Uvicorn
RUN pip install --no-cache-dir fastapi uvicorn

# copy app code & any image_x.png files
COPY main.py .
COPY image_*.png ./

EXPOSE 5000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
