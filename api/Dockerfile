FROM python:3.9-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY etl_api.py .

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "etl_api:app"]
