# FROM --platform=$BUILDPLATFORM python:alpine AS base
FROM python:alpine AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "fetch.py"]
