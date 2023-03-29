FROM --platform=$BUILDPLATFORM python:alpine AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ARG GOOGLE_APPLICATION_CREDENTIALS_FILE=credentials.json

COPY ${GOOGLE_APPLICATION_CREDENTIALS_FILE} .

CMD ["python", "fetch.py"]
