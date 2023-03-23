"""
See API documentation at https://www.ncdc.noaa.gov/cdo-web/webservices/v2#data
"""
import sys
from datetime import datetime, timedelta
import json
import logging
import os
import pathlib
import requests
from time import sleep
# Imports above are standard Python
# Imports below are 3rd-party
from retry import retry
from google.cloud import pubsub_v1

GOOGLE_APPLICATION_CREDENTIALS_FILE = "credentials.json"
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
TOKEN_FILE = "NCDC_CDO_web_services_token"
DATA_SET_ID = "GHCND"  # daily summaries
COLORADO = "FIPS:08"
STATIONS = "stations"
DATATYPES = "datatypes"
START_DATE = "startdate"
END_DATE = "enddate"
UNITS = "metric"
LIMIT = "limit"
RESULTS = "results"
RECORD_LIMIT = 1000
STAMP_FORMAT = "%Y-%m-%d"
GET_DELAY = 1  # seconds between requests
GET_TRY_COUNT = 3  # Total number of tries per REST call, including initial one
GET_BACKOFF = 2  # Retry backoff delay ratio
HEADER_DICT = {
    "token": open(TOKEN_FILE).read().strip(),
}
PROJECT_ID = "verdant-bond-262820"
TOPIC = "weather"
PUBSUB_TOPIC_NAME = f"projects/{PROJECT_ID}/topics/{TOPIC}"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
handler.setFormatter(formatter)
logger.addHandler(handler)
# handler.setLevel(logging.DEBUG)

# Set up Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(pathlib.Path(__file__).parent / GOOGLE_APPLICATION_CREDENTIALS_FILE)

# Create publisher
publisher = pubsub_v1.PublisherClient()


@retry()
def get(url,
        headers=HEADER_DICT,
        params=None,
        tries=GET_TRY_COUNT,
        delay=GET_DELAY,
        backoff=GET_BACKOFF,
        logger=logger
        ):
    # sleep(GET_DELAY)
    response = requests.get(url, headers=headers, params=param_dict)
    response.raise_for_status()
    return response


# Get the different measurement types we might see (NOAA calls them datatypes)
url = f"{BASE_URL}/{DATATYPES}"
param_dict = {
    "locationid": COLORADO,
    LIMIT: RECORD_LIMIT,
    END_DATE: datetime(2000, 1, 1).strftime(STAMP_FORMAT),
}
logger.info(f"Getting {url} with parameters {param_dict} ...")
response = get(url, headers=HEADER_DICT, params=param_dict)
# print(json.dumps(response.json()["results"], sort_keys=True, indent=2))
for datatype in response.json()[RESULTS]:
    logger.debug(datatype)
    continue
    payload = json.dumps(datatype, sort_keys=True, indent=2).encode()
    future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="datatype")
    logger.info(f"Successfully posted message_id: {future.result()}.")
sys.exit()
# Iterate over each station in Colorado with data between January 1, 2000 and today
# The enddate and startdate parameters below look reversed but they are correct,
# see https://www.ncdc.noaa.gov/cdo-web/webservices/v2#stations.
url = f"{BASE_URL}/{STATIONS}"
param_dict = {
    "locationid": COLORADO,
    LIMIT: RECORD_LIMIT,
    END_DATE: datetime(2000, 1, 1).strftime(STAMP_FORMAT),
    START_DATE: (datetime.today() - timedelta(7)).strftime(STAMP_FORMAT),
}
logger.info(f"Getting {url} with parameters {param_dict} ...")
response = get(url, headers=HEADER_DICT, params=param_dict)
for station in response.json()[RESULTS]:
    payload = json.dumps(station, sort_keys=True, indent=2).encode()
    future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="station")
    logger.info(f"Successfully posted message_id: {future.result()}.")
    logger.info(f"Examining station: {station} ...")
    url = f"{BASE_URL}/data"
    param_dict = {
        "datasetid": DATA_SET_ID,
        "stationid": station["id"],
        "units": UNITS,
        LIMIT: RECORD_LIMIT,
        START_DATE: (datetime.today() - timedelta(7)).strftime(STAMP_FORMAT),
        END_DATE: (datetime.today()).strftime(STAMP_FORMAT),
    }
    logger.info(f"Getting {url} with parameters {param_dict} ...")
    response = get(url, headers=HEADER_DICT, params=param_dict)
    # Iterate over the measured data and write to the database
    for measurement in response.json()[RESULTS]:
        logger.info(measurement)
        # Publish
        payload = json.dumps(measurement, sort_keys=True, indent=2).encode()
        future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="measurement")
        logger.info(f"Successfully posted message_id: {future.result()}.")
