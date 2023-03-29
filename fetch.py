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
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
TOKEN_FILE = "NCDC_CDO_web_services_token"
DATA_SET_ID = "GHCND"  # daily summaries
COLORADO = "FIPS:08"
START_DATE = "startdate"
END_DATE = "enddate"
STAMP_FORMAT = "%Y-%m-%d"
HEADER_DICT = {
    "token": open(TOKEN_FILE).read().strip(),
}
PROJECT_ID = "verdant-bond-262820"
TOPIC = "weather"
PUBSUB_TOPIC_NAME = f"projects/{PROJECT_ID}/topics/{TOPIC}"

# Set up Google credentials allowing us to log to Google and publish to PubSub
GOOGLE_APPLICATION_CREDENTIALS_FILE = "credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(pathlib.Path(__file__).parent / GOOGLE_APPLICATION_CREDENTIALS_FILE)

# Set up logging
log_name = os.path.basename(__file__)
gcloud_logging_client = google.cloud.logging.Client()
# Google logging
gcloud_logging_handler = CloudLoggingHandler(gcloud_logging_client, name=log_name)
gcloud_logging_handler.setLevel(logging.INFO)
# Console logging
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
# Add handlers
logger = logging.getLogger(log_name)
logger.setLevel(logging.DEBUG)
logger.addHandler(gcloud_logging_handler)
logger.addHandler(stream_handler)

# Count number of API calls, NOAA has a daily limit
call_count = 0


@retry(tries=3, delay=2, backoff=2, logger=logger)
def get(url,
        headers=HEADER_DICT,
        params=None,
        timeout=10,  # seconds
        ) -> list[dict]:
    """
    :param url: NOAA URL
    :param headers: authentication
    :param params: dictionary telling NOAA what data we want
    :return: the data from the API call
    """
    METADATA = "metadata"
    OFFSET = "offset"
    LIMIT = 1000
    return_list = list()
    offset = 1
    params["limit"] = LIMIT  # See NOAA documentation

    global call_count
    call_count += 1

    while True:
        params[OFFSET] = offset
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        logger.debug(response.status_code)
        response.raise_for_status()
        # logger.debug(response.json()[METADATA])
        return_list.extend(response.json()["results"])
        total_available = response.json()[METADATA]["resultset"]["count"]
        if total_available <= len(return_list):
            logger.info(f"Fetched {len(return_list)} records from API.")
            return return_list
        offset += LIMIT


# Set-up complete, program starts here
# Create publisher
publisher = pubsub_v1.PublisherClient()

# Get the different measurement types we might see (NOAA calls them datatypes)
url = f"{BASE_URL}/datatypes"
param_dict = {
    "locationid": COLORADO,
    END_DATE: datetime(2000, 1, 1).strftime(STAMP_FORMAT),
}
logger.info(f"Getting {url} with parameters {param_dict} ...")
data_list = get(url, params=param_dict)
for datatype in data_list:
    logger.debug(datatype)
    payload = json.dumps(datatype, sort_keys=True, indent=2).encode()
    future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="datatype")
    logger.info(f"Successfully posted message_id: {future.result()}.")

# Iterate over each station in Colorado with data between January 1, 2000 and today
# The enddate and startdate parameters below look reversed but they are correct,
# see https://www.ncdc.noaa.gov/cdo-web/webservices/v2#stations.
url = f"{BASE_URL}/stations"
param_dict = {
    "locationid": COLORADO,
    END_DATE: datetime(2000, 1, 1).strftime(STAMP_FORMAT),
    START_DATE: (datetime.today() - timedelta(7)).strftime(STAMP_FORMAT),
}
logger.info(f"Getting {url} with parameters {param_dict} ...")
data_list = get(url, params=param_dict)

for station in data_list:
    payload = json.dumps(station, sort_keys=True, indent=2).encode()
    future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="station")
    logger.info(f"Successfully posted message_id: {future.result()}.")
    logger.info(f"Examining station: {station} ...")
    url = f"{BASE_URL}/data"
    param_dict = {
        "datasetid": DATA_SET_ID,
        "stationid": station["id"],
        "units": "metric",
        START_DATE: (datetime.today() - timedelta(7)).strftime(STAMP_FORMAT),
        END_DATE: (datetime.today()).strftime(STAMP_FORMAT),
    }
    logger.info(f"Getting {url} with parameters {param_dict} ...")
    try:
        data_list = get(url, params=param_dict)
    except Exception as e:
        # It's okay if we cannot get a particular station's measurements, just log it
        logger.error(f"Skipping station '{station['id']}', error message was: {e}.")
        continue
    # Iterate over the measured data and write to the database
    for measurement in data_list:
        logger.info(measurement)
        # Publish
        payload = json.dumps(measurement, sort_keys=True, indent=2).encode()
        future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="measurement")
        logger.info(f"Successfully posted message_id: {future.result()}.")

logger.info(f"Finished. Made {call_count} API calls.")
