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
START_DATE = "startdate"
END_DATE = "enddate"
STAMP_FORMAT = "%Y-%m-%d"
HEADER_DICT = {
    "token": open(TOKEN_FILE).read().strip(),
    # "includemetadata": "false",
}
PROJECT_ID = "verdant-bond-262820"
TOPIC = "weather"
PUBSUB_TOPIC_NAME = f"projects/{PROJECT_ID}/topics/{TOPIC}"

# Set up Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(pathlib.Path(__file__).parent / GOOGLE_APPLICATION_CREDENTIALS_FILE)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
handler.setFormatter(formatter)
logger.addHandler(handler)
# handler.setLevel(logging.DEBUG)


@retry()
def get(url,
        headers=HEADER_DICT,
        params=None,
        tries=3,
        delay=2,
        backoff=2,
        logger=logger,
        ) -> list[dict]:
    """
    :param url: NOAA URL
    :param headers: authentication
    :param params: tells NOAA what data we want
    :param tries: see https://pypi.org/project/retry/
    :param delay: see https://pypi.org/project/retry/
    :param backoff: see https://pypi.org/project/retry/
    :param logger: see https://pypi.org/project/retry/
    :return: the data from the API call
    """
    METADATA = "metadata"
    OFFSET = "offset"
    LIMIT = 1000
    return_list = list()
    offset = 1
    params["limit"] = LIMIT  # See NOAA documentation
    while True:
        params[OFFSET] = offset
        response = requests.get(url, headers=headers, params=param_dict)
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
data_list = get(url, headers=HEADER_DICT, params=param_dict)
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
data_list = get(url, headers=HEADER_DICT, params=param_dict)

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
    data_list = get(url, headers=HEADER_DICT, params=param_dict)
    # Iterate over the measured data and write to the database
    for measurement in data_list:
        logger.info(measurement)
        # Publish
        payload = json.dumps(measurement, sort_keys=True, indent=2).encode()
        future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="measurement")
        logger.info(f"Successfully posted message_id: {future.result()}.")
