"""
See API documentation at https://www.ncdc.noaa.gov/cdo-web/webservices/v2#data
"""
import collections
from datetime import datetime, timedelta
import json
import logging
import os
import pathlib
import requests
import time
# Imports above are standard Python
# Imports below are 3rd-party
from retry import retry
from google.cloud import pubsub_v1
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud import bigquery
from google.cloud import secretmanager
from flask import Flask

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
TOKEN_SECRET_NAME = "ncdc_cdo_web_services_token"
DATA_SET_ID = "GHCND"  # daily summaries
COLORADO = "FIPS:08"
START_DATE = "startdate"
END_DATE = "enddate"
STAMP_FORMAT = "%Y-%m-%d"
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
stream_handler.setLevel(logging.DEBUG)  # Will send debug if set to DEBUG 3 lines up
# Add handlers
logger = logging.getLogger(log_name)
logger.setLevel(logging.DEBUG)  # Will send debug if set to DEBUG 6 lines up
logger.addHandler(gcloud_logging_handler)
logger.addHandler(stream_handler)
logger.info(f"Starting {__file__} ...")

# Get our NOAA API token for the API call header
client = secretmanager.SecretManagerServiceClient()
resource_name = f"projects/{PROJECT_ID}/secrets/{TOKEN_SECRET_NAME}/versions/latest"
token = client.access_secret_version(name=resource_name).payload.data.decode("UTF-8")
header_dict = {"token": token}


@retry(tries=3, delay=2, backoff=2, logger=logger)
def get(url,
        headers=header_dict,
        params=None,
        timeout=10,  # seconds
        ) -> list[dict]:
    """
    Support the retrieval from the API a record set which may exceed the per-call API limit.
    Use the retry library to retry before giving up.
    :param url: NOAA URL
    :param headers: authentication
    :param params: dictionary telling NOAA what data we want
    :param timeout: for each GET request give up after this many seconds (separate from retry logic)
    :return: the data from the API call
    """
    return_list = list()
    offset = 1
    limit = 1000
    params["limit"] = limit  # See NOAA documentation

    global call_count
    call_count += 1

    while True:
        params["offset"] = offset
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        logger.debug(f"Request status code: {response.status_code}.")
        response.raise_for_status()
        logger.debug(response.json()["metadata"])
        return_list.extend(response.json()["results"])
        total_available = response.json()["metadata"]["resultset"]["count"]
        if total_available <= len(return_list):
            logger.info(f"Fetched {len(return_list)} records from API.")
            return return_list
        offset += limit


# Set-up complete, program starts here
# Count number of API calls, NOAA has a daily limit
call_count, error_count = 0, 0
# Allow invocation via HTTP so this code can be invoked when running in a container
app = Flask(__name__)


@app.route("/")
def run():
    # Note time
    start_time = time.monotonic()
    # Create publisher
    publisher = pubsub_v1.PublisherClient()

    # Fetch via the API the different measurement types we might see (NOAA calls them datatypes)
    url = f"{BASE_URL}/datatypes"
    param_dict = {
        "locationid": COLORADO,
        END_DATE: datetime(2000, 1, 1).strftime(STAMP_FORMAT),
    }
    logger.info(f"Getting {url} with parameters {param_dict} ...")
    data_list = get(url, params=param_dict)
    for datatype in data_list:
        logger.debug(datatype)
        # Publish datatype (measurement type) information
        payload = json.dumps(datatype, sort_keys=True, indent=2).encode()
        future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="datatype")
        logger.info(f"Successfully posted message_id: {future.result()}.")

    # Fetch via the API the stations in Colorado with data between January 1, 2000 and today
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
        # Publish station information
        payload = json.dumps(station, sort_keys=True, indent=2).encode()
        future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="station")
        logger.info(f"Successfully posted message_id: {future.result()}.")

    # Fetch from the target database the station/date combinations which do not exist
    logger.info(f"Querying the target database to determine what data needs to be fetched ...")
    missing_dict = collections.defaultdict(list)
    bigquery_client = bigquery.Client()
    sql = "select station_id, day_ from weather_dw.missing_temperature_measurements_v"
    query_job = bigquery_client.query(sql)
    for row in query_job.result():
        station_id, day_ = row.values()
        missing_dict[station_id].append(day_)

    # For each station with one or more days of missing data, fetch it via the API.
    # To reduce the number of API calls we will fetch a range of dates.
    # For example, if we are missing data for the 10th, 12th and 14th we will fetch
    # from the API the 10th, 11th, 12th, 13th and 14th.
    for station_id, missing_day_list in missing_dict.items():
        start_date = min(missing_day_list)
        end_date = max(missing_day_list)
        logger.info(f"Retrieving data for station {station} for {start_date} through {end_date} ...")
        url = f"{BASE_URL}/data"
        param_dict = {
            "datasetid": DATA_SET_ID,
            "stationid": station_id,
            "units": "metric",
            START_DATE: start_date.strftime(STAMP_FORMAT),
            END_DATE: end_date.strftime(STAMP_FORMAT),
        }
        logger.info(f"Getting {url} with parameters {param_dict} ...")
        try:
            data_list = get(url, params=param_dict)
        except Exception as e:
            # It's okay if we cannot get a particular station's measurements, just log it
            logger.error(f"Skipping station '{station['id']}', error message was: {e}.")
            global error_count
            error_count += 1
            continue
        # Iterate over the measured data and write to the database
        for measurement in data_list:
            logger.info(measurement)
            # Publish measurement for this datatype at this station on this day
            payload = json.dumps(measurement, sort_keys=True, indent=2).encode()
            future = publisher.publish(PUBSUB_TOPIC_NAME, payload, record_type="measurement")
            logger.info(f"Successfully posted message_id: {future.result()}.")

    duration = int(time.monotonic() - start_time)
    logger.info(f"Finished in {duration} seconds. Made {call_count} API calls with {error_count} errors.")
    return {
        "api_call_count": call_count,
        "error_count": error_count,
        "duration_in_seconds": duration,
    }


if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0")
