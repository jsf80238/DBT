# DBT demonstration with GCP
This code uses the following technologies:
- Python and the [requests](https://requests.readthedocs.io/en/latest/) module
- Google [PubSub](https://cloud.google.com/pubsub)
- Google [BigQuery](https://cloud.google.com/bigquery)
- [DBT (Cloud version)](https://cloud.getdbt.com)

# Data Flow
![data flow](images/DBT_and_GCP_Data_Flow.png)

# Potential improvements
- Run on a schedule
- Use protobufs (https://protobuf.dev/)
- The NOAA API limits results to 1000 items; add code to paginate to get all records
- query database to determine what date to start from
- put credentials file, logging level, etc. into a config file
- put credentials file and NOAA REST API token into Google Secrets Manager
- add comments to DDL
- use https://google-auth.readthedocs.io/en/latest/reference/google.auth.credentials.html#google.auth.credentials.Credentials to authenticate
- create surrogate keys for stations (see https://discourse.getdbt.com/t/can-i-create-an-auto-incrementing-id-in-dbt/579/2 and https://docs.getdbt.com/blog/managing-surrogate-keys)
- do something interesting with the temperature table, maybe analysis of temperature vs. elevation or latitude
- performance
  - as weather_ods.measurement grows performance might degrade
  - add clustering key
