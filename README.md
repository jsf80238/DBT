# Potential improvements
- Use protobufs
- query database to determine what date to start from
- add attributes to client.publish call
- put credentials file, logging level, etc. into a config file
- add comments to DDL
- use https://google-auth.readthedocs.io/en/latest/reference/google.auth.credentials.html#google.auth.credentials.Credentials to authenticate
- performance
  - as weather_ods.measurement grows performance might degrade
  - add clustering key