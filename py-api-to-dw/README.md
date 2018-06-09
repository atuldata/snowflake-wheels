API Extractor
=============
This runs a rabbitmq consumer against our internal API for the purpose of loading into the DW.

The consumer as-well as the loader will both run in the back-ground.

api_consumer
------------
This will run a single consumer of the API queues. Can run separately or use the start-api-extractor.sh script to run each of the default configured ones.
    See api_consumer --help

api_harmonizer
--------------
This will pull all of the data from the API DB for a given object_type and instance_uid. It will output a file the the api_loader will pick up and load.
    See api_harmonizer --help

api_loader
----------
This will startup the never ending check for output files from the extractor and load them. Can run separately or use the start-api-extractor.sh script to run.
    See api_loader --help

CONFIG
------
    - Schema Config
        * Both the extractor and the loader share $APP_ROOT/conf/api-extractor-schema-config.yaml
    - Consumer config for the extractor(s)
        * $APP_ROOT/conf/consumer_config/\*.yaml

Data Pivoting
-------------
Use dot notation to extract particular items from the json document. Will duplicate rows when encountering lists. Take care to note that if a row has more than one list the rows will multiply even more. At any point in a json object you can get back the KEYS or VALUES as a list. 

Example keys:
    - revision
    - item.0.key1 <- Note list dot notation
    - item.0.KEYS <- Will return a list of the keys at this location if exist.

SEE ALSO
--------
dw/ox-snowflake-etl/bin/start-api-extractor

TO DO
--------
1. Verify the STAGE TABLE Creation with all required file format parameters.
2. How to identify the rejected load data in snowflake tables(Similar to Reject/Exception keyword in Vertica COPY statement.)
