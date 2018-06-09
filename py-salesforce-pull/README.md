SalesForce Pull
===============
    Will re-build the tables in the data warehouse to match the incoming data from the Salesforce API and load.

Requirements
------------
    * $APP_ROOT/conf/env.yaml
        * SNOWFLAKE_DB_CONNECTION
        * SF_USERNAME
        * SF_PASSWORD
        * SF_SECURITY_TOKEN
        * SF_IS_SANDBOX
        * SF_API_VERSION
        * SF_PROXIES
        * SF_OBJECT_NAMES
    * $APP_ROOT/conf/ox_salesforce_pull.yaml
        * MAX_ATTEMPTS
        * WAIT_BETWEEN_ATTEMPTS
        * LOAD_STATE_VAR

Config/Settings
---------------
    Rarely changed settings are in the ox_dw_salesforce_pull/settings.py

    ENV specific and secret(passwords, etc) are in $APP_ROOT/conf/env.yaml

    Other configuration items are in $APP_ROOT/conf/ox_salesforce_pull.yaml

    Having some configuration outside of the package allows for changes without having to reversion and redeploy the python code.

Usage
-----
    * Run: salesforce_pull
    * For more help: salesforce_pull --help
