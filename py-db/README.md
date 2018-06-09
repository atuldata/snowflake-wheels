# py-db

# OXDB
Caches and return a database connection.Currently supports connection via DSN in odbc.ini
- Required:

  -- connection_name: A defined DB_CONNECTION in env.yaml

- Optional:
  
  -- transactions_support - default is True. For non-transactional data sources
 like Impala, Hive it should be set to False.

In the env.yaml file have a configuration like so:
``` yaml
    DW_NAME: SNOWFLAKE
    DB_CONNECTIONS:
        SNOWFLAKE:
             TYPE: SNOWFLAKE
             KWARGS:
                  account: openx
                  user: XXX
                  password: XXX
                  region: us-east-1
                  database: DEV
                  schema: mstr_datamart
                  warehouse: DEV
                  autocommit: False
                  paramstyle: qmark
                  timezone: UTC
        VERTICA_PRIMARY:
             TYPE: ODBC
             DSN: VerticaDW_qa_etl
        BUYER_DB_META:
             TYPE: ODBC
             DSN: BuyerDB_CA
```

 ### Examples:
``` python
    db = OXDB(ENV['DW_NAME'])
    # Iterate over returned rows
    for row in db.get_executed_cursor(query):
        ...
    with OXDB(ENV['DW_NAME']) as oxdb:
        for statement in statements:
            oxdb.execute(statement)
        # You can commit as you go along
        oxdb.commit()
        # Or will commit on exit if not errors.
        # Will rollback if an exception is raised within the with block.
```

# Snowflake Database:
Configuration and Connection parameters are maintained separately by snowflake for ODBC connection.
- Configuration parameters file : simba.snowflake.ini
- Connection parameters file:odbc.ini

Python snowflake connector can also be used to connect to Snowflake.
- reference docs : https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-snowflake-connector-methods
