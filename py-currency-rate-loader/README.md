Currency Rate Client
--------------------
  - Grabs row data from plain csv text data on the rest api

Loader
------
Will load/merge available data. Starts from the last day loaded in the DB by default until current date.

Downloads
---------
The files are downloaded into ```$APP_ROOT/output/```

Usage
-----
From command line run ```currency_rate_loader```

Safe to run as often as you like. Uses a PidFile lock and will error if it cannot achieve a lock. Will query DB for date range needed with each run unless provided.
