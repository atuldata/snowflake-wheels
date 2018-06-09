Content Topic Loader
--------------------
  - Content Topics are managed by https://wiki.corp.openx.com/display/TECH/Adding+Content+Topics
  - Use Slack role ox3-customers-json to bring them locally to your machine.
  - Currently stored locally in a hardcoded path /etc/ox/customers

Loader
------
  - Will periodically read in the json files and merge into the DW.

Usage
-----
From command line run ```content_topic_loader```

Safe to run as often as you like. Uses a pidlock and will error if it cannot achieve a lock.
