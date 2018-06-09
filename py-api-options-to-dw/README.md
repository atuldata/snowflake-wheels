API Options to DW
=================
There are options json file in git that the API team manages. They are currently available on Riak via rest api. This app does not use the Riak interface but rather simple gets to the rest api.

api_options_to_dw
-----------------
This will run for all configured tables or you can pass it a list of one or more table. The ordering is determined in the config file. Any tables on the command line that are not in the config file will not get pulled as configuration is absolutely needed.

LOCKS
-----
Locks are done at the table level so you could run this script asynchronously with different table options.

PERMUTATIONS
------------
You can configure a table to have a source as a DW table, and produce permutations of a key column and one other column. This works well for tables with low cardinality. Has not been tested for more than 6 keys.

See example_config.yaml

CONFIG
------
The configuration file by default is in $APP_ROOT/conf/api-options-to-dw.yaml
You can use your own with the -c flag. Each table is entered as a list item.

See example_config.yaml

Troubleshooting
---------------
All of the output files for loads including any rejected or exception from the DW are in the $APP_ROOT/output/api-options-to-dw/ folder.
