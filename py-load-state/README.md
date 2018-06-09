LoadState
---------
For working with your load state table.

Requires a database connection. Tested to work with Vertica, sqlite3.


Usage
-----
```python
from ox_load_state import LoadState
from ox_dw_db import OXDB

db_conn = OXDB(DSN, SCHEMA).connection
VARIABLE_NAME = 'my_load_state_variable_name'

load_state = LoadState(db_conn, variable_name=VARIABLE_NAME)

# Quick usage
load_state.upsert('2016-10-01', commit=true)

# Or if only want to commit after a complete transaction. This will automatically commit if there are no errors:
with OXDB(DSN, SCHEMA) as oxdb:
    load_state = LoadState(oxdb.connection, variable_name=VARIABLE_NAME)
    Do some work...
    load_state.upsert('2016-10-01')

```
