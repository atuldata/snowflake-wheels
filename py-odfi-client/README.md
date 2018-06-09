ODFI Client
-----------
  - Reads xml metadata for Feeds, Feed and datasets
  - Can query for the latest dataset or from serial number
  - Can download the partfiles and schema for a given Feed/Serial

Dataset Ordering
----------------
The datasets are ordered by the startTimestamp and NOT the serial. This way if there are 2 serials with the same startTimestamp only the latest one will be returned. So an older serial will be replaced by a new one with the same startTimestamp.

Downloads
---------
The files are downloaded into ```/<DATA_DIR>/<Feed Name>/<Serial>/...```

Default DATA_DIR is ```<current directory>/odfi```

For a given Feed/Serial the downloads of each part file will be in parrallel.

Caching
-------
For downloads we certainly need to have ODFI connectivity, but for uploads no ODFI connectivity is needed. As long as the downloads were succesful, all of the meta data and schema is cached to disk along side the part file downloads.

Usage For Downloads
-------------------
```python
#!/usr/bin/env python
import logging
from ox_odfi_client import (
    Feeds,
    DataSizeMismatchException,
    MD5MismatchException,
    NoDataSetException
)

CONFIG = {
    'ODFI_HOST': 'http://<host>',  # Required
    'ODFI_USER': '<username>',  # Required
    'ODFI_PASS': '<password>',  # Required
    'ODFI_VERSION': 2,  # Optional, Defaults to 2
    'MAX_DATASETS': 5,  # Optional, Defaults to unlimited
    'MAX_DOWNLOADERS': 5,  # Optional Defaults to 10; For concurrent downloads
    'DATA_DIR': '</some/path>',  # Optional, Defaults to <current directory>/odfi
    'FEEDS_REST_PATH': '</some/path>', Optional, Defaults to <settings.FEEDS_REST_PATH>
    'QUERY_REST_PATH': '</some/path>', Optional, Defaults to <settings.QUERY_REST_PATH>
    'CACHE_META_DATA': <boolean>, Optional, Defaults to False
}

FEEDS_TO_DOWNLOAD = [
    ('AdaptiveInsightsDemandReport', '550'),
    ('RTBReport', '1890')
]

feeds = Feeds(CONFIG)
for feed_name, serial in FEEDS_TO_DOWNLOAD:
    try:
        for dataset in feeds[feed_name].get_datasets_since_serial(serial):
            dataset.attempt_download()
    except NoDataSetException as exception:
        logging.warning(exception)
    except (DataSizeMismatchException, MD5MismatchException, IOError) as error:
        logging.error(error)
```

Usage For Uploads or Reporting
------------------------------
```python
#!/usr/bin/env python
import logging
from ox_odfi_client import (
    Feeds,
    DataSizeMismatchException,
    MD5MismatchException,
    NoDataSetException
)

CONFIG = {
    'ODFI_HOST': 'http://<host>',  # Required
    'ODFI_USER': '<username>',  # Required
    'ODFI_PASS': '<password>',  # Required
    'ODFI_VERSION': 2,  # Optional, Defaults to 2
    'MAX_DATASETS': 5,  # Optional, Defaults to 10
    'MAX_DOWNLOADERS': 5,  # Optional Defaults to 10; For concurrent downloads
    'DATA_DIR': '</some/path>',  # Optional, Defaults to <current directory>/odfi
    'FEEDS_REST_PATH': '</some/path>', Optional, Defaults to <settings.FEEDS_REST_PATH>
    'QUERY_REST_PATH': '</some/path>', Optional, Defaults to <settings.QUERY_REST_PATH>
    'CACHE_META_DATA': <boolean>, Optional, Defaults to False
}

FEEDS_TO_DOWNLOAD = [
    ('AdaptiveInsightsDemandReport', '550'),
    ('RTBReport', '1890')
]

feeds = Feeds(CONFIG)
for feed_name, serial in FEEDS_TO_DOWNLOAD:
    try:
        dataset = feeds[feed_name].get_dataset_by_serial(serial):
        if dataset.is_download_complete:
            pass  # Do work
    except (DataSizeMismatchException, MD5MismatchException, IOError) as exception:
        logging.warning(exception)
```
