"""
Common constants here.
"""

# Default path for the ODFI rest feeds
FEEDS_REST_PATH = '/ODFI/rest/feeds'

# The default on the ODFI side is 10 so this must be used.
MAX_FEEDS = 10000

# Default path for ODFI rest queries
QUERY_REST_PATH = '/ODFI/rest/query'

# Default odfi version
DEFAULT_ODFI_VERSION = 2
DEFAULT_SORT_KEY = 'readable_interval'

# Maximum default con-current downloaders
MAX_DOWNLOADERS = 10

# Downloads
CACHE_META_DATA = False
DEFAULT_DATA_DIR = 'odfi'
MAX_ATTEMPTS = 5
WAIT_BETWEEN_ATTEMPTS = 2000  # ms

# By default do not use proxies
PROXIES = {
    "http": None,
    "https": None,
}
