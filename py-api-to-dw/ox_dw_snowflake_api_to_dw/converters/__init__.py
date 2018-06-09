"""
Here we can have the mappings for the converters.
The mapped functions must take a row iterator as an argument and return an
iterator in turn.
"""
from .line_item_targeting import get_converted_rows as line_item_targeting

CONVERTERS = {
    'line_item_targeting': line_item_targeting
}
