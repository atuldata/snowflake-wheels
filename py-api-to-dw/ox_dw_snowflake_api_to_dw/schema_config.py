"""
Reads in the config file and allow access to it.
"""
import yaml

from .settings import DESTINATIONS


class SchemaConfig(object):
    """
    Parses your schema config file and allows access to it's elements.
    """

    _configured_objects = None
    _configured_tables = None

    def __init__(self, schema_config_file):
        self.schema_config_file = schema_config_file
        self.config = yaml.load(open(self.schema_config_file, 'r'))
        self.action_types = self.config.get('ACTION_TYPES')
        self.object_config = self.config.get('OBJECT_CONFIG')
        self.schemas = self.config.get('SCHEMAS')

    @property
    def configured_objects(self):
        """
        Returns a set of the objects from the schema_config file.
        """
        if self._configured_objects is None:
            self._configured_objects = \
                set(self.config.get('OBJECT_CONFIG').get('type').keys())

        return self._configured_objects

    @property
    def configured_tables(self):
        """
        Return the set of configured tables.
        """
        if self._configured_tables is None:
            self._configured_tables = set()
            for table_name in self._get_configured_tables():
                self._configured_tables.add(table_name)

        return self._configured_tables

    def get_column_names(self, table_name):
        """
        Returns a ordered tuple of column names (col1, col2,,,).
        """
        return tuple(col[0] for col in self.get_columns(table_name))

    def get_columns(self, table_name):
        """
        Returns an ordered list of tuple pairs [(col_name, col_def),,]
        """
        return self.schemas.get(table_name)

    def get_obj_tables(self, obj, conf=None):
        """
        Recursively looks for DESTINATIONS based on obj contents.
        """
        if conf is None:
            conf = self.object_config.get('type').get(obj.get('type'), {})
        for key, value in conf.items():
            if key == DESTINATIONS:
                for table_name in value:
                    yield table_name
            elif key in obj:
                new_key = obj.get(key)
                if isinstance(value, dict):
                    for _ in self.get_obj_tables(obj, value):
                        yield _
                    if isinstance(new_key, (int, str)) and new_key in value:
                        for _ in self.get_obj_tables(obj, value.get(new_key)):
                            yield _
                elif isinstance(value, list) and bool(obj.get(key)):
                    for list_value in value:
                        for _ in self.get_obj_tables(obj, list_value):
                            yield _
                        for index, item in list_value.items():
                            if isinstance(index, int) \
                                    and index < len(obj.get(key)):
                                if obj.get(key)[index] in item:
                                    for _ in self.get_obj_tables(
                                            obj,
                                            item.get(obj.get(key)[index])):
                                        yield _

    def _get_configured_tables(self, conf=None):
        """
        Private. Use the configured_tables property as it will cache a set of
        distinct table names.
        """
        if conf is None:
            conf = self.object_config
        for key, value in conf.items():
            if key == DESTINATIONS:
                for table_name in value:
                    yield table_name
            elif isinstance(value, dict):
                for _ in self._get_configured_tables(value):
                    yield _
            elif isinstance(value, list) and bool(value):
                for _ in self._get_configured_tables(value[0]):
                    yield _
