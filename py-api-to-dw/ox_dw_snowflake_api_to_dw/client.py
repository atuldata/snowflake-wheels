"""
This is our custom Rabbit MQ client.
Use get_api_client() as it will setup the consumer name for you
from the config.
"""
import os
import yaml
import pid
from ox_rabbit_mq import RabbitClient
from ox_dw_logger import get_etl_logger
from .schema_config import SchemaConfig
from .settings import APP_NAME, LOCK_ROOT, LOG_DIR
from .workers import APIExtractorWorker


def get_api_client(config_file, schema_file, name=None):
    """
    Returns the api_client. Default name will be taken from the config file.
    """
    if not os.path.isfile(config_file):
        raise EnvironmentError("Config file not found %s" % config_file)
    if not os.path.isfile(schema_file):
        raise EnvironmentError("Schema config file not found %s" % schema_file)
    if name is None:
        name = \
            '-'.join(
                [APP_NAME, os.path.splitext(os.path.basename(config_file))[0]])
    conf = yaml.load(open(config_file, 'r'))

    return \
        APIExtractorClient(
            name,
            conf['RABBITMQ'],
            conf['QUEUES'],
            SchemaConfig(schema_file))


class APIExtractorClient(RabbitClient):
    """
    This is our custom Rabbit MQ client. Behaves primary as a consumer.
    """

    def __init__(self, name, conf, queues, schema_config):
        logger = get_etl_logger(name, LOG_DIR)
        super(APIExtractorClient, self).__init__(
            name, conf, queues, logger=logger)
        self.schema_config = schema_config
        self.lock = pid.PidFile(pidname="%s.LOCK" % APP_NAME,
                                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)

        self._worker = None

    @property
    def worker(self):
        """
        Our custom worker.
        """
        if self._worker is None:
            self._worker = \
                APIExtractorWorker(self.name, self.schema_config, self.logger)

        return self._worker
