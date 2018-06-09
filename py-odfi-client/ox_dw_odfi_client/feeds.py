"""
With the Feeds class you can get information on any available feed.
"""
import os
from .download import get_xml_meta, get_download_dir
from .feed import Feed
from .settings import FEEDS_REST_PATH, MAX_FEEDS


class Feeds(object):
    """
    This contains all of the configured feeds kept in the class variable
    cache for subsequent calls without incurring overhead.
    """
    _meta = None
    _feeds = {}

    def __init__(self, config):
        self.config = config
        self.download_dir = get_download_dir(self.config)
        self.feeds_url = os.path.join(
            self.config.get('ODFI_HOST').rstrip(os.path.sep),
            self.config.get('FEEDS_REST_PATH', FEEDS_REST_PATH).strip(
                os.path.sep))

    @property
    def meta(self):
        """
        The metadata for the odfi feeds.
        """
        if self._meta is None:
            self._set_meta()

        return self._meta

    def get(self, feed_name, clear_cache=False):
        """
        Returns a feed by name.
        If the feed doesn't exist this will attempt to refresh the meta_file
        and try again. If it still doesn't exist then raise error.
        :param string: The name of the feed.
        :return Feed: The feed of the name given.
        :raises KeyError:
        """
        if clear_cache:
            self._set_meta(True)
        if feed_name not in self._feeds:
            for feed in self.meta['feeds']['feedRef']:
                if feed_name == feed.get('@name'):
                    self._feeds.update({
                        feed.get('@name'):
                        Feed(self.config, feed.get('@name'), feed.get('@uri'))
                    })
                    break
            if feed_name not in self._feeds:
                if clear_cache:
                    raise KeyError("No such feed %s!" % feed_name)
                else:
                    return self.get(feed_name, True)

        return self._feeds.get(feed_name)

    __getitem__ = __getattr__ = get

    def __iter__(self):
        """
        :return generator: All of the Feeds.
        """
        for feed in self._feeds.values():
            yield feed

    def _set_meta(self, clear_cache=False):
        """
        Sets the meta data for the feeds.
        """
        if clear_cache:
            xml_file = os.path.join(self.download_dir, 'meta.xml')
            if os.path.exists(xml_file):
                os.remove(xml_file)
        self._meta = get_xml_meta(
            self.config,
            self.download_dir,
            name='meta.xml',
            url=self.feeds_url,
            params={
                'max': MAX_FEEDS
            })
