"""
Utilities for managing downloads.
"""
import os
from xml.parsers.expat import ExpatError
import requests
import warnings
import xmltodict
from retrying import retry
from .exceptions import MissingMetaFile
from .settings import (CACHE_META_DATA, DEFAULT_DATA_DIR, MAX_ATTEMPTS,
                       WAIT_BETWEEN_ATTEMPTS, PROXIES)
warnings.filterwarnings('ignore', 'Unverified HTTPS request')


@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_fixed=WAIT_BETWEEN_ATTEMPTS)
def download_file(config, file_obj):
    """
    Download a file from url to file_name.
    :return boolean:
    """
    response = requests.get(
        file_obj.url,
        stream=True,
        auth=(config.get('ODFI_USER'), config.get('ODFI_PASS')),
        proxies=config.get('PROXIES', PROXIES),
        verify=False)
    response.raise_for_status()

    with open(file_obj.file_name, 'wb') as out_file:
        for block in response.iter_content(1024):
            out_file.write(block)

    return file_obj.is_ready


def get_download_dir(config, feed_name=None, serial=None):
    """
    Where the download files are to be put including part files, schema file
    and cached metadata.
    """
    items = [
        str(item)
        for item in
        [config.get('DATA_DIR', DEFAULT_DATA_DIR), feed_name, serial]
        if item is not None
    ]
    return os.path.join(*items)


def get_xml_meta(config, download_dir, name='meta.xml', url=None, params=None):
    """
    Will look for the cache file first and return that as dict.
    If it doesn't exist then it will query ODFI and write the results to
    the cache file.
    :param config: ODFI config.
    :param xml_file: The path/file for the disk cache.
    :param url: ODFI Url of the xml file.
    :param params: Optional params for the ODFI query.
    :return dict: XML same as what is in the cache file.
    """
    xml_file = os.path.join(download_dir, name)
    try:
        with open(xml_file, 'r') as in_file:
            return xmltodict.parse(in_file.read())
    except IOError:
        if url is not None:
            doc = get_xml(config, url, params=params)
            if config.get('CACHE_META_DATA', CACHE_META_DATA):
                if not os.path.exists(download_dir):
                    os.makedirs(download_dir)
                with open(xml_file, 'w') as out_file:
                    out_file.write(xmltodict.unparse(doc, pretty=True))
            return doc
        else:
            raise MissingMetaFile(xml_file)
    except ExpatError:
        # Something wrong with the meta.xml so let's replace it.
        if os.path.exists(xml_file):
            os.remove(xml_file)
        return get_xml_meta(config, download_dir, url=url, params=params)


@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_fixed=WAIT_BETWEEN_ATTEMPTS)
def get_xml(config, url, params=None):
    """
    :param config: ODFI config.
    :param url: ODFI Url of the xml file.
    :param params: For query string arguments.
    :return dict: XML pulled from url to dict.
    """
    response = requests.get(
        url,
        params=params,
        auth=(config.get('ODFI_USER'), config.get('ODFI_PASS')),
        proxies=config.get('PROXIES', PROXIES),
        verify=False)
    response.raise_for_status()

    return xmltodict.parse(response.text)
