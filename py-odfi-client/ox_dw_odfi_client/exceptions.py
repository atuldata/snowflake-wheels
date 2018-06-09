"""
Exception Classes here.
"""


class DataSizeMismatchException(Exception):
    """
    If the file size doesn't match.
    """

    def __init__(self, file_obj, file_size):
        super(DataSizeMismatchException, self).__init__(
            "Feed: %s Serial: %s - size mismatch %d != %d(expected) for file: "
            "%s" % (file_obj.name, file_obj.serial, file_size,
                    file_obj.data_size, file_obj.file_name))


class MD5MismatchException(Exception):
    """
    If the md5 hash doesn't match digest.
    """

    def __init__(self, file_obj, digest):
        super(MD5MismatchException, self).__init__(
            "Feed: %s Serial: %s - md5 sum mismatch %s != %s(expected) for "
            "file: %s" % (file_obj.name, file_obj.serial, digest,
                          file_obj.digest, file_obj.file_name))


class MissingMetaFile(Exception):
    """
    If the meta.xml is missing.
    """

    def __init__(self, meta_xml_file):
        super(MissingMetaFile, self).__init__(
            "Missing %s. You will need to re-download this dataset." %
            (meta_xml_file))


class NoDataSetException(Exception):
    """
    If no data sets are returned due to the args.
    """

    def __init__(self, uri, params):
        super(NoDataSetException, self).__init__(
            "URI: %s; Params: %s returned no results!" % (uri, params))
