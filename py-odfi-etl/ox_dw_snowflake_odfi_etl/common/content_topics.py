"""
For loading content topics from a new source table.
"""
import os
import csv
import tempfile
from .settings import APP_CONF_ROOT, STAGE_NAME, get_conf

CONFIG = get_conf('content_topics', APP_CONF_ROOT)


def get_content_topics(dbh, logger):
    """
    Fetch content topics using supplie dbh.
    """
    logger.debug(CONFIG['CONTENT_TOPIC_SELECT_SQL'])
    for row in dbh.cursor().execute(CONFIG['CONTENT_TOPIC_SELECT_SQL']):
        yield row


def load_content_topics(source_table, dbh, logger):
    """
    With a given source table name will parse new topics and load them into
    the content topic dim/group tables.
    """
    logger.debug(CONFIG['CONTENT_TOPIC_GROUP_SQL'] % source_table)
    dbh.cursor().execute(CONFIG['CONTENT_TOPIC_GROUP_SQL'] % source_table)
    outfile_name = tempfile.NamedTemporaryFile(suffix='.csv').name
    try:
        with open(outfile_name, 'w') as outfile:
            writer = csv.writer(outfile)
            writer.writerow([
                'content_topic_group_id_string', 'content_topic_group_sid',
                'platform_id'
            ])
            for (content_topic_group_id_string, content_topic_group_sid,
                 platform_id) in get_content_topics(dbh, logger):
                valid_topics = 0
                for topic in content_topic_group_id_string.split(','):
                    # To test if the topic col is valid integer value
                    try:
                        if len(topic) > 8:
                            continue
                        _ = int(topic)
                    except ValueError:
                        continue
                    valid_topics += 1
                    writer.writerow(
                        [str(content_topic_group_sid), platform_id, topic])
        stage = "@%s/%s" % (STAGE_NAME,
                            os.path.join('content_topics', source_table))
        for stmt in ("REMOVE %s" % stage,
                     "PUT file://%s %s AUTO_COMPRESS = TRUE" % (outfile_name,
                                                                stage)):
            logger.debug(stmt)
            dbh.cursor().execute(stmt)
        for stmt in CONFIG['CONTENT_TOPIC_LOAD_SQL']:
            logger.debug(stmt % {'STAGED_DATA': stage})
            dbh.cursor().execute(stmt % {'STAGED_DATA': stage})
        dbh.cursor().execute("REMOVE %s" % stage)
        dbh.commit()
    finally:
        if os.path.exists(outfile_name):
            os.remove(outfile_name)
