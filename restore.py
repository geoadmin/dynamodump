# -*- utf-8 -*-

## USAGE: venv/bin/python restore.py {timestamp}

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, GlobalKeysOnlyIndex
import json
import sys
import os
import time
import zipfile
import shutil
from log import create_dynamo_logger


def open_file_data(file_name):
    try:
        path_data = DUMP_DIR + '/' + file_name
        file_data = open(path_data, 'r')
    except IOError as e:
        print 'The following path doesn\'t exists: %s' %path_data
        raise e
    return file_data

def split_data(data, n):
    # data is a list of object litterals
    for i in xrange(0, len(data), n):
        yield data[i:i+n]

def create_batch_lists(table, chunk):
    try:
        with table.batch_write() as batch_list:
            for row in chunk:
                if len(row['url']) <= 2046:
                    batch_list.put_item(data={
                        'url_short':  row['url_short'],
                        'url':  row['url'],
                        'timestamp': row['timestamp']
                    })
    except Exception as e:
        print e
        raise e

def write_data(file_name):
    try:
        file_data = open_file_data(file_name)
        data = json.load(file_data)
        # Load data as batches of 25 items (maximum value) and should not exceed 1Mb
        for chunk in split_data(data, 25):
            create_batch_lists(table, chunk)
    except Exception as e:
        print e
        raise e
    finally:
        file_data.close()


if __name__ == '__main__':
    DUMP_DIR = '/var/backups/dynamodb/' + sys.argv[1]
    TABLE_NAME = 'short_urls'
    JSON_INDENT = 2

    t0 = time.time()

    logger = create_dynamo_logger('restore')

    # Extract zipfile
    try:
        os.mkdir(DUMP_DIR)
    except OSError as e:
        print e
        logger.error('Can\'t create dump directory %s' %DUMP_DIR)
        logger.error(e)
        sys.exit(1)
    try:
        fh = open(DUMP_DIR + '.zip', 'rb')
        zipf = zipfile.ZipFile(fh)
        zipf.extractall(DUMP_DIR)
        fh.close()
    except Exception as e:
        print e
        logger.error('Can\'t extract file from %s.zip' %DUMP_DIR)
        logger.error(e)
        sys.exit(1)

    # Get data files
    files_names = os.listdir(DUMP_DIR)
    filter_data_files = lambda x: x.startswith('data_')
    files_names = filter(filter_data_files, files_names)

    table = None
    try:
        # Recreate the table
        table = Table.create(TABLE_NAME, schema=[
            HashKey('url_short'),
        ], throughput={
            'read': 15,
            'write': 20
        },
        global_indexes=[
            GlobalKeysOnlyIndex('UrlIndex', parts=[
                HashKey('url')
            ], throughput={
                'read': 15,
                'write': 20
            })
        ])

        ## Wait 30 secs for the table to create
        time.sleep(30)
        map(write_data, files_names)
    except Exception as e:
        print e
        logger.error('An error occured during DB restoration')
        logger.error(e)
        sys.exit(1)
    finally:
        shutil.rmtree(DUMP_DIR)
        tf = time.time()
        toff = tf - t0
        logger.info('It took: %s seconds' %toff)
