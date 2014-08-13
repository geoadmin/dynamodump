# -*- utf-8 -*-

## USAGE: venv/bin/python restore.py {timestamp}

import boto
import json
import sys
import os
import time
import zipfile
import shutil
from log import create_dynamo_logger
#from multiprocessing import Pool


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

def create_batch_lists(conn, table, chunk):
    batch_items = []
    batch_list = conn.new_batch_write_list()
    for row in chunk:
        dynamo_row = table.new_item(
            hash_key = row['url_short'],
            attrs = {
                'url': row['url'],
                'timestamp': row['timestamp']
            })
        batch_items.append(dynamo_row)
    batch_list.add_batch(table, batch_items)
    yield batch_list

def process_batch_list(conn, batch_list):
    response = conn.batch_write_item(batch_list)
    if response['UnprocessedItems']:
        # print something if dict is not empty
        print response['UnprocessedItems']

def write_data(file_name):
    try:
        file_data = open_file_data(file_name)
        data = json.load(file_data)
        # Load data as batches of 25 items (maximum value) and should not exceed 1Mb
        for chunk in split_data(data, 25):
            for batch_list in create_batch_lists(conn, table, chunk):
                process_batch_list(conn, batch_list)
    except Exception as e:
        raise e
    finally:
        file_data.close()


if __name__ == '__main__':
    DUMP_DIR = '/var/backups/dynamodb/' + sys.argv[1]
    TABLE_NAME = 'shorturls'
    JSON_INDENT = 2

    t0 = time.time()

    logger = create_dynamo_logger('restore')

    try:
        conn = boto.connect_dynamodb()
    except Exception as e:
        logger.error('An error occured during DB connection')
        logger.error(e)
        sys.exit(1)

    if TABLE_NAME in conn.list_tables():
        logger.error('Table %s already exists, please make sure you want need to restore it.' %TABLE_NAME)
        sys.exit(1)

    # Extract zipfile
    try:
        os.mkdir(DUMP_DIR)
    except OSError as e:
        logger.error('Can\'t create dump directory %s' %DUMP_DIR)
        logger.error(e)
        sys.exit(1)
    try:
        fh = open(DUMP_DIR + '.zip', 'rb')
        zipf = zipfile.ZipFile(fh)
        zipf.extractall(DUMP_DIR)
        fh.close()
    except Exception as e:
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
        schema = conn.create_schema(hash_key_name='url_short',hash_key_proto_value='S')
        table = conn.create_table(name=TABLE_NAME, schema=schema, read_units=25, write_units=500)

        ## Wait 25 secs for the table to create
        time.sleep(25)
        # If fast restore is necessary increase write unit to 20000 and use:
        # pool = Pool(processes=len(files_names))
        # pool.map(write_data, files_names)
        map(write_data, files_names)
    except Exception as e:
        logger.error('An error occured during DB restoration')
        logger.error(e)
        sys.exit(1)
    finally:
        if table:
            table.update_throughput(25, 25)
        shutil.rmtree(DUMP_DIR)
        tf = time.time()
        toff = tf - t0
        logger.info('It took: %s seconds' %toff)
