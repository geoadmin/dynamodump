# -*- coding: utf-8 -*-

## USAGE: venv/bin/python dump.py

import boto
import json
import time
import datetime
import os 
import sys
import zipfile
import shutil
from log import create_dynamo_logger


def save_schema():
    f = None
    try:
        f = open(DUMP_DIR + FOLDER_NAME + '/schema.json', 'w+')
        f.write(json.dumps(table_desc, indent=JSON_INDENT))
    except Exception as e:
        logger.error('An error occured while saving the schema')
        raise e
    finally:
        if f:
            f.close()

def save_data():
    table = None
    f = None
    counter = 0
    try:
        table = conn.get_table('shorturls')
        table.update_throughput(50, 25)
        maxAttempts = 6
        attempts = 0
        # Wait until throughtput is updated
        while True:
            try:
                scanned_table = table.scan()
                break
            except Exception as e:
                logger.info('Trying to scan the table...')
                time.sleep(5)
                attempt += 1
                if attempts == maxAttempts:
                    logger.error('Unable to scan the table')
                    logger.error(e)
                    raise e
        scanned_table = table.scan()

        # Don't write more than 100'000 items per file
        file_count = 1
        filename = PREFIX_NAME + str(file_count) + '.json'
        f = open(DUMP_DIR + FOLDER_NAME + '/' + filename, 'w+')

        # Needs to be a list for json.loads to work properly
        f.write('[')
        for col in scanned_table:
            json.dump(col, f, indent=JSON_INDENT)
            counter += 1
            if counter%100000 == 0 or counter == nb_items:
                f.write(']')
                f.close()
                file_count += 1
                filename = PREFIX_NAME + str(file_count) + '.json'
                if counter != nb_items:
                    f = open(DUMP_DIR + FOLDER_NAME + '/' + filename, 'w+')
                    f.write('[')
            else:
                f.write(',')
    except Exception as e:
        logger.error('An error occured while writing the json files')
        raise e
    finally:
        if table:
            table.update_throughput(25, 25)
        if f:
            f.close()

def zip_dir(zipname, dir_to_zip, dump_dir):
    try:
        dir_to_zip_len = len(dir_to_zip.rstrip(os.sep)) + 1
        with zipfile.ZipFile(dump_dir + zipname, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for dirname, subdirs, files in os.walk(dir_to_zip):
                for filename in files:
                    path = os.path.join(dirname, filename)
                    entry = path[dir_to_zip_len:]
                    zf.write(path, entry)
    except Exception as e:
        logger.error('An error occured while zipping the directory')
        raise e

def manage_retention(dumpDir):
    # A maximum a 30 dumps
    MAX_RENTENTION = 30
    try:
        dumps = os.listdir(dumpDir)
        dumps.sort()
    except OSError as e:
        logger.error('Dump directory %s doesn\'t exit' %dumpDir)

    if len(dumps) > 0:
        cleandumps = lambda x: shutil.rmtree(dumpDir + x)
        if dumps[len(dumps) - 1].startswith('lost'):
            dumps.pop(len(dumps) - 1)
        map(cleandumps, dumps[0:len(dumps) - MAX_RENTENTION])

if __name__ == '__main__':
    t0 = time.time()
    DUMP_DIR = '/var/backups/dynamodb/'
    PREFIX_NAME = 'data_'
    TABLE_NAME = 'shorturls'
    JSON_INDENT = 2
    FOLDER_NAME = datetime.datetime.fromtimestamp(t0).strftime('%Y%m%d')

    logger = create_dynamo_logger('dump')

    logger.info('Starting dump creation...')

    try:
        os.mkdir(DUMP_DIR + FOLDER_NAME)
    except OSError as e:
        logger.info(e)
        logger.info('Removing directory...')
        shutil.rmtree(DUMP_DIR + FOLDER_NAME)

    try:
        conn = boto.connect_dynamodb()
    except Exception as e:
        logger.error(e)
        logger.error('DUMP_STATUS=1')
        sys.exit(1)

    try:
        table_desc = conn.describe_table(TABLE_NAME)
        nb_items = table_desc['Table']['ItemCount']
        save_schema()
        save_data()
        zip_dir(FOLDER_NAME + '.zip', DUMP_DIR + FOLDER_NAME, DUMP_DIR)
        shutil.rmtree(DUMP_DIR + FOLDER_NAME)
    except Exception as e:
        tf = time.time()
        toff = tf - t0
        logger.error(e)
        logger.error('It took: %s seconds' %toff)
        logger.error('DUMP_STATUS=1')
        sys.exit(1)

    manage_retention(DUMP_DIR)

    tf = time.time()
    toff = tf - t0
    logger.info('It took: %s seconds' %toff)
    logger.info('DUMP_STATUS=0')
