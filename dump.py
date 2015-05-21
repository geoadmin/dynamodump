#!/usr/bin/env python
# -*- coding: utf-8 -*-

## USAGE: venv/bin/python dump.py

from boto.dynamodb import connect_to_region
from boto.dynamodb.exceptions import DynamoDBResponseError
import json
import time
import datetime
import os 
import sys
import zipfile
import shutil
import getopt
from log import create_dynamo_logger

BASE_DIR = '/var/backups/dynamodb/'
PREFIX_NAME = 'data_'
JSON_INDENT = 2

logger = create_dynamo_logger('dump')



def save_schema(dump_dir, table_desc):
    f = None
    try:
        f = open(os.path.join(dump_dir,  'schema.json'), 'w+')
        f.write(json.dumps(table_desc, indent=JSON_INDENT))
        logger.info('schema.json has been created')
    except Exception as e:
        logger.error('An error occured while saving the schema')
        raise e
    finally:
        if f:
            f.close()

def save_data(conn, table_name, dump_dir):
    table = None
    f = None
    counter = 0
    try:
        table = conn.get_table(table_name)
        if table.read_units != 20:
            try:
                print 'Updating throughput'
                table.update_throughput(20, 5)
            except DynamoDBResponseError:
                print 'Cannot updatetable throuput'
        maxAttempts = 6
        attempts = 0
        # Wait until throughtput is updated
        while True:
            try:
                scanned_table = table.scan()
                break
            except Exception as e:
                logger.info('Trying to scan the table...')
                time.sleep(20)
                attempt += 1
                if attempts == maxAttempts:
                    logger.error('Unable to scan the table')
                    logger.error(e)
                    raise e

        scanned_table = table.scan()

        # Don't write more than 100'000 items per file
        file_count = 1
        filename = PREFIX_NAME + str(file_count) + '.json'
        f = open(os.path.join(dump_dir, filename), 'w+')

        # Needs to be a list for json.loads to work properly
        f.write('[')
        print 'Start dump'
        for col in scanned_table:
            json.dump(col, f, indent=JSON_INDENT)
            counter += 1
            if counter%100000 == 0:
                f.write(']')
                f.close()
                file_count += 1
                filename = PREFIX_NAME + str(file_count) + '.json'
                f = open(os.path.join(dump_dir , filename), 'w+')
                f.write('[')
            else:
                f.write(',')

        # Remove last comma
        f.seek(-1, os.SEEK_END)
        f.truncate()
        f.write(']')
        f.close()
        logger.info('All entries have been saved')
    except Exception as e:
        logger.error('An error occured while writing the json files')
        raise e
    finally:
        if table and table.read_units != 10:
            try:
                print 'Back to initial throughput'
                table.update_throughput(10, 5)
            except DynamoDBResponseError:
                print "Cannot bring back to throuput"
        if f:
            f.close()

def zip_dir(zipname, dir_to_zip, dump_dir):
    print 'Zipping %s directory into %s' %(dir_to_zip, zipname)
    try:
        dir_to_zip_len = len(dir_to_zip.rstrip(os.sep)) + 1
        with zipfile.ZipFile(dump_dir + '/' + zipname, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
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
        raise e

    if len(dumps) > 0:
        cleandumps = lambda x: os.remove(dumpDir + x)
        # Remove lost and founds
        if dumps[len(dumps) - 1].startswith('lost'):
            dumps.pop(len(dumps) - 1)
        files2Remove = len(dumps) - MAX_RENTENTION
        if files2Remove > 0:
            map(cleandumps, dumps[0:files2Remove])

def dump(table_name):
    t0 = time.time()
    folder_name = datetime.datetime.fromtimestamp(t0).strftime('%Y%m%d')
    dump_dir = os.path.join(BASE_DIR, table_name, folder_name)
    print 'Dumping into %s' %dump_dir

    logger.info('Starting dump creation for table=%s...' % table_name)


    try:
        os.makedirs(dump_dir)
    except OSError as e:
        print 'Error during dump creation: %s' %e
        logger.info(e)
        logger.info('Removing directory...')
        shutil.rmtree(dump_dir)
        sys.exit(2)

    try:
        conn = connect_to_region(region_name='eu-west-1')
    except Exception as e:
        print 'Error during dump creation: %s' %e
        logger.error(e)
        logger.error('DUMP_STATUS=1')
        sys.exit(1)

    try:
        table_desc = conn.describe_table(table_name)
        save_schema(dump_dir, table_desc)
        save_data(conn, table_name, dump_dir)
        zip_dir(folder_name + '.zip', dump_dir, BASE_DIR + table_name)
        print 'Manage retention in dir %s' %dump_dir
        shutil.rmtree(dump_dir)
        manage_retention(BASE_DIR + table_name)
    except Exception as e:
        tf = time.time()
        toff = tf - t0
        print 'Error during dump creation: %s' %e
        logger.error(e)
        logger.error('It took: %s seconds' %toff)
        logger.error('DUMP_STATUS=1')
        sys.exit(1)

    tf = time.time()
    toff = tf - t0
    print 'Success! It took %s seconds' %toff
    logger.info('Success! It took: %s seconds' %toff)
    logger.info('DUMP_STATUS=0')

def usage():
    print "Dump a dynamoDB table in 'eu-west-1' to /var/backups/dynamodb"
    print ""
    print "Usage: dump.py [-h] [-t table_name]"
    print ""
    print "optional arguments:"
    print "-h, --help               show this message and exit"
    print "-t TABLE, --table=TABLE  dump table TABLE (default to 'shorturl')"

def main(argv):

    TABLE_NAME = 'shorturl'
    try:                                
        opts, args = getopt.getopt(argv, "ht:", ["help", "table="])
    except getopt.GetoptError:           
        usage()                          
        sys.exit(2) 
    for opt, arg in opts:                
        if opt in ("-h", "--help"):      
            usage()                     
            sys.exit()                  
        elif opt in ("-t", "--table"): 
            TABLE_NAME = arg               

    dump(TABLE_NAME)

if __name__ == '__main__':
    main(sys.argv[1:])
