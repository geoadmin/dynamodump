# -*- utf-8 -*-

## USAGE: venv/bin/python restore.py {timestamp} {createtable} {restorefromtimestamp}

from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.fields import HashKey, GlobalKeysOnlyIndex
import json
import sys
import os
import time
import zipfile
import shutil
import getopt
from distutils.util import strtobool
from log import create_dynamo_logger

TIMESTAMP = None
TABLE_NAME = 'shorturl'
CREATE_TABLE = False
JSON_INDENT = 2



def user_yes_no_query(question):
    sys.stdout.write('%s [y/n]\n' % question)
    while True:
        try:
            return strtobool(raw_input().lower())
        except ValueError:
            sys.stdout.write('Please respond with \'y\' or \'n\'.\n')


def open_file_data(path_data):
    try:
        #path_data = DUMP_DIR + '/' + file_name
        file_data = open(path_data, 'r')
    except IOError as e:
        print 'The following path doesn\'t exists: %s' %path_data
        raise e
    return file_data

def split_data(data, n):
    # data is a list of object litterals
    for i in xrange(0, len(data), n):
        yield data[i:i+n]

def create_batch_lists(table, chunk, TIMESTAMP):
    try:
        with table.batch_write() as batch_list:
            for row in chunk:
                row_timestamp = row['timestamp'].split(' ')[0]
                if table.table_name == 'shorturl':
                    if len(row['url']) <= 2046:
                        if TIMESTAMP is None or time.strptime(row_timestamp, '%Y-%m-%d') >= TIMESTAMP:
                            batch_list.put_item(data={
                                'url_short':  row['url_short'],
                                'url':  row['url'],
                                'timestamp': row['timestamp']
                            }, overwrite=True)
                elif table.table_name == 'geoadmin-file-storage':
                    if len(row['adminId']) <= 2046:
                        if TIMESTAMP is None or time.strptime(row_timestamp, '%Y-%m-%d') >= TIMESTAMP:
                            batch_list.put_item(data={
                                'adminId':  row['adminId'],
                                'fileId':  row['fileId'],
                                'timestamp': row['timestamp']
                            }, overwrite=True)

    except Exception as e:
        print e
        raise e

def write_data(table, dump_dir, file_name, timestamp):
    try:
        file_data = open_file_data(os.path.join(dump_dir,file_name))
        data = json.load(file_data)
        # Load data as batches of 25 items (maximum value) and should not exceed 1Mb
        for chunk in split_data(data, 25):
            create_batch_lists(table, chunk, timestamp)
    except Exception as e:
        print e
        raise e
    finally:
        file_data.close()

def usage():
    print "This script a dumped dynamoBD to the given 'table' in 'eu-west-1'"
    print ""
    print "Usage: restore.py [-h --help] [-t --table=table_name] [-c --create] [-f --filter=20140805] dump_dir"
    print
    print "positional argument"
    print "dump_dir                         name of the zip file, e.g. 20140805"
    print
    print "optional arguments"
    print "-h, --help                       print this message and exits"
    print "-t TABLE, --table=TABLE          TABLE to restore. Possible values are 'shortul' (default) and 'geoadmin-file-storage'"
    print "-c, --create                     create TABLE before restoring data"
    print "-f TIMESTAMP, --filter=TIMESTAMP filter records by TIMESTAMP (format 20140805)"

def main(argv):
    table_name = 'shorturl'
    TIMESTAMP = None
    create_table = False

    try:
        opts, args = getopt.getopt(argv, "hct:f:", ["help", "create", "table=", "filter="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-t", "--table"):
            table_name = arg
        elif opt in ("-c", "--create"):
            create_table = True
        elif opt in ("-f", "--filter"):
            TIMESTAMP = arg
            if not TIMESTAMP.isdigit() and not len(TIMESTAMP) != 8:
                print 'Wrong parameter for timestamp: YYYYMMDD so for instance 20140805'
                logger.error('Wrong parameter for timestamp: YYYYMMDD so for instance 20140805')
                usage()
                sys.exit(2)
            TIMESTAMP = time.strptime(TIMESTAMP, '%Y%m%d')

    directory = "".join(args)
    if len(directory) < 1:
        print "No dump directory provided. Exiting"
        usage()
        sys.exit(2)

    dump_dir = os.path.join('/var/backups/dynamodb/', table_name, directory)


    if user_yes_no_query('Do you really want to restore dump \'%s\' to table \'%s\'?' % (dump_dir, table_name)):
        restore(table_name, dump_dir, TIMESTAMP, create_table=create_table)
    else:
        sys.exit()

def restore(table_name, dump_dir, timestamp, create_table=False):


    t0 = time.time()
    logger = create_dynamo_logger('restore')

    logger.info('Starting restore %s at:' % table_name )
    logger.info(time.strftime('%Y-%m-%d %X', time.localtime()))
    print 'Starting restore at:'
    print time.strftime('%Y-%m-%d %X', time.localtime())

    # Extract zipfile
    try:
        os.makedirs(dump_dir)
    except OSError as e:
        print e
        logger.error('Can\'t create dump directory %s' % dump_dir)
        logger.error(e)
        sys.exit(1)
    try:
        fh = open(dump_dir + '.zip', 'rb')
        zipf = zipfile.ZipFile(fh)
        zipf.extractall(dump_dir)
        fh.close()
    except Exception as e:
        print e
        logger.error('Can\'t extract file from %s.zip' % dump_dir)
        logger.error(e)
        sys.exit(1)

    # Get data files
    files_names = os.listdir(dump_dir)
    filter_data_files = lambda x: x.startswith('data_')
    files_names = filter(filter_data_files, files_names)

    table = None
    try:
        if create_table and table_name == 'shorturl':
            print 'Recreating table %s...' % TABLE_NAME
            # Increase write capacity for faster restore
            table = Table.create(table_name, schema=[
                HashKey('url_short'),
            ], throughput={
                'read': 18,
                'write': 18
            },
            global_indexes=[
                GlobalKeysOnlyIndex('UrlIndex', parts=[
                    HashKey('url')
                ], throughput={
                    'read': 18,
                    'write': 18
                })
                ], connection=connect_to_region('eu-west-1')
            )
            ## Wait 30 secs for the table to create
            time.sleep(30)
        elif create_table and table_name == 'geoadmin-file-storage':
            print 'Recreating table %s...' % table_name
            table = Table.create(table_name, schema=[
                HashKey('adminId'),
            ], throughput={
                'read': 10,
                'write': 5
            },
            connection=connect_to_region('eu-west-1')
            )
            time.sleep(30)
        elif create_table:
            print "Unknown table %s. Cannot create..." % table_name
            sys.exit(2)
        else:
            print 'Update existing table %s ...' % table_name
            table = Table(table_name, connection=connect_to_region('eu-west-1'))

        [write_data(table, dump_dir, f, timestamp) for f in files_names]
    except Exception as e:
        print e
        logger.error('An error occured during table \'%s\' restoration' % table_name)
        logger.error(e)
        sys.exit(1)
    finally:
        shutil.rmtree(dump_dir)
        tf = time.time()
        toff = tf - t0
        logger.info('It took: %s seconds' %toff)

if __name__ == '__main__':
    main(sys.argv[1:])

