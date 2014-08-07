# -*- coding: utf-8 -*-

## USAGE: venv/bin/python dump.py
## A dump takes around 35 minutes

import boto
import json
import time
import datetime
import os 
import zipfile


t0 = time.time()
DUMP_DIR = '/var/cache/print/'
PREFIX_NAME = 'data_'
TABLE_NAME = 'shorturls'
JSON_INDENT = 2
FOLDER_NAME = datetime.datetime.fromtimestamp(t0).strftime('%d%m%Y')

def zip_dir(zipname, dir_to_zip, dump_dir):
    dir_to_zip_len = len(dir_to_zip.rstrip(os.sep)) + 1
    with zipfile.ZipFile(dump_dir + zipname, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for dirname, subdirs, files in os.walk(dir_to_zip):
            for filename in files:
                path = os.path.join(dirname, filename)
                entry = path[dir_to_zip_len:]
                zf.write(path, entry)

try:
    os.mkdir(DUMP_DIR + FOLDER_NAME)
except OSError as e:
    print e

conn = boto.connect_dynamodb()

# Save Schema
f = open(DUMP_DIR + FOLDER_NAME + '/schema.json', 'w+')
table_desc = conn.describe_table(TABLE_NAME)
nb_items = table_desc['Table']['ItemCount']
f.write(json.dumps(table_desc, indent=JSON_INDENT))
f.close()

# Save Data
counter = 0
table = conn.get_table('shorturls')
scanned_table = table.scan()


# Don't write more than 100'000 items per file
file_count = 1
filename = PREFIX_NAME + str(file_count) + '.json'
f = open(DUMP_DIR + FOLDER_NAME + '/' + filename, 'w+')
try:
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
            f = open(DUMP_DIR + FOLDER_NAME + '/' + filename, 'w+')
            f.write('[')
        else:
            f.write(',')
except Exception as e:
    print 'An error occured while writing the json files'
    print e
finally:
    f.close()

zip_dir(FOLDER_NAME + '.zip', DUMP_DIR + FOLDER_NAME, DUMP_DIR)
os.removedirs(DUMP_DIR + FOLDER_NAME)
tf = time.time()
toff = tf - t0
print 'It took %s seconds' %toff
