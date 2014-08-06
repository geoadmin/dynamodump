# -*- coding: utf-8 -*-

## USAGE: venv/bin/python dump.py
## A dump takes around 35 minutes

import boto
import json
import time
import datetime
import os 

DUMP_DIR = '/var/cache/print/'
PREFIX_NAME = 'data_'
TABLE_NAME = 'shorturls'
JSON_INDENT = 2

t0 = time.time()
FOLDER_NAME = datetime.datetime.fromtimestamp(t0).strftime('%d%m%Y') + '/'
try:
    os.mkdir(DUMP_DIR + FOLDER_NAME)
except OSError as e:
    print e

conn = boto.connect_dynamodb()

# Save Schema
f = open(DUMP_DIR + FOLDER_NAME + 'schema.json', 'w+')
table_desc = conn.describe_table(TABLE_NAME)
f.write(json.dumps(table_desc, indent=JSON_INDENT))
f.close()

# Save Data
counter = 0
table = conn.get_table('shorturls')
scanned_table = table.scan()

# Don't write more than 1'000'000 items per file
file_count = 1
filename = PREFIX_NAME + str(file_count) + '.json'
f = open(DUMP_DIR + FOLDER_NAME + filename, 'w+')
try:
    # Needs to be a list for json.loads to work properly
    f.write('[')
    for col in scanned_table:
        json.dump(col, f, indent=JSON_INDENT)
        counter += 1
        if counter%1000000 == 0:
            f.write(']')
            f.close()
            file_count += 1
            filename = PREFIX_NAME + str(file_count) + '.json'
            f = open(DUMP_DIR + FOLDER_NAME + filename, 'w+')
        else:
            f.write(',')
except Exception as e:
    print e
finally:
    f.close()

tf = time.time()
toff = tf - t0
print 'It took %s seconds' %toff
