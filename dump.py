# -*- coding: utf-8 -*-

## USAGE: venv/bin/python dump.py
## A dump takes around 35 minutes

import boto
import json
import time
import datetime
import os 

DUMP_DIR = '/var/cache/print/'
TABLE_NAME = 'shorturls'
JSON_INDENT = 2

t0 = time.time()
FOLDER_NAME = datetime.datetime.fromtimestamp(t0).strftime('%d%m%Y') + '/'
os.mkdir(DUMP_DIR + FOLDER_NAME)

conn = boto.connect_dynamodb()

# Save Schema
f = open(DUMP_DIR + FOLDER_NAME + 'schema.json', 'w+')
table_desc = conn.describe_table(TABLE_NAME)
nb_items = table_desc['Table']['ItemCount']
f.write(json.dumps(table_desc, indent=JSON_INDENT))
f.close()

# Save Data
counter = 0
table = conn.get_table('shorturls')
scanned_table = table.scan()
f = open(DUMP_DIR + FOLDER_NAME + 'data.json', 'w+')
try:
    # Needs to be a list for json.loads to work properly
    f.write('[')
    for col in scanned_table:
        json.dump(col, f, indent=JSON_INDENT)
        counter += 1
        if counter != nb_items:
            f.write(',')
finally:
    f.write(']')
    f.close()

tf = time.time()
toff = tf - t0
print 'It took %s seconds' %toff
