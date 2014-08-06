# -*- utf-8 -*-

## USAGE: venv/bin/python restore.py {timestamp}

import boto
import json
import sys
import time

DUMP_DIR = '/var/cache/print/' + sys.argv[1] + '/'
## Change back to shorturls!!!!!
TABLE_NAME = 'test'
JSON_INDENT = 2

t0 = time.time()
# Get Schema
conn = boto.connect_dynamodb()

if TABLE_NAME in conn.list_tables():
    print 'Table %s already exists, please make sure you want need to restore it.' %TABLE_NAME
    sys.exit()

file_data = None
try:
    file_data = open(DUMP_DIR + 'data.json', 'r')
except IOError:
    print 'The following dump directory doesn\'t exists: %s' %DUMP_DIR

# Recreate the table
schema = conn.create_schema(hash_key_name='url_short',hash_key_proto_value='S')
table = conn.create_table(name=TABLE_NAME, schema=schema, read_units=25, write_units=500)

def split_data(data, n):
    # data is a list of object litterals
    for i in xrange(0, len(data), n):
        yield data[i:i+n]

def process_chunk(conn, table, chunk):
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
    response = conn.batch_write_item(batch_list)
    if response['UnprocessedItems']:
        # print something if dict is not empty
        print response['UnprocessedItems']

try:
    data = json.load(file_data)
    # Load data as batches of 25 items (maximum value) and should not exceed 1Mb
    for chunk in split_data(data, 25):
        process_chunk(conn, table, chunk) 
except Exception as e:
    print 'An error occured during DB restoration'
    print '%s' %e
finally:
    # Back to a regular throughput
    table.update_throughput(25, 25)
    if file_data:
        file_data.close()

tf = time.time()
toff = tf - t0
print 'It took %s seconds' %toff
