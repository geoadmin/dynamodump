INSTALL
-------

<pre>
./setup.sh
</pre>


CONFIGURATION
-------------

Logs:

    /var/log/dynamodb/

Backups:

    /var/backups/dynamodb/


DUMP DB USAGE
-------------

  venv/bin/python dump.py

will create a folder which name is today's timestamp. (YYYYMMDD so for instance 20140805)
This folder contains 2 types of files:

schema.json : The description of the table
data_{n}.json: The actual data represented as an array of object litterals

RESTORE DB USAGE
----------------

    venv/bin/python restore.py {timestamp} {createtable} {restorefromtimestamp}

Example:

    venv/bin/python restore.py 20140918 true 20140917


    {timestamp} format: %Y%m%d (the associated compressed dump timestamp)
    {createtable} values: true or false
    {restorefromtimestamp} format: %Y%m%d

In order to commit in this repository
-------------------------------------

    sudo su dynamodb
    cd
    cd dynamodump

Change git variables
--------------------

    git config --global user.email "you@example.com"
    git config --global user.name "Your Name"

Now you're ready!

Update table indices throughput using boto
------------------------------------------

<pre>
from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region


table = Table('shorturl', connection=connect_to_region('eu-west-1'))
table.update(throughput={
    'read': 18,
    'write': 18
},  global_indexes={
    'UrlIndex': {
         'read': 18,
         'write': 18
    }
})
</pre>
