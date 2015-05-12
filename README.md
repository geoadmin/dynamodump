INSTALL
-------

<pre>
./setup.sh
</pre>


CONFIGURATION
-------------

Logs:

    /var/log/dynamodb/<table name>

Backups:

    /var/backups/dynamodb/<table name>


DUMP DB USAGE
-------------

  $ venv/bin/python dump.py [-t table_name] 

will create a directory in /var/backups/dynamodb/<table name> which name is today's timestamp. (YYYYMMDD so for instance 20140805)
This folder contains 2 types of files:

schema.json : The description of the table
data_{n}.json: The actual data represented as an array of object litterals

The default table name is **shorturl**

RESTORE DB USAGE
----------------

    venv/bin/python restore.py [-h --help] [-t --table=table_name] [-c --create] [-f --filter=20140805] dump_dir

Example:

    venv/bin/python restore.py --create --table=geoadmin-file-storage --filter=20140918 /var/backups/dynamodb/geoadmin-file-storage/20140917


    --filter         Format: %Y%m%d (the associated compressed dump timestamp)
    --table          Table name. Default to 'shorturl'
    --create         Create the table before inserting values.

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
