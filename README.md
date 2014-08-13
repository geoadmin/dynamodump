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

<pre>
venv/bin/python dump.py
</pre>

will create a folder which name is today's timestamp. (YYYYMMDD so for instance 20140805)
This folder contains 2 types of files:

schema.json : The description of the table
data_{n}.json: The actual data represented as an array of object litterals

RESTORE DB USAGE
----------------

<pre>
venv/bin/python restore.py {timestamp}
</pre>
