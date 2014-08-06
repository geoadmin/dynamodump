INSTALL
-------

<pre>
./setup.sh
</pre>


DUMP DB USAGE
-------------

<pre>
venv/bin/python dump.py
</pre>

will create a folder which name is today's timestamp. (DDMMYYYY so for instance 05082014)
This folder contains 2 files:

schema.json : The description of the table
data.json: The actual data represented as an array of object litterals

RESTORE DB USAGE
----------------

<pre>
venv/bin/python restore.py {timestamp}
</pre>
