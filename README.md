# ordered_pgsql_dump
Simple scripts for dumping postgresql database with ordered insert statements


Backup.py

 Accepts as first parameter output file name
 As second parameter file with credentials (see credentials_example.ini)

 Default are: database.sql credentials.ini


Restore.py

 Accepts as first parameter input file name
 As second parameter file with credentials (see credentials_example.ini)

 Default are: database.sql credentials.ini


NOTICE:

 Password can be included into credentials file as "password" parameter.
 Otherwise following feature is used: http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
 ("The password file" feature)