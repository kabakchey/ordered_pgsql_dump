#!/usr/bin python
# -*- coding: utf-8 -*-

"""
Backuping database script with insert sorting
"""
import psycopg2
import ConfigParser
import os
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

NONE_TYPE = type(None)
DEFAULT_INPUT_FILE_NAME = 'database.sql'
DEFAULT_SCHEMA = 'public'
DEFAULT_OPTIONS_FILE = 'credentials.ini'

def get_db_con(host, database, user, password, port):
    """
    Get connection to db
    """
    if password is None:
        return psycopg2.connect("host=%s dbname=%s user=%s port=%s" % (host, database, user, port))
    else:
        return psycopg2.connect("host=%s dbname=%s user=%s password=%s port=%s" % (host, database, user, password, port))

def preprocess_input_params():
    """
    Checking argv and connecting to db
    """
    if len(sys.argv) == 2 or len(sys.argv) > 2:
        input_file = sys.argv[1]
    else:
        input_file = DEFAULT_INPUT_FILE_NAME

    if len(sys.argv) > 2:
        ini_file = sys.argv[2]
    else:
        ini_file = DEFAULT_OPTIONS_FILE

    if not os.path.exists(ini_file):
        print u"ERROR: Move restore script into the root project directory, for example 'C:\\xampp\\htdocs', or provide path to credentials.ini as second parameter and input file name as first"
        sys.exit(1)

    config = ConfigParser.ConfigParser()
    config.readfp(open(ini_file))

    if config.has_option('Database', 'schema'):
        schema = config.get('Database', 'schema')
    else:
        schema = DEFAULT_SCHEMA


    if config.has_option('Database', 'password'):
        # To secure store password we can use
        # this feature:  http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
        # ("The password file" feature)
        password = config.get('Database', 'password')
    else:
        password = None

    user = config.get('Database', 'user')
    port = config.get('Database', 'port')
    host = config.get('Database', 'host')
    database = config.get('Database', 'database')

    return (host, database, user, password, port, schema, input_file)

def main():
    """
    Main rutine
    """

    host, database, user, password, port, schema, input_file_name = preprocess_input_params()

    con = get_db_con(host, 'postgres', user, password, port)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    con.cursor().execute("DROP DATABASE IF EXISTS \"%s_backup\""%(database))
    con.cursor().execute("ALTER DATABASE \"%s\" RENAME TO \"%s_backup\""%(database, database))
    con.cursor().execute("CREATE DATABASE \"%s\""%(database))
    con.close()

    con = get_db_con(host, database, user, password, port)
    con.cursor().execute("BEGIN")
    con.cursor().execute(open(input_file_name, 'r').read())
    con.cursor().execute("COMMIT")
    con.close()


main()
