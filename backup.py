#!/usr/bin python
# -*- coding: utf-8 -*-

"""
Backuping database script with insert sorting
"""
import psycopg2
import ConfigParser
import os
import sys
import sql_reserved_words
import decimal
import datetime
import subprocess
import codecs
from shutil import move

NONE_TYPE = type(None)
DEFAULT_OUTPUT_FILE_NAME = 'database.sql'
DEFAULT_SCHEMA = 'public'
DEFAULT_OPTIONS_FILE = 'credentials.ini'

def check_identifier(identifier):
    """
    Wraps identifier with \" if it is reserved word
    """
    if identifier.upper() in sql_reserved_words.RESERVED_WORDS:
        return '"' + identifier + '"'
    else:
        return identifier

def pg_dump_pre_data(host, port, password, user, database, schema, output_file_name):
    """
    Getting pre data for defined db
    """
    if not password is None:
        os.environ['PGPASSWORD'] = password
    pg_dump_process = subprocess.Popen(["C:/Program Files/PostgreSQL/9.3/bin/pg_dump.exe",
                                        "--host",
                                        host,
                                        "--port",
                                        port,
                                        "--username",
                                        user,
                                        "--format",
                                        "plain",
                                        "--no-owner",
                                        "--section",
                                        "pre-data",
                                        "--encoding",
                                        "UTF8",
                                        "--no-privileges",
                                        "--no-tablespaces",
                                        "--verbose",
                                        "--no-unlogged-table-data",
                                        "--schema=%s"%schema,
                                        "--file",
                                        output_file_name,
                                        database],
                                       env=os.environ)
    pg_dump_process.wait()

def pg_dump_post_data(host, port, password, user, database, schema, output_file_name):
    """
    Getting post data for defined db
    """
    if not password is None:
        os.environ['PGPASSWORD'] = password
    pg_dump_process = subprocess.Popen(["C:/Program Files/PostgreSQL/9.3/bin/pg_dump.exe",
                                        "--host",
                                        host,
                                        "--port",
                                        port,
                                        "--username",
                                        user,
                                        "--format",
                                        "plain",
                                        "--no-owner",
                                        "--section",
                                        "post-data",
                                        "--encoding",
                                        "UTF8",
                                        "--no-privileges",
                                        "--no-tablespaces",
                                        "--verbose",
                                        "--no-unlogged-table-data",
                                        "--schema=%s"%schema,
                                        "--file",
                                        output_file_name,
                                        database],
                                       env=os.environ)
    pg_dump_process.wait()

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
        output_file = sys.argv[1]
    else:
        output_file = DEFAULT_OUTPUT_FILE_NAME

    if len(sys.argv) > 2:
        ini_file = sys.argv[2]
    else:
        ini_file = DEFAULT_OPTIONS_FILE

    if not os.path.exists(ini_file):
        print u"ERROR: Move backup script into the root project directory, for example 'C:\\xampp\\htdocs', or provide path to credentials.ini as second parameter and output file name as first"
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

    return (host, database, user, password, port, schema, output_file)

def process_tuple_to_string(tuple_):
    """
    Convert python types to PostgreSQL insert strings
    """
    result = []
    for item in tuple_:
        type_ = type(item)
        if type_ == NONE_TYPE:
            result.append("NULL")
        elif (type_ == unicode or
              type_ == str):
            result.append("'%s'"%item.replace("'", "''").replace("\r", ""))
        elif (type(item) == int or
              type(item) == long):
            result.append("%s"%item)
        elif type(item) == decimal.Decimal:
            result.append("%s"%unicode(item))
        elif type(item) == datetime.datetime:
            result.append("'%s'"%item.isoformat(' '))
        elif type(item) == bool:
            result.append(str(item))
        else:
            raise Exception("Unknown type to convert: '%s'"%type(item))
    return u", ".join([item.decode('utf-8') for item in result])

def get_data(con, schema, output_file_name):
    """
    Get inserts for dump
    """
    output_file = codecs.open(output_file_name, "a", encoding='utf-8')

    cur = con.cursor()

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name", (schema, ))

    for table in cur.fetchall():
        table_name = check_identifier(table[0])

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table[0], ))

        columns = [check_identifier(tuple_[0]) for tuple_ in cur.fetchall()]

        columns_list = ", ".join(columns)

        cur.execute("""SELECT  kcu.column_name
                       FROM    information_schema.tables t
                                LEFT JOIN information_schema.table_constraints tc
                                        ON tc.table_catalog = t.table_catalog
                                        AND tc.table_schema = t.table_schema
                                        AND tc.table_name = t.table_name
                                        AND tc.constraint_type = 'PRIMARY KEY'
                                LEFT JOIN information_schema.key_column_usage kcu
                                        ON kcu.table_catalog = tc.table_catalog
                                        AND kcu.table_schema = tc.table_schema
                                        AND kcu.table_name = tc.table_name
                                        AND kcu.constraint_name = tc.constraint_name
                       WHERE   t.table_schema = %s AND
                               t.table_name = %s
                       ORDER BY t.table_catalog,
                                t.table_schema,
                                t.table_name,
                                kcu.constraint_name,
                                kcu.ordinal_position""", (schema, table_name))

        pkeys = [tuple_[0] for tuple_ in cur.fetchall() if not tuple_[0] is None]

        insert_sql_ = "INSERT INTO %s (%s) VALUES (%s);\n"%(table_name, columns_list, '%s')

        if len(pkeys) > 0:
            select_sql_ = "SELECT %s FROM %s ORDER BY %s;"%(columns_list, table_name, ", ".join([check_identifier(pkey) for pkey in pkeys]))
        else:
            select_sql_ = "SELECT %s FROM %s ORDER BY %s;"%(columns_list, table_name, columns_list)

        cur.execute(select_sql_)

        for tuple_ in cur.fetchall():
            output_file.write(insert_sql_%process_tuple_to_string(tuple_))

        output_file.write("\n")

        for column in columns:
            cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table_name.replace('"', ''), column.replace('"', '')))

            for seq in cur.fetchall():
                if not seq[0] is None:
                    cur.execute("SELECT sequence_name, last_value, is_called FROM %s"%seq[0])
                    output_file.write("SELECT pg_catalog.setval('%s', %s, %s);\n\n\n"%cur.fetchone())
    con.close()

def clear_dump(file_name):
    """
    Remove comments from dump to reduce differences between dumps
    """
    res_file = open(file_name+'clean_file', 'w')
    for line in open(file_name, 'r'):
        if (not line.startswith('-- TOC') and
            not line.startswith('-- Dependencies') and
            not line.startswith('-- Started') and
            not line.startswith('-- Completed')):
            res_file.write(line)
        else:
            res_file.write('\n')

    res_file.close()

    move(file_name+'clean_file', file_name)

def main():
    """
    Main rutine
    """

    host, database, user, password, port, schema, output_file_name = preprocess_input_params()

    con = get_db_con(host, database, user, password, port)

    pg_dump_pre_data(host, port, password, user, database, schema, output_file_name)

    get_data(con, schema, output_file_name)

    buffer_file_name = output_file_name + ".buffer"

    pg_dump_post_data(host, port, password, user, database, schema, buffer_file_name)

    open(output_file_name, "a").write(open(buffer_file_name, "r").read())

    os.remove(buffer_file_name)

    clear_dump(output_file_name)

main()
