from time import asctime
import psycopg2


def create_schema(conn):
    '''
        Create the relations schema if it doesn't already exist
    '''

    try:
        with conn.cursor() as curs:
            print(asctime(), "create schema")
            curs.execute("CREATE SCHEMA IF NOT EXISTS mapping")
            conn.commit()
    except OperationalError:
        print(asctime(), "failed to create schema 'mapping'")
        conn.rollback()


def insert_rows(curs, table, values):
    '''
        Use the bulk insert function to insert rows into the relations table.
    '''

    query = ("INSERT INTO %s VALUES " % table) + ",".join(values)
    try:
        curs.execute(query)
    except psycopg2.OperationalError:
        print(asctime(), "failed to insert rows")
