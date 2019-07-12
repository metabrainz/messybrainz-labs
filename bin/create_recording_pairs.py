#!/usr/bin/env python3

import sys
import pprint
import psycopg2
from psycopg2.extras import execute_values
import operator
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

BATCH_SIZE = 1000

SELECT_RECORDING_PAIRS_QUERY = '''
    SELECT DISTINCT r.name as recording_name, r.gid as recording_mbid, 
                    ac.name as artist_credit_name, array_agg(a.gid) as artist_mbids,
                    rl.name as release_name, rl.gid as release_mbid
             FROM recording r
             JOIN artist_credit ac ON r.artist_credit = ac.id
             JOIN artist_credit_name acn ON ac.id = acn.artist_credit
             JOIN artist a ON acn.artist = a.id
             JOIN track t ON t.recording = r.id
             JOIN medium m ON m.id = t.medium
             JOIN release rl ON rl.id = m.release
             JOIN release_group rg ON rl.release_group = rg.id
       WHERE rg.type = 1
    GROUP BY r.gid, r.name, ac.name, rl.name, rl.gid
    ORDER BY r.name
'''

CREATE_RECORDING_PAIRS_TABLE_QUERY = '''
    CREATE TABLE recording_artist_credit_pairs (
        recording_name            TEXT NOT NULL,
        recording_mbid            UUID NOT NULL, 
        artist_credit_name        TEXT NOT NULL,
        artist_mbid_array         UUID[] NOT NULL,
        release_name              TEXT NOT NULL,
        release_mbid              UUID NOT NULL
    )
'''

def create_table(conn):

    try:
        with conn.cursor() as curs:
            curs.execute("DROP TABLE recording_artist_credit_pairs")
        conn.commit()
    except psycopg2.errors.UndefinedTable as err:
        conn.rollback()

    try:
        with conn.cursor() as curs:
            curs.execute(CREATE_RECORDING_PAIRS_TABLE_QUERY)
        conn.commit()
    except OperationalError as err:
        print("failed to create recording pair table")
        conn.rollback()


def insert_rows(curs, values):

    query = "INSERT INTO recording_artist_credit_pairs VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")

            
def fetch_recording_pairs():

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as mb_conn:
        with mb_conn.cursor() as mb_curs:
            with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as msb_conn:

                # Create the dest table (perhaps dropping the old one first)
                print("Drop/create pairs table")
                create_table(msb_conn)

                with msb_conn.cursor() as msb_curs:

                    rows = []
                    count = 0
                    print("Run fetch recordings query")
                    mb_curs.execute(SELECT_RECORDING_PAIRS_QUERY)
                    print("Fetch recordings and insert to MSB")
                    while True:
                        for i in range(BATCH_SIZE):
                            row = mb_curs.fetchone()
                            if not row:
                                break

                            rows.append((row[0], row[1], row[2], row[3], row[4], row[5]))

                        if len(rows):
                            insert_rows(msb_curs, rows)
                            count += len(rows)
                        else:
                            break

                        rows = []

                        if count % 1000000 == 0:
                            print("inserted %d rows." % count)


if __name__ == "__main__":
    fetch_recording_pairs()
