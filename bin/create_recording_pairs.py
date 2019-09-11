#!/usr/bin/env python3

import sys
import pprint
import psycopg2
from psycopg2.extras import execute_values
import operator
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

from formats import DIGITAL_FORMATS, ANALOG_FORMATS

BATCH_SIZE = 1000

# This query will select release groups and order their releases by release date. The script will need to select the first
# digital release as the representative release for the release_group. the SELECT_RECORDING_PAIRS_QUERY then needs to 
# fetch the tracks from those releases for the matching step. This is only done for non various artist albums.
# NOTE: This will miss "bonus" tracks from alternate releases.
SELECT_RELEASES_QUERY = '''
    SELECT rg.id, r.id, r.name, mf.id, mf.name, rc.country, date_year, date_month, date_day 
      FROM musicbrainz.release_group rg 
      JOIN musicbrainz.release r ON rg.id = r.release_group 
      JOIN musicbrainz.release_country rc ON rc.release = r.id 
      JOIN musicbrainz.medium m ON m.release = r.id 
      JOIN musicbrainz.medium_format mf ON m.format = mf.id 
     WHERE r.artist_credit != 1 
  ORDER BY rg.name, date_year, date_month, date_day, country
'''

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
             JOIN recording_pair_releases rpr ON rl.id = rpr.id
    WHERE rg.type = 1 
    GROUP BY r.gid, r.name, ac.name, rl.name, rl.gid
    ORDER BY r.name
'''
#AND acn.name = 'Portishead'

CREATE_RECORDING_PAIRS_TABLE_QUERY = '''
    CREATE TABLE musicbrainz.recording_artist_credit_pairs (
        recording_name            TEXT NOT NULL,
        recording_mbid            UUID NOT NULL, 
        artist_credit_name        TEXT NOT NULL,
        artist_mbids              UUID[] NOT NULL,
        release_name              TEXT NOT NULL,
        release_mbid              UUID NOT NULL
    )
'''

CREATE_RECORDING_PAIR_RELEASES_TABLE_QUERY = '''
    CREATE TABLE musicbrainz.recording_pair_releases (
        id INTEGER
    )
'''

CREATE_RECORDING_PAIRS_INDEXES_QUERY = '''
    CREATE INDEX artist_credit_name_ndx_recording_artist_credit_pairs 
        ON musicbrainz.recording_artist_credit_pairs(artist_credit_name);
'''

def create_tables(msb_conn, mb_conn):


    # drop/create finished table from MSB
    try:
        with msb_conn.cursor() as curs:
            curs.execute("DROP TABLE musicbrainz.recording_artist_credit_pairs")
        msb_conn.commit()
    except psycopg2.errors.UndefinedTable as err:
        msb_conn.rollback()

    try:
        with msb_conn.cursor() as curs:
            curs.execute(CREATE_RECORDING_PAIRS_TABLE_QUERY)
            curs.execute("ALTER TABLE musicbrainz.recording_artist_credit_pairs OWNER TO messybrainz")
        msb_conn.commit()

    except OperationalError as err:
        print("failed to create recording pair table")
        msb_conn.rollback()


    # Drop/create temp table from MB DB
    try:
        with mb_conn.cursor() as curs:
            curs.execute("DROP TABLE musicbrainz.recording_pair_releases")
        mb_conn.commit()
    except psycopg2.errors.UndefinedTable as err:
        mb_conn.rollback()

    try:
        with mb_conn.cursor() as curs:
            curs.execute(CREATE_RECORDING_PAIR_RELEASES_TABLE_QUERY)
            curs.execute("ALTER TABLE musicbrainz.recording_pair_releases OWNER TO messybrainz")
        mb_conn.commit()

    except OperationalError as err:
        print("failed to create recording pair releases table")
        mb_conn.rollback()



def insert_rows(curs, values):

    query = "INSERT INTO musicbrainz.recording_artist_credit_pairs VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows", err)


def insert_release_rows(curs, values):

    query = "INSERT INTO musicbrainz.recording_pair_releases VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows", err)


def create_indexes(conn):
    try:
        with conn.cursor() as curs:
            curs.execute(CREATE_RECORDING_PAIRS_INDEXES_QUERY)
        conn.commit()
    except OperationalError as err:
        print("failed to create recording pair index", err)
        conn.rollback()


def create_temp_release_table(conn):

    with conn.cursor() as curs:
        with conn.cursor() as curs2:

            count = 0
            print("Run select releases query")
            curs.execute(SELECT_RELEASES_QUERY)

            print("Fetch releases and insert to MB")
            last_rg = 0
            releases = []
            releases_per_rg = []
            while True:
                row = curs.fetchone()
                if not row:
                    break

                # SELECT rg.id, r.id, r.name, mf.id, mf.name, rc.country, date_year, date_month, date_day 
                if not last_rg:
                    last_rg = row[0]
                   
                if row[0] != last_rg:

                    # Pick a release from the release group. If there is only one release in the group, easy.
                    if len(releases_per_rg) == 1:
                        releases.append((releases_per_rg[0][1],))
                    else:
                        # Pick the first digital release. If none are digital, just take the first one
                        for rel in releases_per_rg:
                            if row[3] in DIGITAL_FORMATS:   
                                releases.append((rel[1],))
                                break
                        else:
                            releases.append((releases_per_rg[0][1],))

                    releases_per_rg = []

                releases_per_rg.append(row)
                last_rg = row[0]

                if len(releases) == 1000:
                    insert_release_rows(curs2, releases)
                    count += len(releases)
                    releases = []

                    if count % 100000 == 0:
                        print("inserted %d rows." % count)
                        conn.commit()


            if releases:
                insert_release_rows(curs2, releases)
                count += len(releases)
                conn.commit()
        
            print("inserted %d rows." % count)

    print("done")


def fetch_recording_pairs():

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as mb_conn:
        with mb_conn.cursor() as mb_curs:
            with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as msb_conn:

                # Create the dest table (perhaps dropping the old one first)
                print("Drop/create pairs table")
                create_tables(msb_conn, mb_conn)

                print("select releases from MB")
                create_temp_release_table(mb_conn)

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
                            msb_conn.commit()

                msb_conn.commit()

                print("Create indexes")
                create_indexes(msb_conn)

    print("done")

# TODO: Select release format type and pick one release for many
if __name__ == "__main__":
    fetch_recording_pairs()
