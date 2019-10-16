#!/usr/bin/env python3

import sys
import pprint
import psycopg2
from psycopg2.extras import execute_values
import operator
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

BATCH_SIZE = 1000

# This query will fetch all release groups for single artist release groups and order them
# so that early digital albums are preferred.
SELECT_RELEASES_QUERY_TESTING = '''
    SELECT ac.name, rg.id as rg, r.id, r.name, rgpt.name as pri_type, rgstj.release_group as sec_type, mf.name as format, date_year as year
      FROM musicbrainz.release_group rg 
      JOIN musicbrainz.release r ON rg.id = r.release_group 
      JOIN musicbrainz.release_country rc ON rc.release = r.id 
      JOIN musicbrainz.medium m ON m.release = r.id 
      JOIN musicbrainz.medium_format mf ON m.format = mf.id 
      JOIN musicbrainz.format_sort fs ON mf.id = fs.format
JOIN musicbrainz.artist_credit ac ON rg.artist_credit = ac.id
JOIN musicbrainz.release_group_primary_type rgpt ON rg.type = rgpt.id   
FULL OUTER JOIN musicbrainz.release_group_secondary_type_join rgstj ON rg.id = rgstj.release_group   
     WHERE rg.artist_credit != 1 and ac.name = 'Florence + the Machine'
   ORDER BY rg.artist_credit, rg.type, sec_type desc, rg.name, fs.sort, date_year, date_month, date_day, country
LIMIT 1000;
'''

# I think this version has a problem with NIN.
#  ORDER BY rg.artist_credit, rg.type, rgstj.release_group desc, fs.sort, date_year, date_month, date_day, country, rg.name

SELECT_RELEASES_QUERY = '''
INSERT INTO musicbrainz.recording_pair_releases (release)
      SELECT r.id
       FROM musicbrainz.release_group rg 
       JOIN musicbrainz.release r ON rg.id = r.release_group 
       JOIN musicbrainz.release_country rc ON rc.release = r.id 
       JOIN musicbrainz.medium m ON m.release = r.id 
       JOIN musicbrainz.medium_format mf ON m.format = mf.id 
       JOIN musicbrainz.format_sort fs ON mf.id = fs.format
FULL OUTER JOIN musicbrainz.release_group_secondary_type_join rgstj ON rg.id = rgstj.release_group   
      WHERE rg.artist_credit != 1 
   ORDER BY rg.artist_credit, rg.type, rgstj.release_group desc, rg.name, fs.sort, date_year, date_month, date_day, country
'''
#   ORDER BY rg.artist_credit, rg.type, rgstj.release_group desc, fs.sort, date_year, date_month, date_day, country, rg.name

#    SELECT r.name as recording_name, r.gid as recording_mbid, 
#           ac.name as artist_credit_name, array_agg(a.gid) as artist_mbids,
#           rl.name as release_name, rl.gid as release_mbid,
#           ac.id, rpr.id
SELECT_RECORDING_PAIRS_QUERY = '''
    SELECT r.name as recording_name, r.gid as recording_mbid, 
           ac.name as artist_credit_name, array_agg(a.gid) as artist_mbids,
           rl.name as release_name, rl.gid as release_mbid,
           ac.id, rpr.id
      FROM recording r
      JOIN artist_credit ac ON r.artist_credit = ac.id
      JOIN artist_credit_name acn ON ac.id = acn.artist_credit
      JOIN artist a ON acn.artist = a.id
      JOIN track t ON t.recording = r.id
      JOIN medium m ON m.id = t.medium
      JOIN release rl ON rl.id = m.release
      JOIN recording_pair_releases rpr ON rl.id = rpr.release
    GROUP BY rpr.id, ac.id, rl.gid, artist_credit_name, r.gid, r.name, a.gid, release_name
    ORDER BY ac.id, rpr.id
'''
#WHERE acn.name = 'Nine Inch Nails' and r.name = 'Hurt'

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
        id      SERIAL, 
        release INTEGER
    )
'''

CREATE_RECORDING_PAIRS_INDEXES_QUERY = '''
    CREATE INDEX artist_credit_name_ndx_recording_artist_credit_pairs 
        ON musicbrainz.recording_artist_credit_pairs(artist_credit_name);
'''

CREATE_RELEASES_SORT_INDEX = '''
    CREATE INDEX recording_pair_releases_id
        ON musicbrainz.recording_pair_releases(id)
'''

CREATE_RELEASES_ID_INDEX = '''
    CREATE INDEX recording_pair_releases_release
        ON musicbrainz.recording_pair_releases(release)
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
        print("Run select releases query")
        curs.execute(SELECT_RELEASES_QUERY)
        curs.execute(CREATE_RELEASES_ID_INDEX)
        curs.execute(CREATE_RELEASES_SORT_INDEX)

    print("done")


#     rec_name, rec_gid, ac_name, {artist_gids}, rel_name, rel_gid, ac.id, rpr.id, 
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
                    last_ac_mbid = None
                    artist_recordings = {}
                    count = 0
                    print("Run fetch recordings query")
                    mb_curs.execute(SELECT_RECORDING_PAIRS_QUERY)
                    print("Fetch recordings and insert to MSB")
                    while True:
                        row = mb_curs.fetchone()
                        if not row:
                            break

                        if not last_ac_mbid:
                            last_ac_mbid = row[6]

                        if row[6] != last_ac_mbid:
                            # insert the rows that made it
                            rows.extend(artist_recordings.values())
                            artist_recordings = {}

                            if len(rows) > BATCH_SIZE:
                                insert_rows(msb_curs, rows)
                                count += len(rows)
                                msb_conn.commit()
                                print("inserted %d rows." % count)
                                rows = []

                        if row[0] not in artist_recordings:
                            artist_recordings[row[0]] = (row[0], row[1], row[2], row[3], row[4], row[5])

                        last_ac_mbid = row[6]


                    rows.extend(artist_recordings.values())
                    if rows:
                        insert_rows(msb_curs, rows)
                        msb_conn.commit()
                        count += len(rows)


                print("inserted %d rows total." % count)

                print("Create indexes")
                create_indexes(msb_conn)

    print("done")

# TODO: Select release format type and pick one release for many
if __name__ == "__main__":
    fetch_recording_pairs()
