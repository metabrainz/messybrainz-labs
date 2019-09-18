#!/usr/bin/env python3

import sys
import pprint
import psycopg2
import operator
import ujson
import uuid
from sys import stdout
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject
from psycopg2.extras import execute_values, register_uuid

'''
docker exec -it musicbrainz-docker_db_1 pg_dump -U musicbrainz -t recording_artist_credit_pairs musicbrainz_db > recording_artist_credit_pairs.sql
create schema musicbrainz;
grant musicbrainz to messybrainz;
docker exec -it musicbrainz-docker_db_1 bash
psql -U messybrainz messybrainz < /tmp/recording_artist_credit_pairs.sql
'''

SELECT_MSB_RECORDINGS_QUERY = '''
    SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name, artist as artist_msid,
           lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name, r.gid AS recording_msid,
           lower(musicbrainz.musicbrainz_unaccent(rl.title)) AS release_name, rl.gid AS release_msid
      FROM recording r
      JOIN recording_json rj ON r.data = rj.id
      JOIN release rl ON r.release = rl.gid
'''
#WHERE left(rj.data->>'artist', 6) = 'Portis'

SELECT_MB_RECORDINGS_QUERY = '''
    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
                    lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid,
                    lower(musicbrainz.musicbrainz_unaccent(release_name)) AS release_name, release_mbid
      FROM musicbrainz.recording_artist_credit_pairs 
'''
#WHERE left(artist_credit_name, 6) = 'Portis'

CREATE_MAPPING_TABLE_QUERY = """
    CREATE TABLE musicbrainz.msd_mb_mapping (
        count INTEGER,
        msb_artist_name    TEXT,
        msb_artist_msid    UUID,
        msb_recording_name TEXT,
        msb_recording_msid UUID,
        msb_release_name   TEXT,
        msb_release_msid   UUID,
        mb_artist_name     TEXT,
        mb_artist_gids     UUID[],
        mb_recording_name  TEXT,
        mb_recording_gid   UUID,
        mb_release_name    TEXT,
        mb_release_gid     UUID
    )
"""

CREATE_MAPPING_INDEXES_QUERIES = [
    "CREATE INDEX msd_mb_mapping_msb_recording_name_ndx ON musicbrainz.msd_mb_mapping(msb_recording_name)",
    "CREATE INDEX msd_mb_mapping_msb_recording_msid_ndx ON musicbrainz.msd_mb_mapping(msb_recording_msid)",
    "CREATE INDEX msd_mb_mapping_msb_artist_name_ndx ON musicbrainz.msd_mb_mapping(msb_artist_name)",
    "CREATE INDEX msd_mb_mapping_msb_artist_msid_ndx ON musicbrainz.msd_mb_mapping(msb_artist_msid)",
    "CREATE INDEX msd_mb_mapping_msb_release_name_ndx ON musicbrainz.msd_mb_mapping(msb_release_name)",
    "CREATE INDEX msd_mb_mapping_msb_release_msid_ndx ON musicbrainz.msd_mb_mapping(msb_release_msid)",
]

def create_table(conn):

    with conn.cursor() as curs:
        while True:
            try:
                print("create table")
                curs.execute(CREATE_MAPPING_TABLE_QUERY)
                conn.commit() 
                break

            except DuplicateTable as err:
                conn.rollback() 
                curs.execute("DROP TABLE musicbrainz.msd_mb_mapping")
                conn.commit() 


def create_indexes(conn):

    try:
        with conn.cursor() as curs:
            for query in CREATE_MAPPING_INDEXES_QUERIES:
                print("  ", query)
                curs.execute(query)
            conn.commit()
    except OperationalError as err:
        conn.rollback()
        print("creating indexes failed.")


def insert_rows(curs, values):

    query = "INSERT INTO musicbrainz.msd_mb_mapping VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")

            
def calculate_msid_mapping():

    msb_recordings = []
    mb_recordings = []

    recording_mapping = {}
    artist_mapping = {}

    print("load MSB recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MSB_RECORDINGS_QUERY)
            while True:
                msb_row = curs.fetchone()
                if not msb_row:
                    break

                msb_recordings.append((msb_row[0], msb_row[1], msb_row[2], msb_row[3], msb_row[4], msb_row[5]))


    print("sort MSB recordings (%d items)" % (len(msb_recordings)))
    msb_recording_index = list(range(len(msb_recordings)))
    msb_recording_index = sorted(msb_recording_index, key=lambda rec: (msb_recordings[rec][0], msb_recordings[rec][2]))

    print("load MB recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MB_RECORDINGS_QUERY)
            while True:
                mb_row = curs.fetchone()
                if not mb_row:
                    break

                mb_recordings.append((mb_row[0], mb_row[1][1:-1].split(","), mb_row[2], mb_row[3], mb_row[4], mb_row[5]))

    print("sort MB recordings (%d items)" % (len(mb_recordings)))
    mb_recording_index = list(range(len(mb_recordings)))
    mb_recording_index = sorted(mb_recording_index, key=lambda rec: (mb_recordings[rec][0], mb_recordings[rec][2]))

    mb_index = -1
    msb_index = -1
    while True:
        if not msb_row:
            try:
                msb_index += 1
                msb_row = msb_recordings[msb_recording_index[msb_index]]
            except IndexError:
                break
            
        if not mb_row:
            try:
                mb_index += 1
                mb_row = mb_recordings[mb_recording_index[mb_index]]
            except IndexError:
                break

        pp = "%-37s %-37s = %-27s %-37s" % (msb_row[0][0:25], msb_row[2][0:25], mb_row[0][0:25], mb_row[2][0:25])
        if msb_row[0] > mb_row[0]:
#            print("> %s" % pp)
            mb_row = None
            continue

        if msb_row[0] < mb_row[0]:
#            print("< %s" % pp)
            msb_row = None
            continue

        if msb_row[2] > mb_row[2]:
#            print("} %s" % pp)
            mb_row = None
            continue

        if msb_row[2] < mb_row[2]:
#            print("{ %s" % pp)
            msb_row = None
            continue

#        print("= %s %s" % (pp, mb_row[3][0:15]))

        k = "%s=%s" % (msb_row[1], mb_row[1])
        try:
            artist_mapping[k][0] += 1
        except KeyError:
            artist_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]


        k = "%s=%s" % (msb_row[3], mb_row[3])
        try:
            recording_mapping[k][0] += 1
        except KeyError:
            recording_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]

        msb_row = None

#    top_index = []
#    for k in artist_mapping:
#        top_index.append((artist_mapping[k][0], k))
#    with open("artists.json", "w") as j:
#        for count, k in sorted(top_index, reverse=True):
#            a = artist_mapping[k]
#            j.write(ujson.dumps((a[0],
#                msb_recordings[a[1]][1], 
#                msb_recordings[a[1]][0], 
#                mb_recordings[a[2]][1],
#                mb_recordings[a[2]][0],
#                )) + "\n")

#   # Give a hint that we no longer need these
#    artist_mapping = None

    create_table(conn)

    print("save data to new table")
    top_index = []
    for k in recording_mapping:
        top_index.append((recording_mapping[k][0], k))

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            register_uuid(curs)
            rows = []
            for count, k in sorted(top_index, reverse=True):
                a = recording_mapping[k]
                rows.append((a[0],
                    msb_recordings[a[1]][0], 
                    msb_recordings[a[1]][1], 
                    msb_recordings[a[1]][2], 
                    msb_recordings[a[1]][3], 
                    msb_recordings[a[1]][4], 
                    msb_recordings[a[1]][5], 
                    mb_recordings[a[2]][0],
                    [ uuid.UUID(u) for u in mb_recordings[a[2]][1]],
                    mb_recordings[a[2]][2],
                    mb_recordings[a[2]][3],
                    mb_recordings[a[2]][4],
                    mb_recordings[a[2]][5]
                    ))
                if len(rows) == 1000:
                    insert_rows(curs, rows)
                    rows = []

            insert_rows(curs, rows)
            conn.commit()

            print("create indexes")
            create_indexes(conn)

if __name__ == "__main__":
    calculate_msid_mapping()
