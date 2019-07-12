#!/usr/bin/env python3

import sys
import pprint
import psycopg2
import operator
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject
from icu import UnicodeString

'''
docker exec -it musicbrainz-docker_db_1 pg_dump -U musicbrainz -t recording_artist_credit_pairs musicbrainz_db > recording_artist_credit_pairs.sql
create schema musicbrainz;
grant musicbrainz to messybrainz;
docker exec -it musicbrainz-docker_db_1 bash
psql -U messybrainz messybrainz < /tmp/recording_artist_credit_pairs.sql
'''

SELECT_MSB_RECORDINGS_QUERY = '''
    SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name, artist as artist_msid,
           lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name, gid AS recording_msid
      FROM recording r
      JOIN recording_json rj ON r.data = rj.id
     WHERE left(rj.data->>'artist', 6) = 'Portis'
'''

SELECT_MB_RECORDINGS_QUERY = '''
    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
           lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid
      FROM musicbrainz.recording_artist_credit_pairs 
     WHERE left(artist_credit_name, 6) = 'Portis'
'''


def create_or_truncate_table(conn):

    try:
        with conn.cursor() as curs:
            print("create table")
            curs.execute(CREATE_RELATIONS_TABLE_QUERY)

    except DuplicateTable as err:
        conn.rollback() 
        try:
            with conn.cursor() as curs:
                print("truncate")
                curs.execute(TRUNCATE_RELATIONS_TABLE_QUERY)
                conn.commit()

            with conn.cursor() as curs:
                print("drop indexes")
                try:
                    curs.execute("DROP INDEX artist_artist_relations_artist_0_ndx")
                    conn.commit()
                except UndefinedObject as err:
                    conn.rollback()

                try:
                    curs.execute("DROP INDEX artist_artist_relations_artist_1_ndx")
                    conn.commit()
                except UndefinedObject as err:
                    conn.rollback()

        except OperationalError as err:
            print("failed to truncate existing table")
            conn.rollback()


def create_indexes(conn):
    try:
        with conn.cursor() as curs:
            for query in CREATE_INDEX_QUERIES:
                curs.execute(query)
            conn.commit()
    except OperationalError as err:
        conn.rollback()
        print("creating indexes failed.")


def insert_rows(curs, values):

    query = "INSERT INTO artist_artist_relations VALUES " + ",".join(values)
    try:
        curs.execute(query)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")

            
def dump_similarities(conn, relations):

    create_or_truncate_table(conn)

    values = []
    with conn.cursor() as curs:

        for k in relations:
            r = relations[k]
            if r[0] > 2:
                values.append("(%d, %d, %d)" % (r[0], r[1], r[2]))

            if len(values) > 1000:
                insert_rows(curs, values)
                conn.commit()
                values = []

        if len(values):
            insert_rows(curs, values)
            conn.commit()


# TODO: Filter MB tracks by album type
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

                msb_recordings.append((msb_row[0], msb_row[1], msb_row[2], msb_row[3]))


    print("sort MSB recordings (%d items)" % (len(msb_recordings)))
    msb_recording_index = list(range(len(msb_recordings)))
    msb_recording_index = sorted(msb_recording_index, key=lambda rec: (msb_recordings[rec][0], msb_recordings[rec][2]))

    # This sort runs faster, but takes more time to finish. Swapping?
    #msb_recordings = sorted(msb_recordings, key=operator.itemgetter(0, 2))

    print("load MB recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MB_RECORDINGS_QUERY)
            while True:
                mb_row = curs.fetchone()
                if not mb_row:
                    break

                mb_recordings.append((mb_row[0], mb_row[1], mb_row[2], mb_row[3]))

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

#        pp = "%-27s %-27s = %-27s %-27s" % (msb_row[0][0:25], msb_row[2][0:25], mb_row[0][0:25], mb_row[2][0:25])
        pp = "%-27s %-27s = %-27s %-27s" % (msb_row[2][0:25], msb_row[3][0:25], mb_row[2][0:25], mb_row[3][0:25])
        if msb_row[0] > mb_row[0]:
            print("> %s" % pp)
            mb_row = None
            continue

        if msb_row[0] < mb_row[0]:
            print("< %s" % pp)
            msb_row = None
            continue

        if msb_row[2] > mb_row[2]:
            print("} %s" % pp)
            mb_row = None
            continue

        if msb_row[2] < mb_row[2]:
            print("{ %s" % pp)
            msb_row = None
            continue

        print("= %s" % pp)

        k = "%s=%s" % (msb_row[1], mb_row[1])
        try:
            artist_mapping[k][0] += 1
        except KeyError:
            artist_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]


        k = "%s=%s" % (msb_row[3], mb_row[3])
        print("%-27s %s = %-27s %s" % (msb_row[2], msb_row[3], mb_row[2], mb_row[3]))
        try:
            recording_mapping[k][0] += 1
        except KeyError:
            recording_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]

        msb_row = None

    print("artist votes: %d" % len(artist_mapping))
    for k in sorted(artist_mapping, key=operator.itemgetter(0), reverse=True)[0:1000]:
        a = artist_mapping[k]
        print("%5d %-50s = %-50s" % (a[0], msb_recordings[a[1]][0][0:50], mb_recordings[a[2]][0][0:50]))

    print()
    print("recording votes: %d" % len(recording_mapping))
    for k in sorted(recording_mapping, key=operator.itemgetter(0), reverse=True)[0:1000]:
        r = recording_mapping[k]
        print("%5d %-50s = %-50s" % (r[0], msb_recordings[r[1]][2][0:50], mb_recordings[r[2]][2][0:50]))



if __name__ == "__main__":
    calculate_msid_mapping()
