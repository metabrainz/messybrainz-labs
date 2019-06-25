#!/usr/bin/env python3

import sys
import pprint
import psycopg2
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
      JOIN recording_json rj ON r.id = rj.id
     WHERE left(rj.data->>'artist', 4) = 'Port'
  ORDER BY musicbrainz.musicbrainz_collate(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT))),
           musicbrainz.musicbrainz_collate(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)))
'''

SELECT_MB_RECORDINGS_QUERY = '''
    SELECT * FROM (
        SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
               lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid
          FROM musicbrainz.recording_artist_credit_pairs 
         WHERE left(artist_credit_name, 4) = 'Port') AS pairs
  ORDER BY musicbrainz.musicbrainz_collate(lower(musicbrainz.musicbrainz_unaccent(pairs.artist_credit_name))),
           musicbrainz.musicbrainz_collate(lower(musicbrainz.musicbrainz_unaccent(pairs.recording_name)))
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

    msb_row = []
    mb_row = []

    recording_mapping = {}

    print("query for MSB recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MSB_RECORDINGS_QUERY)

            print("query for MB recordings")
            with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn2:
                with conn2.cursor() as curs2:
                    curs2.execute(SELECT_MB_RECORDINGS_QUERY)

                    while True:
                        if not msb_row:
                            msb_row = curs.fetchone()
                            if not msb_row:
                                break
                            
                        if not mb_row:
                            mb_row = curs2.fetchone()
                            if not mb_row:
                                break

                        msb_artist = UnicodeString(msb_row[0])
                        mb_artist = UnicodeString(mb_row[0])
                        msb_recording = UnicodeString(msb_row[2])
                        mb_recording = UnicodeString(mb_row[2])

                        pp = "%-27s %-27s = %-27s %-27s" % (msb_row[0][0:25], msb_row[2][0:25], mb_row[0][0:25], mb_row[2][0:25])
                        if msb_artist > mb_artist:
                            print("> %s" % pp)
                            mb_row = None
                            continue

                        if msb_artist < mb_artist:
                            print("< %s" % pp)
                            msb_row = None
                            continue

                        if msb_recording > mb_recording:
                            print("} %s" % pp)
                            mb_row = None
                            continue

                        if msb_recording < mb_recording:
                            print("{ %s" % pp)
                            msb_row = None
                            continue

                        print("= %s" % pp)

                        k = "%s=%s" % (msb_row[1], mb_row[1])
                        try:
                            recording_mapping[k][0] += 1
                        except KeyError:
                            recording_mapping[k] = [ 1, msb_row[1], mb_row[1] ]

                        msb_row = None



if __name__ == "__main__":
    calculate_msid_mapping()
