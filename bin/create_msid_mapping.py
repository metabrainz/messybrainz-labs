#!/usr/bin/env python3

import sys
import os
import pprint
import psycopg2
import operator
import ujson
import psutil
from uuid import UUID
import datetime
import subprocess
import re
import gc
from sys import stdout
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject
from psycopg2.extras import execute_values, register_uuid
from utils import insert_mapping_rows

REMOVE_NON_WORD_CHARS = False

# The name of the script to be saved in the source field.
SOURCE_NAME = "exact"
NO_PARENS_SOURCE_NAME = "noparens"

SELECT_MSB_RECORDINGS_QUERY = '''
         SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name, artist as artist_msid,
                lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name, r.gid AS recording_msid,
                lower(musicbrainz.musicbrainz_unaccent(rl.title)) AS release_name, rl.gid AS release_msid
           FROM recording r
           JOIN recording_json rj ON r.data = rj.id
LEFT OUTER JOIN release rl ON r.release = rl.gid
WHERE      left(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)), 4) = 'guns'
'''
#WHERE left(rj.data->>'artist', 6) = 'Portis'
#  AND      left(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)), 3) = 'ain'

SELECT_MB_RECORDINGS_QUERY = '''
    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_credit_id,
                    lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_id,
                    lower(musicbrainz.musicbrainz_unaccent(release_name)) AS release_name, release_id
      FROM musicbrainz.recording_artist_credit_pairs 
WHERE left(artist_credit_name, 4) = 'Guns'
'''

CREATE_MAPPING_TABLE_QUERY = """
    CREATE TABLE musicbrainz.msd_mb_mapping (
        count INTEGER,
        msb_artist_name     TEXT,
        msb_artist_msid     UUID,
        msb_recording_name  TEXT,
        msb_recording_msid  UUID,
        msb_release_name    TEXT,
        msb_release_msid    UUID,
        mb_artist_name      TEXT,
        mb_artist_credit_id INTEGER, 
        mb_recording_name   TEXT,
        mb_recording_id     INTEGER,
        mb_release_name     TEXT,
        mb_release_id       INTEGER,
        source              TEXT
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

def mem_stats():
    process = psutil.Process(os.getpid())
    return "%d MB" % (process.memory_info().rss / 1024 / 1024)


def create_table(conn):

    with conn.cursor() as curs:
        while True:
            try:
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

            
def load_MSB_recordings(stats):

    msb_recordings = []

    count = 0
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MSB_RECORDINGS_QUERY)
            while True:
                msb_row = curs.fetchone()
                if not msb_row:
                    break

                artist = msb_row[0]
                artist_msid = msb_row[1]
                recording = msb_row[2]
                recording_msid = msb_row[3]
                release = msb_row[4] or ""
                release_msid = msb_row[5] or None
                    
                if REMOVE_NON_WORD_CHARS:
                    artist = re.sub(r'\W+', '', artist)
                    recording = re.sub(r'\W+', '', recording)
                    release = re.sub(r'\W+', '', release)

                msb_recordings.append({ 
                    "artist_name" : artist,
                    "artist_msid" : artist_msid,
                    "recording_name" : recording,
                    "recording_msid" : recording_msid,
                    "release_name" : release,
                    "release_msid" : release_msid
                })
                count += 1
                if count % 1000000 == 0:
                    print("load MSB %d, %s" % (count, mem_stats()))

    stats["msb_recording_count"] = len(msb_recordings)

    print("sort MSB recordings %d items, %s" % (len(msb_recordings), mem_stats()))
    msb_recording_index = list(range(len(msb_recordings)))
    msb_recording_index = sorted(msb_recording_index, key=lambda rec: (msb_recordings[rec]["artist_name"], msb_recordings[rec]["recording_name"]))

    return (msb_recordings, msb_recording_index)


def load_MB_recordings(stats):

    mb_recordings = []
    count = 0
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MB_RECORDINGS_QUERY)
            while True:
                mb_row = curs.fetchone()
                if not mb_row:
                    break

                artist = mb_row[0]
                artist_credit_id = int(mb_row[1])
                recording = mb_row[2]
                recording_id = int(mb_row[3])
                release = mb_row[4]
                release_id = int(mb_row[5])
                if REMOVE_NON_WORD_CHARS:
                    artist = re.sub(r'\W+', '', artist)
                    recording = re.sub(r'\W+', '', recording)
                    release = re.sub(r'\W+', '', release)

                mb_recordings.append({ 
                    "artist_name" : artist,
                    "artist_credit_id" : artist_credit_id,
                    "recording_name" : recording,
                    "recording_id" : recording_id,
                    "release_name" : release,
                    "release_id" : release_id,
                })
                count += 1
                if count % 1000000 == 0:
                    print("load MB %d, %s" % (count, mem_stats()))

    print("sort MB recordings %d items, %s" % (len(mb_recordings), mem_stats()))
    mb_recording_index = list(range(len(mb_recordings)))
    mb_recording_index = sorted(mb_recording_index, key=lambda rec: (mb_recordings[rec]["artist_name"], mb_recordings[rec]["recording_name"]))

    return (mb_recordings, mb_recording_index)


def match_recordings(stats, msb_recordings, msb_recording_index, mb_recordings, mb_recording_index, source):

    recording_mapping = {}
    mb_index = -1
    msb_index = -1
    msb_row = None
    mb_row = None
    count  = 0
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

#        pp = "%-37s %-37s = %-27s %-37s %s" % (msb_row["artist_name"][0:25], msb_row["recording_name"][0:25], 
#            mb_row["artist_name"`][0:25], mb_row["recording_name"][0:25], msb_row["recording_msid"][0:8])
        if msb_row["artist_name"] > mb_row["artist_name"]:
#            print("> %s" % pp)
            mb_row = None
            continue

        if msb_row["artist_name"] < mb_row["artist_name"]:
#            print("< %s" % pp)
            msb_row = None
            continue

        if msb_row["recording_name"] > mb_row["recording_name"]:
#            print("} %s" % pp)
            mb_row = None
            continue

        if msb_row["recording_name"] < mb_row["recording_name"]:
#            print("{ %s" % pp)
            msb_row = None
            continue

#        print("= %s %s" % (pp, mb_row["recording_id"]))

        k = "%s=%s" % (msb_row["recording_msid"], mb_row["recording_id"])
        try:
            recording_mapping[k][0] += 1
        except KeyError:
            recording_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]

        count += 1
        msb_row = None
        if count % 1000000 == 0:
            print("%d matches, %s" % (count, mem_stats()))

    stats["recording_mapping_count"] = len(recording_mapping)

    print("Calculate listen count histogram")
    top_index = []
    for k in recording_mapping:
        top_index.append((recording_mapping[k][0], k))

    print("write records to disk")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            register_uuid(curs)
            rows = []
            total = 0
            for count, k in sorted(top_index, reverse=True):
                a = recording_mapping[k]
                row = (a[0],
                    msb_recordings[a[1]]["artist_name"], 
                    msb_recordings[a[1]]["artist_msid"], 
                    msb_recordings[a[1]]["recording_name"], 
                    msb_recordings[a[1]]["recording_msid"], 
                    msb_recordings[a[1]]["release_name"], 
                    msb_recordings[a[1]]["release_msid"], 

                    mb_recordings[a[2]]["artist_name"],
                    mb_recordings[a[2]]["artist_credit_id"],
                    mb_recordings[a[2]]["recording_name"],
                    mb_recordings[a[2]]["recording_id"],
                    mb_recordings[a[2]]["release_name"],
                    mb_recordings[a[2]]["release_id"],
                    source
                    )
                total += 1
                if len(rows) == 2000:
                    insert_mapping_rows(curs, rows)
                    rows = []

                if total % 1000000 == 0:
                    print("wrote %d of %d, %s" % (total, len(recording_mapping), mem_stats()))
                    conn.commit()

            print("insert last mapping bits: %d" % len(rows))
            insert_mapping_rows(curs, rows)
            conn.commit()

        stats['msid_mbid_mapping_count'] = total


    stats["completed"] = datetime.datetime.utcnow().isoformat()


#                no_paren_rec = recording[:recording.find("("):].strip()
#                if no_paren_rec != recording:
#                    msb_recordings.append((artist, artist_mbid, no_paren_rec, recording_mbid, release, release_mbid, recording))
#                    no_paren_recordings += 1
#                    no_paren_rec = re.sub(r'\W+', '', no_paren_rec)

def calculate_msid_mapping():

    stats = {}
    stats["started"] = datetime.datetime.utcnow().isoformat()
    stats["git commit hash"] = subprocess.getoutput("git rev-parse HEAD")

    print("Load MSB recordings")
    msb_recordings, msb_recording_index = load_MSB_recordings(stats)

    print("Load MB recordings")
    mb_recordings, mb_recording_index = load_MB_recordings(stats)

    print("match recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        create_table(conn)

    match_recordings(stats, msb_recordings, msb_recording_index, mb_recordings, mb_recording_index, SOURCE_NAME)

    print("free memory, %s" % mem_stats())
    msb_recordings = None
    msb_recording_index = None
    mb_recordings = None
    mb_recording_index = None
    gc.collect()
    print("post gc, %s" % mem_stats())

    print("create indexes")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        create_indexes(conn)

    with open("mapping-stats.json", "w") as f:
        f.write(ujson.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    calculate_msid_mapping()
