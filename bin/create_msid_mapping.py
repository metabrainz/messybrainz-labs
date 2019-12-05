#!/usr/bin/env python3

import sys
import pprint
import psycopg2
import operator
import ujson
import uuid
import datetime
import subprocess
import re
from sys import stdout
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject
from psycopg2.extras import execute_values, register_uuid
from util import insert_mapping_rows

REMOVE_NON_WORD_CHARS = True

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
    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
                    lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid,
                    lower(musicbrainz.musicbrainz_unaccent(release_name)) AS release_name, release_mbid,
                    artist_credit_id
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
        mb_artist_gids      UUID[],
        mb_artist_credit_id INTEGER, 
        mb_recording_name   TEXT,
        mb_recording_gid    UUID,
        mb_release_name     TEXT,
        mb_release_gid      UUID,
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



            
def calculate_msid_mapping():

    stats = {}
    stats["started"] = datetime.datetime.utcnow().isoformat()
    stats["git commit hash"] = subprocess.getoutput("git rev-parse HEAD")

    msb_recordings = []
    mb_recordings = []

    recording_mapping = {}

    print("load MSB recordings")
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_MSB_RECORDINGS_QUERY)
            while True:
                msb_row = curs.fetchone()
                if not msb_row:
                    break

                artist = msb_row[0]
                recording = msb_row[2]
                no_paren_rec = recording[:recording.find("("):].strip()
                release = msb_row[4] or ""
                if REMOVE_NON_WORD_CHARS:
                    artist = re.sub(r'\W+', '', artist)
                    recording = re.sub(r'\W+', '', recording)
                    no_paren_rec = re.sub(r'\W+', '', no_paren_rec)
                    release = re.sub(r'\W+', '', release)
                msb_recordings.append((artist, msb_row[1], recording, msb_row[3], release, msb_row[5], False))
                if no_paren_rec != recording:
                    msb_recordings.append((artist, msb_row[1], no_paren_rec, msb_row[3], release, msb_row[5], True))

    stats["msb_recording_count"] = len(msb_recordings)

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

                artist = mb_row[0]
                recording = mb_row[2]
                release = mb_row[4]
                if REMOVE_NON_WORD_CHARS:
                    artist = re.sub(r'\W+', '', artist)
                    recording = re.sub(r'\W+', '', recording)
                    release = re.sub(r'\W+', '', release)
                mb_recordings.append((artist, mb_row[1][1:-1].split(","), recording, mb_row[3], release, mb_row[5], mb_row[6]))

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

        pp = "%-37s %-37s = %-27s %-37s %s" % (msb_row[0][0:25], msb_row[2][0:25], mb_row[0][0:25], mb_row[2][0:25], msb_row[3][0:8])
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

        # Add a mapping entry, being careful to distinguish a normal match from a non paren match
        if not msb_row[6]:
            k = "%s=%s" % (msb_row[3], mb_row[3])
        else:
            k = "%s=%s~" % (msb_row[3], mb_row[3])
        try:
            recording_mapping[k][0] += 1
        except KeyError:
            recording_mapping[k] = [ 1, msb_recording_index[msb_index], mb_recording_index[mb_index] ]

        msb_row = None

    print("sort mapping for post processing")
    mapping_index = sorted(recording_mapping.keys())
    duplicates = []
    for i in enumerate(mapping_index):
        try:
            msb0, mb0 = mapping_index[i].split("=")
            msb1, mb1 = mapping_index[i + 1].split("=")
        except IndexError:
            break

        if msb0 == msb1 and mb1.endswith("~"):
            duplicates.append(i + 1)

    duplicates = sorted(duplicates, reverse==True)
    for d in duplicates:
        del recording_mapping[mapping_index[d]]


    print("save data to new table")
    create_table(conn)
    stats["recording_mapping_count"] = len(recording_mapping)

    top_index = []
    for k in recording_mapping:
        top_index.append((recording_mapping[k][0], k))

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            register_uuid(curs)
            rows = []
            total = 0
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
                    mb_recordings[a[2]][6],
                    mb_recordings[a[2]][2],
                    mb_recordings[a[2]][3],
                    mb_recordings[a[2]][4],
                    mb_recordings[a[2]][5],
                    SOURCE_NAME
                    ))
                total += 1
                if len(rows) == 1000:
                    insert_mapping_rows(curs, rows)
                    rows = []

            insert_mapping_rows(curs, rows)
            conn.commit()

            stats['msid_mbid_mapping_count'] = total

            print("create indexes")
            create_indexes(conn)

    stats["completed"] = datetime.datetime.utcnow().isoformat()

    with open("mapping-stats.json", "w") as f:
        f.write(ujson.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    calculate_msid_mapping()
