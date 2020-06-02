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
from Levenshtein import distance
from operator import itemgetter
from util import insert_mapping_rows

# The name of the script to be saved in the source field.
SOURCE_NAME = "fuzzy recording"

# TODO: 
#   Remove all non-word characters
#   Make fuzzing matching dependent on the length of the string

SELECT_MISPELLED_ARTIST_QUERY = '''
     SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name,
            lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name,
            map.mb_recording_gid AS recording_mbid, map.mb_artist_credit_id as artist_credit_id, r.gid AS recording_msid, r.artist AS artist_credit_msid
       FROM recording r
       JOIN recording_json rj ON r.data = rj.id 
LEFT OUTER JOIN musicbrainz.msd_mb_mapping map
         ON r.gid = map.msb_recording_msid
   ORDER BY recording_name, artist_name, mb_recording_gid
'''
#WHERE left(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)), 4) = 'guns' 

ignore = """
     SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name,
            lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name,
            map.mb_recording_gid AS recording_mbid, map.mb_artist_credit_id as artist_credit_id, r.gid AS recording_msid, r.artist AS artist_credit_msid
       INTO tmp_find_mispelings   
       FROM recording r
       JOIN recording_json rj ON r.data = rj.id 
LEFT OUTER JOIN musicbrainz.msd_mb_mapping map
         ON r.gid = map.msb_recording_msid
      WHERE left(lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)), 4) = 'guns' 
   ORDER BY recording_name, artist_name, mb_recording_gid
"""
SELECT_MISPELLED_ARTIST_QUERY_TEST = '''
     SELECT recording_name, artist_name, recording_mbid, artist_credit_id, recording_msid, artist_credit_msid
       FROM musicbrainz.tmp_find_mispelings
   ORDER BY recording_name, artist_name, recording_mbid
'''

def process_mapping_rows(rows, artist_names):

    new_matches = []
    candidates = []
    targets = {}
    for row in rows:
        if row['recording_mbid']:
            if not row['recording_mbid'] in targets:
                targets[row['recording_mbid']] = row
            try:
                artist_names[row["artist_credit_id"]].add(row['artist_name'])
            except KeyError:
                artist_names[row["artist_credit_id"]] = set()
                artist_names[row["artist_credit_id"]].add(row['artist_name'])
        else:
            candidates.append(row)


    targets = targets.values()
    if not len(targets):
        pass
#        for candidate in candidates:
#            print("  X", candidate)
#        if len(candidates):
#            print()
    else:
#        for target in targets:
#            print("===", target)
#        for candidate in candidates:
#            print("CCC", candidate)
#        if len(candidates) or len(targets):
#            print()

        assert(len(targets) == 1)
        if len(targets) == 1 and len(candidates):
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

    return new_matches


def find_mispelings(edit_distance_threshold):

    stats = {}
    stats["started"] = datetime.datetime.utcnow().isoformat()
    stats["git commit hash"] = subprocess.getoutput("git rev-parse HEAD")

    with psycopg2.connect('dbname=messybrainz_db user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(SELECT_MISPELLED_ARTIST_QUERY)

            new_matches = 0
            last_recording = ""
            last_artist = ""
            rows = []
            artist_names = {}
            while True:
                row = curs.fetchone()
                if not row:
                    break

                d_recording = distance(row['recording_name'], last_recording)
                d_artist = distance(row['artist_name'], last_artist)
                if len(rows) and d_artist > edit_distance_threshold or d_recording > edit_distance_threshold:
                    new_matches += process_mapping_rows(rows, artist_names)
                    rows = []

#                print("R '%s' - '%s' mbid: %s" % (row['recording_name'], row['artist_name'], row['recording_mbid']))
                rows.append(row)
#                print("'%s' ~ '%s' = %d '%s' ~ '%s' = %d" % (row['recording_name'], last_recording, d_recording, \
#                    row['artist_name'], last_artist, d_artist))
                last_recording = row['recording_name']
                last_artist = row['artist_name']

                if len(new_matches) == 1000:
                    insert_mapping_rows(
    print()
    print("%s new matches found." % new_matches)

    hist = {}
    for key in artist_names.keys():
        hist[key] = len(artist_names[key])

    for key in sorted(hist.items(), key=lambda kv: kv[1], reverse=True):
        if len(artist_names[key[0]]) == 1:
            continue
        print("%d: " % len(artist_names[key[0]]), artist_names[key[0]])

    stats["completed"] = datetime.datetime.utcnow().isoformat()

    with open("mispeling-stats.json", "w") as f:
        f.write(ujson.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    threshold = 4
    if len(sys.argv) == 2:
        threshold = int(sys.argv[1])
        
    find_mispelings(threshold)
