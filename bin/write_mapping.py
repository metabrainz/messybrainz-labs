#!/usr/bin/env python3

import sys
import pprint
import os
import bz2
import psycopg2
import ujson

SELECT_RECORDING_QUERY = """
    SELECT DISTINCT msb_recording_msid, mb_recording_gid FROM musicbrainz.msd_mb_mapping
""";

SELECT_ARTIST_QUERY = """
    SELECT DISTINCT msb_artist_msid, mb_artist_gids FROM musicbrainz.msd_mb_mapping
""";

SELECT_COMBINED_QUERY = """
    SELECT DISTINCT msb_recording_msid, mb_recording_gid, msb_artist_msid, mb_artist_gids, mb_artist_credit_id FROM musicbrainz.msd_mb_mapping
""";

def dump_mapping(table, filename, query):

    count = 0
    with bz2.open(filename, "wt") as f:
        with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
            with conn.cursor() as curs:
                print("run query")
                curs.execute(query)
                print("dump data")
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    f.write(ujson.dumps({ 
                        "msb_recording_msid" : data[0], 
                        "mb_recording_gid" : data[1], 
                        "msb_artist_msid" : data[2], 
                        "mb_artist_gids" : data[3][1:-1].split(","),
                        "mb_artist_credit_id" : int(data[4]) }) + "\n")
                    count += 1

                    if count % 1000000 == 0:
                        print("recording: wrote %d lines" % count)



if __name__ == "__main__":
#    dump_mapping("recording", "recording-msid-mbid-mapping.bz2", SELECT_RECORDING_QUERY)
#    dump_mapping("artist", "artist-msid-mbid-mapping.bz2", SELECT_ARTIST_QUERY)
    dump_mapping("combined", "recording-artist-msid-mbid-mapping.bz2", SELECT_COMBINED_QUERY)
