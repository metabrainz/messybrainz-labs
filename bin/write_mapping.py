#!/usr/bin/env python3

import sys
import datetime, time
import os
import bz2
import psycopg2
import psycopg2.extras
import ujson
import tarfile
import click
from tempfile import mkstemp

DUMP_FILE = "msid-mbid-mapping%s.tar.bz2"

SELECT_QUERY = """
    SELECT DISTINCT msb_recording_msid, mb_recording_id,
                    msb_artist_msid, mb_artist_credit_id,
                    msb_release_msid, mb_release_id
               FROM musicbrainz.msd_mb_mapping
""";

SELECT_QUERY_WITH_TEXT = """
    SELECT DISTINCT msb_recording_msid, mb_recording_id, msb_recording_name,
                    msb_artist_msid, mb_artist_credit_id,
                    msb_release_msid, mb_release_id, msb_release_name
               FROM musicbrainz.msd_mb_mapping
""";

SELECT_XREF_QUERY = """SELECT id, gid FROM %s"""
SELECT_XREF_QUERY_WITH_TEXT = """SELECT id, gid, name FROM %s"""
SELECT_ARTIST_CREDITS_QUERY = """
    SELECT ac.id AS ac_id, ac.name AS ac_name, array_agg(a.gid) AS artist_mbids
      FROM artist_credit ac 
      JOIN artist_credit_name acn 
        ON ac.id = acn.artist_credit 
      JOIN artist a 
        ON acn.artist = a.id 
  GROUP BY ac.id, ac.name
""";

def load_id_xref(table, include_text):

    index = {}
    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            if include_text:
                curs.execute(SELECT_XREF_QUERY_WITH_TEXT % table)
            else:
                curs.execute(SELECT_XREF_QUERY % table)
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if include_text:
                    index[row[0]] = (row[1], row[2])
                else:
                    index[row[0]] = (row[1],)

    return index


def load_artist_credit_xref():

    index = {}
    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(SELECT_ARTIST_CREDITS_QUERY)
            while True:
                row = curs.fetchone()
                if not row:
                    break

                index[row['ac_id']] = (row['ac_name'], row['artist_mbids'][1:-1].split(","))


    return index



def dump_mapping(include_text, include_matchable, partial = False):

    print("load artist index...")
    artist_credit_index = load_artist_credit_xref()
    print("load release index...")
    release_index = load_id_xref("release", include_text)
    print("load recording index...")
    recording_index = load_id_xref("recording", include_text)

    if include_matchable:
        filename = DUMP_FILE % "-with-matchable"
    elif include_text:
        filename = DUMP_FILE % "-with-text"
    else:
        filename = DUMP_FILE % ""

    count = 0
    fh, temp_file = mkstemp()
    os.close(fh) # pesky!

    print("writing mapping to %s" % temp_file)
    with open(temp_file, "wt") as f:
        with psycopg2.connect('dbname=messybrainz_db user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                if include_text:
                    query = SELECT_QUERY_WITH_TEXT
                else:
                    query = SELECT_QUERY

                if partial:
                    query += " LIMIT 1000"

                curs.execute(query)

                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    data_dict = { 
                        "msb_recording_msid" : data["msb_recording_msid"], 
                        "mb_recording_mbid" : recording_index[int(data["mb_recording_id"])][0], 
                        "msb_artist_msid" : data["msb_artist_msid"], 
                        "mb_artist_credit_mbids" : artist_credit_index[int(data["mb_artist_credit_id"])][1],
                        "mb_artist_credit_id" : int(data["mb_artist_credit_id"]),
                        "msb_release_msid" : data["msb_release_msid"], 
                        "mb_release_mbid" : release_index[int(data["mb_release_id"])][0]
                    }
                    if include_text:
                        data_dict["msb_recording_name"] = recording_index[int(data["mb_recording_id"])][1]
                        data_dict["msb_artist_credit_name"] = artist_credit_index[int(data["mb_artist_credit_id"])][0]
                        data_dict["msb_release_name"] = release_index[int(data["mb_release_id"])][1]
                    if include_matchable:
                        data_dict["msb_recording_name_matchable"] = data["msb_recording_name"] 
                        data_dict["msb_artist_credit_name_matchable"] = artist_credit_index[int(data["mb_artist_credit_id"])][0]
                        data_dict["msb_release_name_matchable"] = data["msb_release_name"]

                    f.write(ujson.dumps(data_dict) + "\n")
                    count += 1
                    if count % 1000000 == 0:
                        print("recording: wrote %d lines" % count)

    print("create tar file...")
    with tarfile.open(filename, "w:bz2") as tf:
        tf.add(temp_file, os.path.join('msbdump', 'msid-mbid-mapping.json'))
        tf.add('admin/data_dump_files/COPYING', 'COPYING')
        tf.add('admin/data_dump_files/README', 'README')

        os.unlink(temp_file)

        with open(temp_file, "wt") as f:
            utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
            utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
            f.write(datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat())
            f.write("\n")

        tf.add(temp_file, 'TIMESTAMP')
        os.unlink(temp_file)


@click.command()
@click.option('--with-text', '-t', is_flag=True, default=False)
@click.option('--with-matchable', '-t', is_flag=True, default=False)
def dump(**opts):
    if opts['with_matchable']:
        opts['with_text'] = True
    dump_mapping(opts['with_text'], opts['with_matchable'])


if __name__ == "__main__":
    dump()
    sys.exit(0)
