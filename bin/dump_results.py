#!/usr/bin/env python3

import sys
import pprint
import os
import ujson
import re
from operator import itemgetter
from copy import deepcopy
import psycopg2

NUM_LEVELS = 3

SELECT_QUERY = """
    SELECT DISTINCT msb_artist_name, msb_artist_msid, msb_recording_name, msb_recording_msid, msb_release_name,  msb_release_msid,   
                    mb_artist_name, mb_artist_gids, mb_recording_name, mb_recording_gid, mb_release_name, mb_release_gid 
      FROM musicbrainz.msd_mb_mapping
  ORDER BY msb_recording_name, msb_artist_name, msb_release_name, mb_artist_name, mb_recording_name, mb_release_name,
           msb_artist_msid, msb_recording_msid, msb_release_msid, mb_artist_gids, mb_recording_gid, mb_release_gid
""";
#     WHERE msb_recording_name = 'o baby'

def dump_artists_to_html():

    total_artists = 0
    cats = "0123456789abcdefghijklmnopqrstuvwxyz"
    categories = {}
    categories['others'] = []
    for cat in cats:
        categories[cat] = []

    print("load artists")
    with open("artists.json", "r") as j:
        while True:
            line = j.readline()
            if not line:
                break

            total_artists += 1
            count, msid, msb_artist, mbids, mb_artist = ujson.loads(line)
            if msb_artist[0] in cats:
                categories[msb_artist[0]].append((count, msid, msb_artist, mbids, mb_artist))
            else:
                categories['others'].append((count, msid, msb_artist, mbids, mb_artist))


    print("output artists")
    try:
        os.makedirs("html/artist")
    except FileExistsError:
        pass

    for cat in categories.keys():
        with open("html/artist/%s.html" % cat, "w") as f:
            f.write('<html><head><meta charset="UTF-8"><title>%s artists</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></link></head><body>\n' % cat)
            f.write("<h1>%s artists</h1>" % cat)
            f.write("<p>count: %d</p>" % total_artists)
            f.write("<table><tr><th>count</th><th>msid</th><th>MsB artist</th><th>MB artist</th></tr>\n")

            for count, msid, msb_artist, mbids, mb_artist in sorted(categories[cat], key=itemgetter(2)):
                f.write('<tr><td>%5d</td><td>%s</td><td>%s</td><td>' % (count, msid, msb_artist))
                f.write('<a href="https://musicbrainz.org/artist/%s">%s</a> ' % (mbids[0], mb_artist))
                for i, mbid in enumerate(mbids[1:]):
                    f.write('<a href="https://musicbrainz.org/artist/%s">%s</a> ' % ((mbid, "additional artist #%d" % i)))
                f.write('</td></tr>\n')
            f.write("</table></body></html>\n")

    with open("html/artist/index.html", "w") as f:
        f.write('<html><head><meta charset="UTF-8"><title>artist matches</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></link></head><body>\n')
        f.write("<h1>artist matches</h1>")
        for cat in categories.keys():
            f.write('<a href="%s.html">%s</a> ' % (cat, cat))
        f.write("<p></body></html>\n")


def dump_recordings_to_html():

    total_count = 0

    print("load & categorize recordings")
    categories = {}

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_QUERY)
            while True:
                data = curs.fetchone()
                if not data:
                    break

                total_count += 1

                key = data[2]
                category = ""

                for i in range(NUM_LEVELS):
                    ch = key[i : i+1]
                    if re.search("^\w", ch, flags=re.A):
                        category += ch
                    else:
                        category += '*'

                try:
                    categories[category].append(data)
                except KeyError:
                    categories[category] = [data]



    print("output recordings")
    try:
        os.makedirs("html/recording")
    except FileExistsError:
        pass

    for cat in sorted(categories.keys()):
        print("%s" % cat, end="\b\b\b\b")
        sys.stdout.flush()
        dest_dir = os.path.join("html", "recording", *(list(cat[0:-1]))) 
        try:
            os.makedirs(dest_dir)
        except FileExistsError:
            pass

        with open(os.path.join(dest_dir, "%s.html" % cat[-1:]), "w") as f:
            f.write('<html><head><meta charset="UTF-8"><title>%s recordings</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></head><body>\n' % cat)
            f.write("<h1>%s recordings</h1>" % cat)
            f.write("<p>count: %d</p>" % len(categories[cat]))
            f.write("<table><tr><th>MsB recording</th><th>MsB artist</th><th>MB artist</th>")
            f.write("<th>MB recording</th><th>MB release</th><th>rec id</th><th>rel id</th></tr>\n")

            last_data = []
            for data in sorted(categories[cat], key=itemgetter(2, 0, 6, 8, 10)):

                # If rows are the same as the previous row, skip it
                if last_data and data[0] == last_data[0] and data[2] == last_data[2] and data[4] == last_data[4] and data[6] == last_data[6] and data[7] == last_data[7] and data[8] == last_data[8] and \
                    data[9] == last_data[9] and data[10] == last_data[10] and data[11] == last_data[11]:
                    continue
                msb_artist_name, msb_artist_id, msb_recording_name, msb_recording_id, msb_release_name, msb_release_id, \
                    mb_artist_name, mb_artist_ids, mb_recording_name, mb_recording_id, mb_release_name, mb_release_id = data
                mb_artist_ids = [ id for id in mb_artist_ids[1:-1].split(',') ] 

                last_data = data

                f.write('<tr>')
                f.write('<td title="%s">%s</td>' % (msb_recording_id, msb_recording_name))
                f.write('<td title="%s">%s</td>' % (msb_artist_id, msb_artist_name))
                f.write('<td>')
                f.write('<a title="%s" href="https://musicbrainz.org/artist/%s">%s</a> ' % (mb_artist_ids[0], mb_artist_ids[0], mb_artist_name))
                for i, mbid in enumerate(mb_artist_ids[1:]):
                    f.write('<a title="%s" href="https://musicbrainz.org/artist/%s">%s</a> ' % ((mbid, mbid, "id #%d" % i)))

                f.write('</td>')
                f.write('<td><a title="%s" href="https://musicbrainz.org/recording/%s">%s</a></td>' % (mb_recording_id, mb_recording_id, mb_recording_name))
                f.write('<td><a title="%s" href="https://musicbrainz.org/release/%s">%s</a></td>' % (mb_release_id, mb_release_id, mb_release_name))
                f.write('<td title="%s">%s</td>' % (mb_recording_id, mb_recording_id[0:6]))
                f.write('<td title="%s">%s</td>' % (mb_release_id, mb_release_id[0:6]))
                f.write('</tr>\n')
            f.write("</table></body></html>\n")

    print("write indexes")
    write_indexes("", categories, os.path.join("html", "recording"))
    print("done")


def write_indexes(level, categories, dest_dir):

    if len(level) == NUM_LEVELS:
        return

    items = []
    for ch in list("*_abcdefghijklmnopqrstuvwxyz0123456789"):
        new_level = level + ch
        for cat in categories.keys():
            if cat.startswith(new_level):
                items.append(ch)
                break

    if level == "":
        file_name = "index.html"
        title = "Recording matches: Main index"
    else:
        file_name = "index-%s.html" % level
        title = "Recording matches: index %s" % level.upper()

    with open(os.path.join(dest_dir, file_name), "w") as f:
        f.write('<html><head><meta charset="UTF-8"><title>%s</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></link></head><body>\n' % title)
        f.write("<h1>%s</h1>" % title)
        if len(level):
            f.write('<p><a href="/recording/index.html">top index</a></p>')

        f.write('<p>')
        for item in items:
            full_item = level + item
            if len(level) == NUM_LEVELS - 1:
                f.write('<a href="%s.html" style="margin-right: 2em">%s</a>' % (os.path.join(*list(full_item)), full_item.upper()))
            else:
                f.write('<a href="index-%s.html" style="margin-right: 2em">%s</a>' % (full_item, full_item.upper()))
        f.write('</p>')
            
        f.write("</body></html>\n")

    for ch in list("abcdefghijklmnopqrstuvwxyz0123456789_*"):
        write_indexes(level + ch, categories, dest_dir)


if __name__ == "__main__":
    dump_recordings_to_html()
