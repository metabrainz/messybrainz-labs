#!/usr/bin/env python3

import sys
import pprint
import os
import ujson
import re
from operator import itemgetter
from copy import deepcopy

NUM_LEVELS = 3

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

# MSB
#    SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name, artist as artist_msid,
#           lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name, gid AS recording_msid
#           lower(musicbrainz.musicbrainz_unaccent(rl.title)) AS release_name, rl.gid AS release_msid
# MB
#    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
#           lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid
#           lower(musicbrainz.musicbrainz_unaccent(release_name)) AS release_name, release_mbid

    total_count = 0

    print("load & categorize recordings")
    categories = {}
    with open("recordings.json", "r") as j:
        while True:
            line = j.readline()
            if not line:
                break

            total_count += 1
            data = ujson.loads(line)

            key = data[3]
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
            for data in sorted(categories[cat], key=itemgetter(3,5,7)):

                # If rows are the same as the previous row, skip it
                if last_data and data[1] == last_data[1] and data[3] == last_data[3] and data[5] == last_data[5] and data[7] == last_data[7] and data[8] == last_data[8] and \
                    data[9] == last_data[9] and data[10] == last_data[10] and data[11] == last_data[11] and data[12] == last_data[12]:
                    print("skip")
                    continue

                count, msb_artist_name, msb_artist_id, msb_recording_name, msb_recording_id, msb_release_name, msb_release_id, \
                    mb_artist_name, mb_artist_ids, mb_recording_name, mb_recording_id, mb_release_name, mb_release_id = data

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

    if len(level) == NUM_LEVELS - 1:
        return

    items = []
    for ch in list("abcdefghijklmnopqrstuvwxyz0123456789_*"):
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
            f.write('<p><a href="/html/recording/index.html">top index</a></p>')

        for item in items:
            full_item = level + item
            if len(level) == NUM_LEVELS - 1:
                f.write('<p><a href="%s.html">%s</a></p>' % (os.path.join(*list(full_item)), full_item.upper()))
            else:
                f.write('<p><a href="index-%s.html">%s</a></p>' % (full_item, full_item.upper()))
            
        f.write("</body></html>\n")

    for ch in list("abcdefghijklmnopqrstuvwxyz0123456789_*"):
        write_indexes(level + ch, categories, dest_dir)


if __name__ == "__main__":
    dump_recordings_to_html()
