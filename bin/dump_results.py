#!/usr/bin/env python3

import sys
import pprint
import os
import ujson
import re
from operator import itemgetter

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
# MSB
#    SELECT lower(musicbrainz.musicbrainz_unaccent(rj.data->>'artist'::TEXT)) AS artist_name, artist as artist_msid,
#           lower(musicbrainz.musicbrainz_unaccent(rj.data->>'title'::TEXT)) AS recording_name, gid AS recording_msid
#           lower(musicbrainz.musicbrainz_unaccent(rl.title)) AS release_name, rl.gid AS release_msid
# MB
#    SELECT DISTINCT lower(musicbrainz.musicbrainz_unaccent(artist_credit_name)) as artist_credit_name, artist_mbids, 
#           lower(musicbrainz.musicbrainz_unaccent(recording_name)) AS recording_name, recording_mbid
#           lower(musicbrainz.musicbrainz_unaccent(release_name)) AS release_name, release_mbid

    print("load & categorize recordings")
    categories = {}
    with open("recordings.json", "r") as j:
        while True:
            line = j.readline()
            if not line:
                break

            total_count += 1
            data = ujson.loads(line)
            k = data[3]
            if re.search("^\w\w", k, flags=re.A):
                k0 = data[3][0]
                k1 = data[3][1]
            elif re.search("^\W\w", k, flags=re.A):
                k0 = "*"
                k1 = data[3][1]
            elif re.search("^\w\W", k, flags=re.A):
                k0 = data[3][0]
                k1 = "*"
            elif re.search("^\W\W", k, flags=re.A):
                k0 = "*"
                k1 = "*"

            while True:
                try:
                    categories[k0][k1].append(data)
                    break
                except KeyError:
                    if not k0 in categories:
                        categories[k0] = {}
                    if not k1 in categories[k0]:
                        categories[k0][k1] = []


    print("output recordings")
    try:
        os.makedirs("html/recording")
    except FileExistsError:
        pass

    for top_cat in categories.keys():
        for bot_cat in categories[top_cat].keys():
            with open("html/recording/%s%s.html" % (top_cat, bot_cat), "w") as f:
                cat = top_cat + bot_cat
                f.write('<html><head><meta charset="UTF-8"><title>%s recordings</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></head><body>\n' % cat)
                f.write("<h1>%s recordings</h1>" % cat)
                f.write("<p>count: %d</p>" % len(categories[top_cat][bot_cat]))
                f.write("<table><tr><th>MsB recording</th><th>MsB artist</th><th>MB artist</th>")
                f.write("<th>MB recording</th><th>MB release</th><th>rec id</th><th>rel id</th></tr>\n")

                for count, msb_artist_name, msb_artist_id, msb_recording_name, msb_recording_id, msb_release_name, msb_release_id, \
                    mb_artist_name, mb_artist_ids, mb_recording_name, mb_recording_id, mb_release_name, mb_release_id in sorted(categories[top_cat][bot_cat], key=itemgetter(3,5,7)):

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


    with open("html/recording/index.html", "w") as f:
        f.write('<html><head><meta charset="UTF-8"><title>artist matches</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></link></head><body>\n')
        f.write("<h1>recording matches</h1>")
        for top_cat in sorted(categories.keys()):
            f.write('<h3>%s</h3><p>' % top_cat)
            for bot_cat in sorted(categories[top_cat].keys()):
                cat = top_cat + bot_cat
                f.write('<a href="%s.html">%s</a> ' % (cat, cat))
            f.write('</p>')
            
        f.write("<p></body></html>\n")


if __name__ == "__main__":
    dump_recordings_to_html()
