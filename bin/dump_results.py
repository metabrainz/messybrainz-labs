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
                    mb_artist_name, mb_artist_gids, mb_recording_name, mb_recording_gid, mb_release_name, mb_release_gid, source
      FROM musicbrainz.msd_mb_mapping
  ORDER BY msb_recording_name, msb_artist_name, msb_release_name, mb_artist_name, mb_recording_name, mb_release_name,
           msb_artist_msid, msb_recording_msid, msb_release_msid, mb_artist_gids, mb_recording_gid, mb_release_gid, source
""";

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


def output_line(f, data, count):

    data['mb_artist_ids']= [ id for id in data['mb_artist_ids'][1:-1].split(',') ] 
    f.write('<tr>')
    f.write('<td>%d</td>' % count)
    f.write('<td title="%s">%s</td>' % (data['msb_recording_id'], data['msb_recording_name']))
    f.write('<td title="%s">%s</td>' % (data['msb_artist_id'], data['msb_artist_name']))
    f.write('<td>')
    f.write('<a title="%s" href="https://musicbrainz.org/artist/%s">%s</a> ' % (data['mb_artist_ids'][0], data['mb_artist_ids'][0], data['mb_artist_name']))
    for i, mbid in enumerate(data['mb_artist_ids'][1:]):
        f.write('<a title="%s" href="https://musicbrainz.org/artist/%s">%s</a> ' % ((mbid, mbid, "id #%d" % i)))

    f.write('</td>')
    f.write('<td><a title="%s" href="https://musicbrainz.org/recording/%s">%s</a></td>' % (data['mb_recording_id'], data['mb_recording_id'], data['mb_recording_name']))
    f.write('<td><a title="%s" href="https://musicbrainz.org/release/%s">%s</a></td>' % (data['mb_release_id'], data['mb_release_id'], data['mb_release_name']))
    f.write('<td title="%s">%s</td>' % (data['mb_recording_id'], data['mb_recording_id'][0:6]))
    f.write('<td title="%s">%s</td>' % (data['mb_release_id'], data['mb_release_id'][0:6]))
    f.write('<td>%s</td>' % data['source'])
    f.write('</tr>\n')


def dump_recordings_to_html():

    total_count = 0

    with open("recording-pairs-stats.json", "r") as f:
        pair_stats = ujson.loads(f.read())

    with open("mapping-stats.json", "r") as f:
        mapping_stats = ujson.loads(f.read())

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
                row = {}
                row['msb_artist_name'], row['msb_artist_id'], row['msb_recording_name'], row['msb_recording_id'], row['msb_release_name'], row['msb_release_id'], \
                    row['mb_artist_name'], row['mb_artist_ids'], row['mb_recording_name'], row['mb_recording_id'], row['mb_release_name'], row['mb_release_id'], row['source'] = data

                category = ""

                for i in range(NUM_LEVELS):
                    ch = row['msb_recording_name'][i : i+1]
                    if re.search("^\w", ch, flags=re.A):
                        category += ch
                    else:
                        category += '*'

                try:
                    categories[category].append(row)
                except KeyError:
                    categories[category] = [row]



    print("output recordings")
    try:
        os.makedirs("html/recording")
    except FileExistsError:
        pass

    for cat in sorted(categories.keys()):
        dest_dir = os.path.join("html", "recording", *(list(cat[0:-1]))) 
        try:
            os.makedirs(dest_dir)
        except FileExistsError:
            pass

        with open(os.path.join(dest_dir, "%s.html" % cat[-1:]), "w") as f:
            f.write('<html><head><meta charset="UTF-8"><title>%s recordings</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></head><body>\n' % cat)
            f.write("<h1>%s recordings</h1>" % cat)
            f.write("<p>count: %d</p>" % len(categories[cat]))
            f.write('<p><a href="/recording/index.html">top index</a></p>')
            f.write("<table><tr><th>num</th><th>MsB recording</th><th>MsB artist</th><th>MB artist</th>")
            f.write("<th>MB recording</th><th>MB release</th><th>rec id</th><th>rel id</th></tr>\n")

            duplicate_count = 0
            last = None
            for d in sorted(categories[cat], key=itemgetter('msb_recording_name', 'msb_artist_name', 'mb_artist_name', 'mb_recording_name', 'mb_release_name')):

                if not last:
                    last = d

                # If rows are the same as the previous row, skip it
                if d['msb_artist_name'] != last["msb_artist_name"] or \
                    d['msb_recording_name'] != last["msb_recording_name"] or \
                    d['mb_artist_name'] != last["mb_artist_name"] or \
                    d['mb_recording_name'] != last["mb_recording_name"] or \
                    d['mb_release_name'] != last["mb_release_name"]:

                    output_line(f, last, duplicate_count)
                    duplicate_count = 0

                duplicate_count += 1
                last = d

            output_line(f, last, duplicate_count)
            f.write("</table></body></html>\n")

    print("write indexes")
    write_indexes("", categories, os.path.join("html", "recording"), pair_stats, mapping_stats)
    with open(os.path.join("html", "index.html"), "w") as f:
        f.write('<html><body><h1>data sets</h1><a href="recording/index.html">recording mapping</a></body></html>')

    print("done")


def write_indexes(level, categories, dest_dir, pair_stats, mapping_stats):

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
        stats = "<h3>Artist recording pair stats</h3><table>"

        pair_stats["MB release coverage"] = "%.1f%%" % (100 * int(pair_stats["recording_pair_release_count"]) / float(pair_stats["mb_release_count"]))
        pair_stats["MB recording coverage"] = "%.1f%%" % (100 * int(pair_stats["recording_artist_pair_count"]) / float(pair_stats["mb_recording_count"]))
        for key in ("started", "recording_pair_release_count", "mb_release_count", "MB release coverage", "recording_artist_pair_count", 
            "mb_release_count", "MB recording coverage", "git commit hash", "completed"):
            try:    
                stats += "<tr><td>%s</td><td>%s</td></tr>" % (key, '{:,}'.format(int(pair_stats[key])))
            except ValueError:
                stats += "<tr><td>%s</td><td>%s</td></tr>" % (key, pair_stats[key])

        stats += "</table>"
        stats += "<h3>Mapping stats</h3><table>"
        mapping_stats["MSID mapping coverage"] = "%.1f%%" % (100 * int(mapping_stats["msid_mbid_mapping_count"]) / float(mapping_stats["msb_recording_count"]))
        for key in ("started", "recording_mapping_count", "msid_mbid_mapping_count", "msb_recording_count", 
            "MSID mapping coverage", "git commit hash", "completed"):
            try:    
                stats += "<tr><td>%s</td><td>%s</td></tr>" % (key, '{:,}'.format(int(mapping_stats[key])))
            except ValueError:
                stats += "<tr><td>%s</td><td>%s</td></tr>" % (key, mapping_stats[key])
        stats += "</table>"
    else:
        file_name = "index-%s.html" % level
        title = "Recording matches: index %s" % level.upper()
        stats = ""

    with open(os.path.join(dest_dir, file_name), "w") as f:
        f.write('<html><head><meta charset="UTF-8"><title>%s</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/kognise/water.css@latest/dist/light.min.css"></link></head><body>\n' % title)
        f.write("<h1>%s</h1>" % title)
        f.write(stats)
        f.write("<h1>recording / artist mapping</h1>")
        if len(level) > 0:
            f.write('<p><a href="/recording/index.html">top index</a></p>')

        f.write('<p>')
        for i, item in enumerate(items):
            full_item = level + item
            if len(level) == NUM_LEVELS - 1:
                f.write('<a href="%s.html" style="margin-right: 1em">%s</a>' % (os.path.join(*list(full_item)), full_item.upper()))
            else:
                f.write('<a href="index-%s.html" style="margin-right: 2em">%s</a>' % (full_item, full_item.upper()))
            if i == (len(items)-1)//2:
                f.write("</p><p>")

        f.write('</p>')
            
        f.write("</body></html>\n")

    for ch in list("abcdefghijklmnopqrstuvwxyz0123456789_*"):
        write_indexes(level + ch, categories, dest_dir, pair_stats, mapping_stats)


if __name__ == "__main__":
    dump_recordings_to_html()
