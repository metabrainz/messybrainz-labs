#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config


class ArtistCreditRecordingPairsLookupQuery(Query):

    def names(self):
        return ("acrp-lookup", "MusicBrainz Artist Credit Recording Pairs lookup")

    def inputs(self):
        return ['artist_credit_name', 'recording_name']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a match in MusicBrainz."""

    def outputs(self):
        return ['artist_credit_name', 'release_name', 'recording_name', 
                'artist_credit_id', 'release_mbid', 'recording_mbid']

    def fetch(self, params, offset=-1, limit=-1):
        artists = []
        recordings = []
        for param in params:
            artists.append("".join(param['artist_credit_name'].lower().split()))
            recordings.append("".join(param['recording_name'].lower().split()))
        artists = tuple(artists)
        recordings = tuple(recordings)

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT ac.name AS artist_credit_name, 
                                       rl.name AS release_name, 
                                       r.name AS recording_name,
                                       rl.gid AS release_mbid,
                                       r.gid AS recording_mbid,
                                       artist_credit_id
                                  FROM mapping.recording_artist_credit_pairs
                                  JOIN recording r
                                    ON r.id = recording_id
                                  JOIN release rl
                                    ON rl.id = release_id
                                  JOIN artist_credit ac
                                    ON r.artist_credit = ac.id
                                 WHERE artist_credit_name IN %s
                                   AND recording_name IN %s""", (artists, recordings))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
