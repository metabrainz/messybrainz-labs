#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

DEFAULT_LIMIT = 50


class MSIDMappingQuery(Query):

    def names(self):
        return ("msid-mapping", "MessyBrainz <=> MusicBrainz Mapping")

    def inputs(self):
        return ['msb_artist_credit_name', 'msb_recording_name']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a match in the MessyBrainz mapping."""

    def outputs(self):
        return ['mb_artist_name', 'mb_release_name', 'mb_recording_name', 'mb_release_mbid', 'mb_recording_mbid']

    def fetch(self, params, offset=-1, limit=-1):

        artist = "".join(params['msb_artist_credit_name'].lower().split())
        recording = "".join(params['msb_recording_name'].lower().split())

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT mb_artist_name, mb_release_name, mb_recording_name, 
                                       rl.gid AS mb_release_mbid,
                                       r.gid AS mb_recording_mbid
                                  FROM mapping.msid_mbid_mapping
                                  JOIN recording r
                                    ON r.id = mb_recording_id
                                  JOIN release rl
                                    ON rl.id = mb_release_id
                                 WHERE msb_artist_name = %s
                                   AND msb_recording_name = %s""", (artist, recording))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(data)

                return results


class MSIDLookupQuery(Query):

    def names(self):
        return ("msid-lookup", "MessyBrainz <=> MusicBrainz Lookup")

    def inputs(self):
        return ['recording_msid']

    def introduction(self):
        return """This page allows you to enter a MessyBrainz recording id and the query will attempt to find a 
                   match in the MessyBrainz mapping, yielding MBIDs"""

    def outputs(self):
        return ['mb_artist_name', 'mb_release_name', 'mb_recording_name', 'mb_release_mbid', 'mb_recording_mbid']

    def fetch(self, params, offset=-1, limit=-1):

        msid = params['recording_msid']

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT mb_artist_name, mb_release_name, mb_recording_name, 
                                       rl.gid AS mb_release_mbid,
                                       r.gid AS mb_recording_mbid
                                  FROM mapping.msid_mbid_mapping
                                  JOIN recording r
                                    ON r.id = mb_recording_id
                                  JOIN release rl
                                    ON rl.id = mb_release_id
                                 WHERE msb_recording_msid = %s""", (msid,))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(data)

                return results


register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
