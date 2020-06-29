#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

class MSIDLookupQuery(Query):

    def names(self):
        return ("msid-lookup", "MessyBrainz <=> MusicBrainz Lookup")

    def inputs(self):
        return ['[recording_msid]']

    def introduction(self):
        return """This page allows you to enter a MessyBrainz recording id and the query will attempt to find a 
                   match in the MessyBrainz mapping, yielding MBIDs"""

    def outputs(self):
        return ['mb_artist_name', 'mb_release_name', 'mb_recording_name', 'mb_release_mbid', 'mb_recording_mbid']

    def fetch(self, params, offset=-1, limit=-1):

        msid = tuple(params['[recording_msid]'])
        print(msid)

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT ac.name AS mb_artist_name, 
                                       rl.name AS mb_release_name, 
                                       r.name AS mb_recording_name, 
                                       rl.gid AS mb_release_mbid,
                                       r.gid AS mb_recording_mbid
                                  FROM mapping.msid_mbid_mapping
                                  JOIN recording r
                                    ON r.id = mb_recording_id
                                  JOIN release rl
                                    ON rl.id = mb_release_id
                                  JOIN artist_credit ac
                                    ON r.artist_credit = ac.id
                                 WHERE msb_recording_msid IN %s""", (msid,))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
