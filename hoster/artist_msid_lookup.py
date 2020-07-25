#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

class ArtistMSIDLookupQuery(Query):

    def names(self):
        return ("artist-msid-lookup", "MessyBrainz Artist <=> MusicBrainz Artist Lookup")

    def inputs(self):
        return ['[artist_msid]']

    def introduction(self):
        return """This page allows you to lookup and artist_msid and get a list of possible 
                  artist_credit_ids back."""

    def outputs(self):
        return ['artist_msid', 'artist_credit_id', 'artist_credit_mbids', 'artist_credit_name']

    def fetch(self, params, offset=-1, limit=-1):

        msid = tuple(params['[artist_msid]'])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT map.artist_msid as artist_msid,
                                       ac.id AS artist_credit_id,
                                       ac.name AS artist_credit_name,
                                       array_agg(a.gid) AS artist_credit_mbids
                                  FROM artist_credit ac 
                                  JOIN artist_credit_name acn 
                                    ON ac.id = acn.artist_credit 
                                  JOIN artist a 
                                    ON acn.artist = a.id 
                                  JOIN (SELECT DISTINCT mb_artist_credit_id AS artist_credit_id, 
                                                        msb_artist_msid AS artist_msid
                                                   FROM mapping.msid_mbid_mapping m
                                                  WHERE msb_artist_msid in %s) AS map(artist_credit_id, artist_msid)
                                    ON ac.id = map.artist_credit_id
                              GROUP BY map.artist_msid, ac.id, ac.name""", (msid,))
                results = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    results.append(dict(row))

                return results
