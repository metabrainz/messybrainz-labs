from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config


class ArtistCreditIdLookupQuery(Query):

    def names(self):
        return ("artist-mbid-lookup", "MusicBrainz Artist Credit Lookup from Artist MBIDs")

    def inputs(self):
        return ['artist_mbid']

    def introduction(self):
        return """Look up artists from their mbids. Returns a list of artist_credit_ids and artist mbids."""

    def outputs(self):
        return ['artist_mbid', 'artist_credit_ids']

    def fetch(self, params, offset=-1, limit=-1):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                acs = tuple([ p['artist_mbid'] for p in params ])
                curs.execute("""SELECT a.gid AS artist_mbid, 
                                       array_agg(ac.id) AS artist_credit_ids
                                  FROM artist_credit ac 
                                  JOIN artist_credit_name acn 
                                    ON ac.id = acn.artist_credit 
                                  JOIN artist a 
                                    ON acn.artist = a.id 
                                 WHERE a.gid in %s
                              GROUP BY a.gid 
                              """, (acs,))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    acs.append(dict(row))

                return acs
