from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config


class ArtistCreditIdLookupQuery(Query):

    def names(self):
        return ("artist-credit-id-lookup", "MusicBrainz Artist Credit Id Lookup")

    def inputs(self):
        return ['[artist_credit_id]']

    def introduction(self):
        return """Look up artists from their credit ids. Returns artist_credit_name and artist mbids."""

    def outputs(self):
        return ['artist_credit_id', 'artist_credit_name', 'artist_credit_mbids']

    def fetch(self, params, offset=-1, limit=-1):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                acs = tuple(params['[artist_credit_id]'])
                curs.execute("""SELECT ac.id AS artist_credit_id, 
                                       ac.name AS artist_credit_name, 
                                       array_agg(a.gid) AS artist_credit_mbids
                                  FROM artist_credit ac 
                                  JOIN artist_credit_name acn 
                                    ON ac.id = acn.artist_credit 
                                  JOIN artist a 
                                    ON acn.artist = a.id 
                                 WHERE ac.id in %s
                              GROUP BY ac.id, ac.name
                              """, (acs,))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    r['artist_credit_mbids'] = r['artist_credit_mbids'][1:-1].split(",")
                    acs.append(r)

                return acs
