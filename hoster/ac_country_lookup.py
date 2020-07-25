from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config


class ArtistCreditCountryLookupQuery(Query):

    def names(self):
        return ("artist-credit-id-country-code", "MusicBrainz Artist Credit Country Lookup")

    def inputs(self):
        return ['[artist_credit_id]']

    def introduction(self):
        return """Given artist credit ids look up areas for those artists."""

    def outputs(self):
        return ['artist_credit_id', 'area_code']

    def fetch(self, params, offset=-1, limit=-1):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                acs = tuple(params['[artist_credit_id]'])
                curs.execute(""" SELECT ac.id AS artist_credit_id, 
                                        array_agg(code) AS area_code
                                   FROM artist_credit ac 
                                   JOIN artist_credit_name acn 
                                     ON ac.id = acn.artist_credit 
                                   JOIN artist a 
                                     ON acn.artist = a.id 
                                   JOIN area ar 
                                     ON a.area = ar.id
                                   JOIN iso_3166_1 i
                                     ON i.area = a.area
                                  WHERE ac.id in %s
                               GROUP BY ac.id, ar.name""", (acs,))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    r['area_code'] = ",".join(r['area_code'])
                    acs.append(r)

                return acs
