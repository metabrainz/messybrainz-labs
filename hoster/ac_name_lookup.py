from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

class ArtistCreditNameLookupQuery(Query):

    def names(self):
        return ("artist-credit-name-lookup", "MusicBrainz Artist Credit Name Lookup")

    def inputs(self):
        return ['artist_credit_name']

    def introduction(self):
        return """Look up artist credit ids from artist names. Artist names must be spelled
                  exactly (except for case) as they are in MusicBrainz."""

    def outputs(self):
        return ['artist_credit_id', 'artist_credit_name', 'disambiguation']

    def fetch(self, params, offset=-1, limit=-1):

        ac_names = [ p['artist_credit_name'].lower() for p in params ]
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT ac.id AS artist_credit_id, 
                                       ac.name AS artist_credit_name, 
                                       a.comment AS disambiguation
                                  FROM musicbrainz.artist_credit ac
                                  JOIN musicbrainz.artist a
                                    ON ac.id = a.id
                                 WHERE lower(ac.name) IN %s
                              ORDER BY ac.name, ac.id""", (tuple(ac_names),))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    acs.append(dict(row))

                return acs
