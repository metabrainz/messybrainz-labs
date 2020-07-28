from operator import itemgetter

from datasethoster import Query
from datasethoster.main import app, register_query
import psycopg2
import psycopg2.extras
from werkzeug.exceptions import NotFound

import config


class ArtistCreditCountryLookupQuery(Query):

    def names(self):
        return ("artist-credit-id-country-code", "MusicBrainz Artist Credit Country Lookup")

    def inputs(self):
        return ['artist_credit_id']

    def introduction(self):
        return """Given artist credit ids look up areas for those artists. Any artist_credit_ids
                  not found in the database will be omitted from the results. If none are
                  found a 404 error is returned."""

    def outputs(self):
        return ['artist_credit_id', 'country_code']

    def fetch(self, params, offset=-1, limit=-1):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                acs = tuple([ r['artist_credit_id'] for r in params ])
                curs.execute(""" SELECT ac.id AS artist_credit_id, 
                                        array_agg(ar.id) AS area_id,
                                        array_agg(code) AS country_code
                                   FROM artist_credit ac 
                                   JOIN artist_credit_name acn 
                                     ON ac.id = acn.artist_credit 
                                   JOIN artist a 
                                     ON acn.artist = a.id 
                                   JOIN area ar 
                                     ON a.area = ar.id
                       FULL OUTER JOIN iso_3166_1 iso 
                                     ON iso.area = ar.id
                                  WHERE ac.id IN %s
                               GROUP BY ac.id, ar.name, iso.code""", (acs,))
                areas = []
                mapping = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    areas.extend(r['area_id'])
                    mapping.append(dict(row))

                if not areas:
                    raise NotFound("None of the given artist_credits_ids were found.")

                areas = tuple(areas)
                curs.execute("""WITH RECURSIVE area_descendants AS (
                                        SELECT entity0 AS parent, entity1 AS descendant, 1 AS depth
                                          FROM l_area_area laa
                                          JOIN link ON laa.link = link.id
                                         WHERE link.link_type = 356
                                           AND entity1 IN %s 
                                         UNION
                                        SELECT entity0 AS parent, descendant, (depth + 1) AS depth
                                          FROM l_area_area laa
                                          JOIN link ON laa.link = link.id
                                          JOIN area_descendants ON area_descendants.parent = laa.entity1
                                         WHERE link.link_type = 356
                                           AND entity0 != descendant
                                        )
                                        SELECT descendant AS area, iso.code AS country_code
                                          FROM area_descendants ad
                                          JOIN iso_3166_1 iso ON iso.area = ad.parent""", (areas,))
                area_index = {}
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    area_index[r['area']] = r['country_code']

                result = []
                for i, row in enumerate(mapping):
                    for j, (area, code) in enumerate(zip(row['area_id'], row['country_code'])):
                        if not code:
                            mapping[i]['country_code'][j] = area_index[area]

                return mapping
