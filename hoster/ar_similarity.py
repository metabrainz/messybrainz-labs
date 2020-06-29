from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

SELECT_RELATIONS_QUERY = '''
SELECT count, arr.artist_credit_0, 
       a0.name AS artist_name_0, 
       arr.artist_credit_1, 
       a1.name AS artist_name_1 
  FROM relations.artist_credit_artist_credit_relations arr
  JOIN musicbrainz.artist a0 
    ON arr.artist_credit_0 = a0.id
  JOIN musicbrainz.artist a1 
    ON arr.artist_credit_1 = a1.id
 WHERE (arr.artist_credit_0 IN (65, 197) OR arr.artist_credit_1 IN (65, 197))
 ORDER BY count DESC
'''

def multisort(xs, specs):
    for key, reverse in reversed(specs):
        xs.sort(key=itemgetter(key), reverse=reverse)
    return xs


class ArtistCreditSimilarityQuery(Query):

    def names(self):
        return ("artist-credit-similarity", "MusicBrainz Artist Similarity")

    def inputs(self):
        return ['[artist_credit_id]', 'threshold']

    def introduction(self):
        return """Look up related artists for a given artist credit id or a list of artist credit ids."""

    def outputs(self):
        return ['count', 'artist_credit_name', 'artist_credit_id', 'related_artist_credit_name', 'related_artist_credit_id']

    def fetch(self, params, offset=-1, limit=-1):

        ac_ids = tuple(params['[artist_credit_id]'])
        threshold = int(params['threshold'])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT count, arr.artist_credit_0, 
                                       a0.name AS artist_credit_name_0, 
                                       arr.artist_credit_1,
                                       a1.name AS artist_credit_name_1 
                                  FROM relations.artist_credit_artist_credit_relations arr
                                  JOIN artist a0 
                                    ON arr.artist_credit_0 = a0.id
                                  JOIN artist a1 
                                    ON arr.artist_credit_1 = a1.id
                                 WHERE (arr.artist_credit_0 IN %s OR arr.artist_credit_1 IN %s)
                                   AND count > %s 
                                 ORDER BY count DESC""", (ac_ids, ac_ids, threshold))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if str(row['artist_credit_0']) in ac_ids:
                        relations.append(dict({
                            'count' : row['count'],
                            'artist_credit_name' : row['artist_credit_name_0'], 
                            'artist_credit_id' : row['artist_credit_0'], 
                            'related_artist_credit_id' : row['artist_credit_1'],
                            'related_artist_credit_name' : row['artist_credit_name_1']
                        }))
                    else:
                        relations.append(dict({
                            'count' : row['count'],
                            'artist_credit_name' : row['artist_credit_name_1'], 
                            'artist_credit_id' : row['artist_credit_1'], 
                            'related_artist_credit_id' : row['artist_credit_0'],
                            'related_artist_credit_name' : row['artist_credit_name_0']
                        }))

                return multisort(list(relations), (('artist_credit_id', False), ('count', True)))
