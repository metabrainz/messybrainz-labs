import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

SELECT_RELATIONS_QUERY = '''
    SELECT count, arr.artist_credit_0, a0.name AS artist_name_0, arr.artist_credit_1, a1.name AS artist_name_1 
      FROM artist_credit_artist_credit_relations arr
      JOIN artist a0 ON arr.artist_credit_0 = a0.id
      JOIN artist a1 ON arr.artist_credit_1 = a1.id
     WHERE (arr.artist_credit_0 = %s OR arr.artist_credit_1 = %s)
       AND count > 2
  ORDER BY count desc
'''

class ArtistCreditSimilarityQuery(Query):

    def names(self):
        return ("artist-credit-similarity", "MusicBrainz Artist Similarity")

    def inputs(self):
        return ['[artist_credit_id]']

    def introduction(self):
        return """Look up related artists for a given artist. Artist names must be spelled exactly 
                  as they are in the MusicBrainz database, but case is not important."""

    def outputs(self):
        return ['count', 'artist_credit_name', 'artist_credit_id']

    def fetch(self, params, offset=-1, limit=-1):

        msid = tuple(params['[recording_msid]'])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor() as curs:
                curs.execute("""SELECT artist_credit
                                  FROM artist_credit_name
                                 WHERE name = %s 
                        , (artist_credit_name,))
                row = curs.fetchone()
                if not row:
                    return None

                artist_credit_id = row[0]
                print(artist_credit_id)
                curs.execute(SELECT_RELATIONS_QUERY, (artist_credit_id, artist_credit_id))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if artist_credit_id == row[1]: 
                        relations.append({
                            'count' : row[0],
                            'artist_credit_id' : row[3],
                            'artist_credit_name' : row[4]
                        })
                    else:
                        relations.append({
                            'count' : row[0],
                            'artist_credit_id' : row[1],
                            'artist_credit_name' : row[2]
                        })

                return { 
                    'artist_credit' : artist_credit_id,
                    'artist_credit_name' : artist_credit_name,
                    'relations' : relations
                }

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT ac.name AS mb_artist_name, 
                                       rl.name AS mb_release_name, 
                                       r.name AS mb_recording_name,
                                       rl.gid AS mb_release_mbid,
                                       r.gid AS mb_recording_mbid,
                                       mb_artist_credit_id
                                  FROM mapping.msid_mbid_mapping
                                  JOIN recording r
                                    ON r.id = mb_recording_id
                                  JOIN release rl
                                    ON rl.id = mb_release_id
                                  JOIN artist_credit ac
                                    ON r.artist_credit = ac.id
                                 WHERE msb_artist_name IN %s
                                   AND msb_recording_name IN %s""", (artists, recordings))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
