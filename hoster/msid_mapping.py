#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

a = """
                       SELECT DISTINCT arg.name AS arg_name, arg.rec AS arg_recording,
                                       ac.name AS mb_artist_name, 
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
                            RIGHT JOIN (values ('portishead', 'u2'), ('strangers', 'gloria')) AS arg(name, rec)
                                    ON msb_artist_name = arg.name AND msb_recording_name = arg.rec

                                 SELECT DISTINCT p.name AS ar_name, p.rec AS rec_name, ac.name AS mb_artist_name,
                                        r.name AS mb_recording_name
                                   FROM mapping.msid_mbid_mapping
                                   JOIN recording r
                                     ON r.id = mb_recording_id
                                   JOIN release rl
                                     ON rl.id = mb_release_id
                                   JOIN artist_credit ac
                                     ON r.artist_credit = ac.id
                             RIGHT JOIN (values ('u2', 'gloria'), ('portishead', 'strangers'), ('fuss', 'rump')) AS p(name, rec)
                                     ON msb_artist_name = p.name AND msb_recording_name = p.rec;
"""


class MSIDMappingQuery(Query):

    def names(self):
        return ("msid-mapping", "MessyBrainz <=> MusicBrainz Mapping")

    def inputs(self):
        return ['[msb_artist_credit_name]', '[msb_recording_name]']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a match in the MessyBrainz mapping."""

    def outputs(self):
        return ['artist_arg', 'recording_arg', 
                'mb_artist_name', 'mb_release_name', 'mb_recording_name', 
                'mb_release_mbid', 'mb_recording_mbid', 'mb_artist_credit_id']

    def fetch(self, params, offset=-1, limit=-1):
        artists = []
        for artist in params['[msb_artist_credit_name]']:
            artists.append("".join(artist.lower().split()))
        artists = tuple(artists)

        recordings = []
        for recording in params['[msb_recording_name]']:
            recordings.append("".join(recording.lower().split()))
        recordings  = tuple(recordings)

        args = []
        for artist, recording in zip(artists, recordings):
            args.append(tuple((artist, recording)))
        args = tuple(args)

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                subst = ",".join(['%s'] * len(args))
                curs.execute("""SELECT DISTINCT arg.name AS artist_arg, arg.rec AS recording_arg,
                                       ac.name AS mb_artist_name, 
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
                            RIGHT JOIN (values """ + subst + """) AS arg(name, rec)
                                    ON msb_artist_name = arg.name AND msb_recording_name = arg.rec""", args)
                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
