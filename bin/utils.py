import sys
import psycopg2
from psycopg2.errors import OperationalError
from psycopg2.extras import execute_values


FIND_UNMATCHED_MSIDS = """
    SELECT gid 
      INTO unmatched_msids
      FROM recording 
    EXCEPT 
           SELECT msb_recording_msid 
             FROM msd_mb_mapping
"""

def insert_mapping_rows(curs, values):

    query = "INSERT INTO musicbrainz.msd_mb_mapping VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")


def calculate_unmatched_msids():
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            conn.begin()
            curs.execute("DROP TABLE IF EXISTS unmatched_msids")
            curs.execute(FIND_UNMATCHED_IDS)
            conn.commit()
