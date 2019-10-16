#!/usr/bin/env python3

import sys
import pprint
import csv
import psycopg2
from psycopg2.extras import execute_values
import operator
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

# count | msb_artist_name |           msb_artist_msid            | msb_recording_name |          msb_recording_msid          | msb_release_name |           msb_release_msid           | mb_artist_name |             mb_artist_gids             | mb_recording_name |           mb_recording_gid           | mb_release_name |            mb_release_gid
#-------+-----------------+--------------------------------------+--------------------+--------------------------------------+------------------+--------------------------------------+----------------+----------------------------------------+-------------------+--------------------------------------+-----------------+--------------------------------------
#     1 | nina nastasia   | d9bd0cc9-e335-47b0-b8bb-dfe5342bbff8 | what's out there   | fffffe2e-1217-4bc4-b5d1-ad6ca1429727 | outlaster        | c21e90ae-ed71-4d53-88e4-cbd8be721ca7 | nina nastasia  | {3d7b2f63-31a4-434b-91c7-58f6231bd9ad} | what's out there  | 8134b95f-d10c-41e6-a919-3ce0044af356 | outlaster       | 71ccd6ba-4d8c-4851-8fb8-b3b898747afb

# ['pigs on the wing, part 1', 'pink floyd', 'aca2620e-eee7-416c-bb3b-b881b7d68780', 'animals', 'e802a957-519f-3382-a9cb-a8bb2d0be466', '20c77fb4-1c9f-33c8-9d7e-c4977f11e847']

TEST_MAPPING_QUERY = '''
    SELECT m.mb_recording_gid
      FROM musicbrainz.msd_mb_mapping m
     WHERE msb_artist_name = %s 
       AND msb_recording_name = %s
'''

def read_test_data(filename):
    data = []
    with open(filename, newline='') as csvfile:
         reader = csv.reader(csvfile, delimiter=',', quotechar='"')
         for row in reader:
             data.append(row)

    return data


def test_mapping():

    data = read_test_data("bin/test/mapping_test_cases.csv")

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            for rdata in data:
                curs.execute(TEST_MAPPING_QUERY, (rdata[1], rdata[0]))
                row = curs.fetchone()
                if not row:
                    print("no match for '%s' '%s'" % (rdata[0], rdata[1]))
                    continue

                if row[0] != rdata[2]:
                    print("'%s' '%s' expected %s, got %s" % (rdata[0], rdata[1], rdata[2], row[0]))
                else:
                    print("'%s' '%s' ok" % (rdata[0], rdata[1]))
                


if __name__ == "__main__":
    test_mapping()
