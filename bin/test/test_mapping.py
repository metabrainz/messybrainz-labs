import csv
import psycopg2
from nose.tools import assert_equals


TEST_MAPPING_QUERY = '''
    SELECT m.mb_recording_gid
      FROM musicbrainz.msd_mb_mapping m
     WHERE msb_artist_name = %s 
       AND msb_recording_name = %s
'''

def _read_test_data(filename):
    data = []
    with open(filename, newline='') as csvfile:
         reader = csv.reader(csvfile, delimiter=',', quotechar='"')
         for row in reader:
             data.append(row)

    return data


def test_mapping():
    ''' This test will actually run as many test as there are in the CSV file '''

    data = _read_test_data("bin/test/mapping_test_cases.csv")

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

                assert_equals(row[0], rdata[2])
