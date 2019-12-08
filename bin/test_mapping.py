import csv
import psycopg2
from settings import USE_MINIMAL_DATASET

TEST_MAPPING_QUERY = '''
    SELECT m.mb_release_id
      FROM musicbrainz.msd_mb_mapping m
     WHERE msb_artist_name = %s 
       AND msb_recording_name = %s
'''

def _read_test_data(filename):
    data = []
    with open(filename, newline='') as csvfile:
         reader = csv.reader(csvfile, delimiter=',', quotechar='"')
         for row in reader:
             if not row:
                if USE_MINIMAL_DATASET:
                    break
                else:
                    continue
             data.append(row)

    return data


def test_mapping():
    ''' This test will actually run as many test as there are in the CSV file '''

    data = _read_test_data("bin/test/mapping_test_cases.csv")

    passed = 0
    failed = 0

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            for rdata in data:
                curs.execute(TEST_MAPPING_QUERY, (rdata[1], rdata[0]))
                row = curs.fetchone()
                if not row:
                    print("no match for '%s' '%s'" % (rdata[0], rdata[1]))
                    failed += 1
                    continue

                if row[0] != int(rdata[2]):
                    print("'%s' '%s' expected %s, got %s" % (rdata[0], rdata[1], rdata[2], row[0]))
                    failed += 1
                else:
                    print("'%s' '%s' ok" % (rdata[0], rdata[1]))
                    passed += 1

    print("%d passed, %d failed." % (passed, failed))

if __name__ == "__main__":
    test_mapping()
