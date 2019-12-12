import sys
import csv
import psycopg2
from settings import USE_MINIMAL_DATASET

# TODO: Make this work for punct removed data as well

TEST_PAIRS_QUERY = '''
    SELECT m.recording_id
      FROM musicbrainz.recording_artist_credit_pairs m
     WHERE artist_credit_name = %s 
       AND recording_name = %s
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


def test_pairs():

    data = _read_test_data("bin/test/mapping_test_cases.csv")

    passed = 0
    failed = 0

    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            for rdata in data:
                curs.execute(TEST_PAIRS_QUERY, (rdata[1], rdata[0]))
                row = curs.fetchone()
                if row:
                    passed += 1
                else:
                    failed += 1
                    print("no match for '%s' '%s'" % (rdata[0], rdata[1]))

                        if row[0] != int(rdata[2]):
                            mbid = get_mbid_for_release_id(mb_curs, int(row[0]))
                            print("'%s' '%s' expected %s, got %s (%s)" % (rdata[0], rdata[1], rdata[2], row[0], mbid))
                            failed += 1
                        else:
                            print("'%s' '%s' ok" % (rdata[0], rdata[1]))
                            passed += 1
    print("%d passed, %d failed." % (passed, failed))
    if failed == 0:
        sys.exit(0)
    else:
        sys.exit(-1)

if __name__ == "__main__":
    test_pairs()
