To build the container:

./run.sh build

To run the container so we can start programs in it:

./run.sh up

To create the MSID mapping you'll first need to run create recordings pairs script:

./run.sh bin/create_recording_pairs.py

Once this completes, run the create msid mapping.py script:

./run.sh bin/create_msid_mapping.py

Now the mapping is complete. Test it using:

./run.sh bin/test.sh


Then you can write the mapping dump files to disk:

./run.sh write_mapping.py

To dump the HTML static pages

./run.sh bin/dump_results.py

To host the static files in a docker container:

docker rm -f messybrainz-results ; docker run --name messybrainz-output -p 80:80 --name messybrainz-results -v `pwd`/html:/usr/share/nginx/html:ro -d nginx

To get a DB prompt:

docker exec -it musicbrainz-docker_db_1 psql -U musicbrainz messybrainz
docker exec -it musicbrainz-docker_db_1 psql -U musicbrainz musicbrainz_db


Database preparation
====================

createuser -U musicbrainz -s -P msbpw

CREATE INDEX artist_name_ndx_recording_json ON recording_json ((data ->> 'artist'));
CREATE INDEX artist_credit_name_ndx_recording_artist_credit_pairs ON musicbrainz.recording_artist_credit_pairs(artist_credit_name);

CREATE EXTENSION musicbrainz_unaccent;




