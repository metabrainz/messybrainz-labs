To build the container:

docker build -f Dockerfile -t metabrainz/msid-mapping .

To run the script:

docker run -it --network=musicbrainz-docker_default -v `pwd`:/code/mapping metabrainz/msid-mapping

To run the artist_pairs script:

docker run -it --network=musicbrainz-docker_default -v `pwd`:/code/mapping metabrainz/msid-mapping /code/mapping/bin/create_recording_pairs.py


To dump the HTML static pages

docker build -t metabrainz/msid-mapping . && docker run -it -v `pwd`:/code/mapping --network=musicbrainz-docker_default metabrainz/msid-mapping /code/mapping/bin/dump_results.py

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




