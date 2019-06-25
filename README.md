To build the container:

docker build -f Dockerfile -t metabrainz/msid-mapping .

To run the script:

docker run -it --network=musicbrainz-docker_default metabrainz/msid-mapping


---

createuser -U musicbrainz -s -P msbpw

CREATE INDEX artist_name_ndx_recording_json ON recording_json ((data ->> 'artist'));
CREATE INDEX artist_credit_name_ndx_recording_artist_credit_pairs ON musicbrainz.recording_artist_credit_pairs(artist_credit_name);

CREATE EXTENSION musicbrainz_unaccent;
