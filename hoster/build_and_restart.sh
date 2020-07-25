#!/bin/sh

docker build -t metabrainz/msid-mapping-hoster .
docker rm -f msid-mapping-hoster
docker run -d -p 8000:80 --name msid-mapping-hoster --network musicbrainzdocker_default metabrainz/msid-mapping-hoster
