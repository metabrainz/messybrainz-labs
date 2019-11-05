#!/bin/bash

if [ "$1" = "build" ]
then
    docker build -f Dockerfile -t metabrainz/msid-mapping .
    exit
fi

if [ "$1" = "up" ]
then
    docker run -d --rm --name msid-mapping-host -v `pwd`:/code/mapping --network=musicbrainz-docker_default \
        metabrainz/msid-mapping python3 bin/_dummy_loop.py
    exit
fi

if [ "$1" = "down" ]
then
    docker rm -f msid-mapping-host
    exit
fi

docker exec -it msid-mapping-host python3 $@
