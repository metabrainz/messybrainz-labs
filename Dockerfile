FROM metabrainz/python:3.7

RUN apt-get update && apt-get install -y ca-certificates && \
        pip install --upgrade pip

RUN mkdir -p /etc/service/mapper
COPY mapper.service /etc/service/mapper/run
RUN chmod 755 /etc/service/mapper/run
COPY ./consul-template.conf /etc/consul-template.conf

RUN mkdir -p /code/mapper
WORKDIR /code/mapper
COPY requirements.txt /code/mapper
RUN python -m pip install -r requirements.txt
COPY . /code/mapper
