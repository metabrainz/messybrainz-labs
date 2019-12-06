#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject
from psycopg2.extras import execute_values, register_uuid

def insert_mapping_rows(curs, values):

    query = "INSERT INTO musicbrainz.msd_mb_mapping VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")

