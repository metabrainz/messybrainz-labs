#!/usr/bin/env python3

from datasethoster.main import app, register_query
from msid_mapping import MSIDMappingQuery
from msid_lookup import MSIDLookupQuery

register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
