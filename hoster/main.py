#!/usr/bin/env python3

from datasethoster.main import app, register_query
from msid_mapping import MSIDMappingQuery
from msid_lookup import MSIDLookupQuery
from ar_similarity import ArtistCreditSimilarityQuery
from ac_lookup import ArtistCreditLookupQuery
from acrp_lookup import ArtistCreditRecordingPairsLookupQuery

register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())
register_query(ArtistCreditSimilarityQuery())
register_query(ArtistCreditLookupQuery())
register_query(ArtistCreditRecordingPairsLookupQuery())

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
