#!/usr/bin/env python3

from datasethoster.main import app, register_query
from msid_mapping import MSIDMappingQuery
from msid_lookup import MSIDLookupQuery
from ar_similarity import ArtistCreditSimilarityQuery
from ac_name_lookup import ArtistCreditNameLookupQuery
from ac_id_lookup import ArtistCreditIdLookupQuery
from acrp_lookup import ArtistCreditRecordingPairsLookupQuery
from recording_lookup import RecordingLookupQuery

register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())
register_query(ArtistCreditSimilarityQuery())
register_query(ArtistCreditNameLookupQuery())
register_query(ArtistCreditIdLookupQuery())
register_query(ArtistCreditRecordingPairsLookupQuery())
register_query(RecordingLookupQuery())

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
