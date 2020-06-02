#!/usr/bin/env python3

import sys

from mapping.msid_mapping import create_mapping
from mapping.recording_pairs import create_pairs
from mapping.test.test_mapping import test_mapping
from mapping.test.test_pairs import test_pairs

prog = sys.argv[1]

if prog == 'create-mapping':
    create_mapping()
elif prog == 'create-pairs':
    create_pairs() 
elif prog == 'test':
#    test_pairs() 
    test_mapping() 
else:
    print("huh?")
