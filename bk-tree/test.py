#!/usr/bin/env python3

import sys

import pybktree
from Levenshtein import distance


tree = pybktree.BKTree(distance, [])
count = 0

with open("artists.txt", "r") as fin:
    while True:
        line = fin.readline()
        if not line:
            break

        tree.add(line.strip())
        count += 1
        if count % 100000 == 0:
            print(count)

while True:
    f = input("search> ")
    if not f:
        break

    for m in tree.find(f.strip(), 2)[:10]:
        print(m)
    print()
