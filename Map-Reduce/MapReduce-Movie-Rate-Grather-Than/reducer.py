#!/usr/bin/env python
# coding: utf-8

import sys

current_movie = None

for line in sys.stdin:
    movie_id , rating = line.rstrip().split('\t')    

    if current_movie != movie_id :
        print('%s,%s' % (movie_id, rating))
        current_movie = movie_id

