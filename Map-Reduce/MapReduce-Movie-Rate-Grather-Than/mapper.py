#!/usr/bin/env python
# coding: utf-8


import sys

for line in sys.stdin:
    record = line.strip().split("\t")
    movie_id=record[1]
    rating=record[2]
    
    try:
        rating = int(rating)
    except ValueError:
        continue
        
    if(rating > 3):    
        print('%s\t%s' % (movie_id, rating))





