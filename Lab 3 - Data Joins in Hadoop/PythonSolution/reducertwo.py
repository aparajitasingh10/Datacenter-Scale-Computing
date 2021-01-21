#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_patent = None
v_cited = []
v_cited_state = []
citing_state = None

for line in sys.stdin:
    
    line = line.rstrip('\n')
    
    try:
        key, value = line.split('\t', 1)
    except:
        print('Improperly formatted: ', line)
        continue
        
    try:
        patent = int(key)
        
    except ValueError:
        continue
        
    if current_patent == None:
        current_patent = patent
        
    if current_patent == patent:
        if value.count(',')==0:
            values = value.split('\t',1)
            v_cited.append(values[0])
            v_cited_state.append(values[1])
        if value.count(',')>2:
            citing_state = value.split(',')[4]
     
    if current_patent != patent and current_patent != None:
        for x, y in zip(v_cited, v_cited_state):
            print('%s\t%s\t%s\t%s' % (current_patent, citing_state, x, y))
        
        v_cited = []
        v_cited_state = []
        citing_state = None
        current_patent = patent
        
        
        