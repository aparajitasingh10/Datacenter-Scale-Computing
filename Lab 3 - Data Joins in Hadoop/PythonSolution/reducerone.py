#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_patent = None
values = []
state = None

for line in sys.stdin:
    
    line = line.rstrip('\n')
    
    
    try:
        key, value = line.split('\t', 1)
    except:
        print('Improperly formatted:  ', line)
        
        continue
        
    try:
        patent = int(key)
    
    except ValueError:
        continue
        
    if current_patent == None:
        current_patent = patent
        
    if current_patent == patent:
        if value.count(',') == 0:
            values.append(value)
        if value.count(',')>2:
            state = value.split(',')[4]
    
    if current_patent != patent and current_patent != None:
        
        for v in values:
            print('%s\t%s\t%s' % (current_patent, v, state))
        
        values = []
        state = None
        current_patent = patent   
        