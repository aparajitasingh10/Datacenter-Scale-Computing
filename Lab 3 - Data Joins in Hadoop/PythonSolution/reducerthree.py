#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_patent = None
current_count = 0
citing_state = None
cited_state = None
info_line = None

for line in sys.stdin:
    
    line = line.rstrip('\n')

    
    try:
        key, value = line.split('\t',1)
        
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
        
        if value.count(',') == 3:
            
            values=value.split(',')
        
            citing_state = values[0]
            cited_state = values[1]
            count = values[2]
            
            # check if the states are the same
            
            if citing_state.lower() == cited_state.lower() and citing_state != None and cited_state != None:
                current_count += count
        
        if value.count(',') > 3:
            info_line = value
        
    if current_patent!=patent and current_patent!=None:
        
        print('%s\t%s\t%s' % (current_patent, info_line, current_count))
    
        current_patent = patent
        citing_state = None
        cited_state = None
        current_count = 0