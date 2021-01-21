#!/usr/bin/env python
"""mapper.py"""

import sys

for line in sys.stdin:
    
    cited = "none"
    citing = "none"
    cited_state = "none"
    
    line = line.rstrip('\n')
    
    try:
        words = line.split(",")
    except:
        #
        # Can't split, so invalid line
        #
        continue
    
    if len(words) == 3:
        try:
            cited = words[0]
            citing = words[1]
            cited_state = words[2]
            
            print('%s\t%s\t%s' % (citing, cited, cited_state))
            
        except Exception as e:    
            pass
    
    #if len(words) >3:
    else:
            
        try:
            citing = words[0]
            
            print("%s\t%s" % (citing, ','.join(words[1:])))
            
        except Exception as e:
            pass
        
        
            