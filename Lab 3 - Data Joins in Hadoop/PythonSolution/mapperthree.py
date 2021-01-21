#!/usr/bin/env python
"""mapper.py"""

import sys

for line in sys.stdin:
    num = 1
    citing = "none"
    citing_state = "none"
    cited = "none"
    cited_state = "none"
    
    line = line.rstrip('\n')
    
    try:
        words = line.split(",")
    except:
        
        continue
       
    if len(words) ==4 :
        
        try:
            citing = words[0]
            citing_state = words[1]
            cited = words[2]
            cited_state = words[3]
            
            print('%s\t%s\t%s\t%s' % (citing, citing_state, cited_state, num))
            
        except Exception as e:
            pass
    else:
        
        try:
            citing = words[0]
            print("%s\t%s" % (citing, ','.join(words[1:])))
        
        except Exception as e:
            pass