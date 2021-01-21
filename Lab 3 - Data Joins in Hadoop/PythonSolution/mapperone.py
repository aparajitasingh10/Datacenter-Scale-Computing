#!/usr/bin/env python
"""mapper.py"""

import sys

debug = False

# input comes from STDIN (standard input)
for line in sys.stdin:
    
    ## set missing data to none
    
    citing = "none"
    cited = "none"
    
    line = line.rstrip('\n')
    # split the line into CSV fields
    try:
        words = line.split(",")
    except:
        #
        # Can't split, so invalid line
        #
        continue

    if len(words) == 2:
        #
        # It's a citation
        #
        try:
            
            citing = words[0]
            cited = words[1]
            
            print('%s\t%s' % (cited, citing) )
            
        except Exception as e:
            # improperly formed citation number
            if debug:
                print("Exception ", e);
            pass
    else:
        #
        # It's patent info 
        #
        try:
            cited = words[0]
            
            print('%s\t%s' % (cited, ','.join(words[1:])))
            
        except Exception as e:
            # improperly formed citation number
            if debug:
                print("Exception ", e);
            pass