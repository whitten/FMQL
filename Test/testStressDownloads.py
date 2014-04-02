#!/usr/bin/env python

import sys
import urllib, urllib2
import json
from datetime import datetime
    
sys.path.append("../Framework")
from fmqlInterface import FMQLInterface
from cacheObjectInterface import CacheObjectInterface
    
"""
Simple stresses/ downloads
"""

def describeWholeFile(fmqlIF, fileType, limit=1000, cstop=1000):
    
    afterien = 0
    start = datetime.now()
    
    while True:

        # Ex/ DESCRIBE 52 LIMIT 1000 AFTERIEN 100 CSTOP 1000 - only AFTERIEN varies
        query = "DESCRIBE %(fileType)s LIMIT %(limit)s AFTERIEN %(afterien)s CSTOP %(cstop)s" % {"fileType": fileType, "limit": limit, "afterien": afterien, "cstop": 1000}
        
        try:
        
            response = fmqlIF.invokeQuery(query)

        except: # timeout
            print "... ending early after", datetime.now() - start, "at afterien", afterien
            raise
            
        try:
            jreply = json.loads(response.read())
        except:
            print "... ending early after", datetime.now() - start, "at afterien", afterien
            raise Exception("Got invalid/non JSON reply: " + str(reply))
            
        # catch error (won't happen)
        if "error" in jreply:
            print "... ending early after", datetime.now() - start, "at afterien", afterien
            raise Exception(jreply["error"])
        
        noResults += len(jreply["results"])
        
        # at the end - get back less than asked for
        if len(jreply["results"]) < limit:
            print "got last", jreply["results"], "so done"
            break
            
        if noResults % 100000 == 0:
            print "Got another 100K bringing total to", noResults
            
        # go again: form of id in each description is {filetype}-{ien}
        # ... get id (uri) of last result
        afterien = jreply["results"][-1]["uri"]["value"].split("-")[1]
        
    print "Looped ", noResults, "of file", fileType, "in", datetime.now() - start
    
# ############################### Driver ############################
            
def main():

    CHCSFMQLCSPEP_WINDOWS = "http://10.255.167.116:57772/csp/fmquery/FMQL.csp"
    CHCSFMQLCSPEP_VMS = "http://10.255.181.20/chcs/csp/fmquery/FMQL.csp"    

    coi = CacheObjectInterface(CHCSFMQLCSPEP_WINDOWS) # Windows
    describeWholeFile(coi, "2", limit=2)
                     
if __name__ == "__main__":
    main()
