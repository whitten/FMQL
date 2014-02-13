#!/usr/bin/env python

import os
import re 
import urllib, urllib2
import json
import StringIO
import shutil
from datetime import datetime
try: 
    from pyld import jsonld
except:
    raise Exception("pyld not installed - required to make RDF from JSON-LD. Download and install from https://github.com/digitalbazaar/pyld")
# http://rdflib.readthedocs.org/en/latest/gettingstarted.html
# tested with: rdflib-4.0.1-py2.7.egg/rdflib
try: 
    import rdflib 
except:
    raise Exception("rdflib not installed - required to transform RDF N-QUADS to TTL. Download from 'https://pypi.python.org/pypi/rdflib/' or use 'easy_install rdflib'")
    
"""
Simple Utility to Cache a FileMan file's content using FMQL. Uses standard library for JSON-LD and rdflib to create RDF TTL.

Used by a more comprehensive dataset maker that can cache multiple files, make a dataset description and then zip up the result.

Dependencies:
- opensource pyld (JSON-LD) library and rdflib (for RDF in Python)

TODO:
- consider using https://github.com/RDFLib/rdflib-jsonld
- try direct LOAD into Fuseki with no caching of TTL
- BIG: will need to rework CacheObjectInterface to work off direct JSON-LD from Cache in V2 ie/ will bypass Apache
"""

"""
Mandatory arguments:
- fileType: ex/ 2 or 120_5

Optional/defaulted arguments:
- limit for query: defaults to 1000
- cstop for query: defaults to 10
- filter for query: default is none
- maxNumber: maximum number to retrieve. Default is no limit (-1)
- cacheLocation: where to store the RDF. Default is in a directory /RDF under the current directory

Extras for restart (TODO: calculate from directory and name files for AFTERIEN):
- afterIEN
- queryNo 

TODO: work through JSON-LD caching as change FMQL JSON-LD for 1.2
"""

def cacheFile(fileType, limit=500, cstop=1000, filter="", maxNumber=-1, cacheLocation="", fmqlEP="", afterien=0, queryNo=0):

    # Ensure /RDF under cacheLocation
    try:
        os.makedirs(cacheLocation + "/RDF")
    except:
        pass
        
    if not fmqlEP:
        fmqlEP = "http://livevista.caregraf.info/fmqlEP" # CG Demo VistA

    queryTempl = "DESCRIBE " + fileType
    if filter:
        queryTempl += " FILTER(" + filter + ")"
    queryTempl += " LIMIT %(limit)s AFTERIEN %(afterien)s CSTOP " + str(cstop)

    # queryNo and afterIEN are usually 0 but can start again    
    # Loop until there is no more or we reach the maximum
    numberOfTypeCached = 0
    start = datetime.now()
    while True:
        queryNo += 1
        query = queryTempl % {"limit": limit, "afterien": afterien}
        print "Sending query number", queryNo, "after", numberOfTypeCached, "cached -", query
        queryURL = fmqlEP + "?" + urllib.urlencode({"fmql": query, "format": "JSON-LD"})
        reply = urllib2.urlopen(queryURL).read()
        try:
            jreply = json.loads(reply)
        except:
            print "Couldn't parse reply as JSON", reply
            raise
        if "error" in jreply:
            raise Exception(jreply["error"])
        # reset query named graph from FMQL
        jreply["id"] = "DUMMY" # removing per query graph identifier
        # V1.2 FMQL won't be necessary - normalized data makes counting harder + bug where internal references to resources of the same type leads to out of band repetition of the type
        resourcesOfType = [resource for resource in jreply["@graph"] if "type" in resource and resource["type"] == resource["type"].split(":")[0] + ":" + fileType and len(resource) > 3]
        print "... got", len(resourcesOfType), "resources"
        if len(resourcesOfType) == 0:
            break
        # REM: not doing .nq (N-QUADS) as mixed support relative to turtle and harder to read
        rdfFileName = cacheLocation + "RDF/" + fileType + "-" + str(queryNo) + "-" + str(afterien) + ".ttl"
        print "Making RDF in", rdfFileName
        g = rdflib.ConjunctiveGraph()
        # tell RDF Lib about the namespaces
        for ns, nsURI in {"prov": "http://www.w3.org/ns/prov#", "owl": "http://www.w3.org/2002/07/owl#", "vs": "http://datasets.caregraf.org/vs/", "chcss": "http://datasets.caregraf.org/chcss/"}.iteritems():
            g.bind(ns, nsURI)
        # Now use RDFLIB to turn json-ld's NQUADS into plain old turtle
        g.load(StringIO.StringIO(jsonld.to_rdf(jreply, {"format": "application/nquads"})), format="nquads")
        g.serialize(rdfFileName, format="turtle")
        numberOfTypeCached += len(resourcesOfType)
        if len(resourcesOfType) != limit:
            print "At end - < limit resources returned"
            break
        # TODO: properly reset limit at the start to make sure maximum never exceeded
        if maxNumber != -1 and numberOfTypeCached >= maxNumber:
            print "Breaking as got or exceeded maximum", maxNumber
            break
        afterien = resourcesOfType[-1]["id"].split("-")[1]
        if (queryNo % 100) == 0:
            print "So far this has taken", datetime.now() - start          
        
    print "Finished - cached", numberOfTypeCached, "as RDF in", cacheLocation + "RDF"  
    print "Took", datetime.now() - start           
    
    print "To insert into Fuseki, use (for example):"
    print "\t", "./s-post http://localhost:3030/ds/data 'GRAPHID'", rdfFileName
            
def purgeCache(cacheLocation):
    try:
        shutil.rmtree(cacheLocation)
        print "Purged cache", cacheLocation
    except:
        pass
            
# ############################# Demo Driver ####################################

def testCGVistA():
    
    cacheFile("50", limit=100, cstop="1000", cacheLocation="VSDATASETS/", maxNumber=350)
    cacheFile("120_5", filter=".02=2-9&.01>2008-04-01", limit=5, cacheLocation="DATASETS/")
    
def main():

    testCGVistA()

if __name__ == "__main__":
    main()
