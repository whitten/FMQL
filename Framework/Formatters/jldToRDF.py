#!/usr/bin/env python

import re 
import json
import urllib, urllib2
import StringIO
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
Utility to take a basic "Describe" reply (@graph, @context) and create RDF TTL

Relies on both pyld AND rdflib
"""

# These settings should match your FMQL configuration 
# FMQLEP = "http://www.examplehospital.com/fmqlEP" # Endpoint address
FMQLEP = "http://livevista.caregraf.info/fmqlEP" # Endpoint address of Caregraf demo VistA

def jldToRDF(jld, fileName, cacheLocation=""):
    start = datetime.now()
    # nix all but @context and @graph
    jld = {"@context": jld["@context"], "@graph": jld["@graph"]}
    qualFileName = cacheLocation + fileName + ".ttl"
    g = rdflib.ConjunctiveGraph()
    # tell RDF Lib about the namespaces
    nss = {c: jld["@context"][c] for c in jld["@context"] if not re.match(r'\@', c) and isinstance(jld["@context"][c], basestring) and re.match(r'http', jld["@context"][c])}
    for ns, nsURI in nss.iteritems():
        g.bind(ns, nsURI)
    # Now use RDFLIB to turn json-ld's NQUADS into plain old turtle
    g.load(StringIO.StringIO(jsonld.to_rdf(jld, {"format": "application/nquads"})), format="nquads")
    g.serialize(qualFileName, format="turtle")
    print "Made RDF", qualFileName, " - serialization took", datetime.now() - start

# ######################### Test/Demo ###################
    
def main():

    # Grab the vitals
    query = {"fmql": "DESCRIBE 120_5 FILTER(.02=2-9&.01>2008-04-01)", "format": "JSON-LD"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    jreply = json.loads(urllib2.urlopen(queryURL).read())
    fileName = "120_5_GT_2008"
    jldToRDF(jreply, fileName, cacheLocation="")

if __name__ == "__main__":
    main()
    