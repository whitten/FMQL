#!/usr/bin/env python

import re 
import json
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
Simplest utility to take a basic "Describe" reply (@graph, @context) and create RDF TTL
"""

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
    