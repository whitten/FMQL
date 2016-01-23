#!/usr/bin/env python

import sys
import urllib
import urllib2
import json
import codecs
# Crude: should properly install the framework
sys.path.append("../../Framework")
from describeResult import DescribeReply
sys.path.append("../../Framework/Formatters")
from describeReplyToRDF import DescribeRepliesToSGraph

"""
Patient Grapher

The work of RDF graphing is performed by 'DescribeRepliesToSGraph', part of the FMQL Python Framework.

This utility is used behind the FMQL Apache endpoint to serialize RDF/XML. Here we use it to incrementally build a graph for a patient's data. 

We don't rely on Apache to serialize the RDF/XML from FMQL/JSON - instead we serialize it locally. Why? Because it is easier to keep track of replies in JSON than in RDF/XML itself.

Once we've made the RDF/XML, we can load it into triple stores like Jena/Fuseki or BigData.

See examples of Patient Graphs downloaded with code like this in https://github.com/caregraf/FMQL/tree/master/Datasets

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it under the terms of
# the GNU Affero General Public License version 3 (AGPL) as published by the Free Software
# Foundation.
# (c) 2014 caregraf
#
"""

# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"     

def main():
       
    print "RDFing patient 9 from the Caregraf 'Live Vista'"
    
    # We know this is a VistA, not a CHCS - hence "fms" = "vs" rather than "chcss". We
    # will also specify a base url for resources in the graph
    gdRDF = DescribeRepliesToSGraph(fms="vs", systemBase="http://livevista.caregraf.info/")

    #
    # Three types of patient data - demographics (file 5), prescriptions (52) 
    # and vitals (120.5)
    # 
    # Exercise:
    # - add more queries yourself for other types of patient data
    # - wildcard the patient identifier to provide a service to graph the data
    #   of any patient
    #
    QUERIES = ["DESCRIBE 2-9", "DESCRIBE 52 FILTER(2=2-9)", "DESCRIBE 120_5 FILTER(.02=2-9&!bound(2))"]
    
    for query in QUERIES:
        
        print "About to query", query
        queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": query}) 
        reply = json.loads(urllib2.urlopen(queryURL).read())
        dr = DescribeReply(reply)
        gdRDF.processReply(dr)
                        
    with codecs.open("vsPatientGraph9.rdf", "w", encoding="utf-8") as resultFile:
        resultFile.write(gdRDF.done().getvalue())
        resultFile.close()
        
    print "graph written out as vsPatientGraph9.rdf - put it into Jena or BigData"
    
if __name__ == "__main__":
    main()
