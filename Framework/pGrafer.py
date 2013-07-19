import urllib
import urllib2
import json

import json
import re
import sys
from datetime import datetime
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, URIRef, BNode, XSD
import urllib, urllib2
import gzip
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO    
import os

from describeResult import DescribeReply
from Formatters.describeReplyToRDF import DescribeRepliesToSGraph

"""
PGrafer leverages the more granular DescribeReply and DescribeRepliesToSGraph to produce an RDF graph for a patient OR a report on the data available for a patient.
"""
class PGrafer:

    # These queries are either filter based (graph style) or dinum-based (containment style)
    # Problem demo system: 55-1 for Pharma and 63-4 for Lab ... want 9
    DEFAULT_QUERY_TEMPLATES = ["DESCRIBE 2-%(patientId)s CSTOP 100", "DESCRIBE 120_5 FILTER(.02=2-%(patientId)s) CSTOP 100", "DESCRIBE 120_8 FILTER(.01=2-%(patientId)s) CSTOP 100", "DESCRIBE 55 FILTER(.01=2-%(patientId)s) CSTOP 100", "DESCRIBE 52 FILTER(2=2-%(patientId)s) CSTOP 100", "DESCRIBE 74 FILTER(2=2-%(patientId)s) CSTOP 100", "DESCRIBE 63-%(patientId)s) CSTOP 100", "DESCRIBE 9000011 FILTER(.02=9000001-%(patientId)s) CSTOP 100", "DESCRIBE 8925 FILTER(.02=9000001-%(patientId)s) CSTOP 100"]

    """
    fmqlEP: Endpoint
    queryTemplates: ; array of templates that take one field - "patientId" ex/ "DESCRIBE 63_04-{patientId}s"
    fms: FileMan Schema (vs or chcss)
    systemBase: base for URL in graph ex/ http://livevista.caregraf.info
    """
    def __init__(self, fmqlEP, queryTemplates=None, fms="vs", systemBase="", describeReplyEnhancer=None):
        self.fmqlEP = fmqlEP
        self.queryTemplates = queryTemplates if queryTemplates else self.DEFAULT_QUERY_TEMPLATES
        self.systemBase = systemBase if systemBase else fmqlEP.split("fmqlEP")[0]
        self.fms = fms
        self.describeReplyEnhancer = describeReplyEnhancer
        
    """
    patientId: IEN of patient in file 2
    
    Returns StringIO - caller can post on (if web service) or save to disk
    """
    def graphPatient(self, patientId):
                
        gdRDF = DescribeRepliesToSGraph(self.fms, self.systemBase)
        
        for queryTemplate in self.queryTemplates:
        
            # Hack for CG Test system til fix to reflect dinum's
            if re.match(r'DESCRIBE 63', queryTemplate):
                query = queryTemplate % {"patientId": "4"}
            else:      
                query = queryTemplate % {"patientId": patientId}
            queryURL = self.fmqlEP + "?" + urllib.urlencode({"fmql": query}) 
            reply = json.loads(urllib2.urlopen(queryURL).read())
            dr = DescribeReply(reply)
            if dr.stopped():
                raise Exception("CSTOP too small - containment for %s is stopped" % query)
            gdRDF.processReply(dr)
            
        return gdRDF.done()
            
# ##################### DEMO ######################

# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"     

def main():

    start = datetime.now()
    pger = PGrafer(FMQLEP)
    print "Graphing Patient 9 - Christopher Jones"
    rdfGr = pger.graphPatient("9") # IEN 9 == Christopher Jones in CG test system
    rdfFileName = "pGrafLiveVistACG_9.xml"        
    open(rdfFileName, "w").write(rdfGr.getvalue())
    print "Fetched and wrote graph in", datetime.now()-start
    
if __name__ == "__main__":
    main()
