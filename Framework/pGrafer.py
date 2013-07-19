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
    # Problem: 55-1 for Pharma and 63-4 for Lab ... want 9
    DEFAULT_QUERY_TEMPLATES = ["DESCRIBE 2-%(patientId)s CSTOP 100", "DESCRIBE 120_5 FILTER(.02=2-%(patientId)s) CSTOP 100", "DESCRIBE 120_8 FILTER(.01=2-%(patientId)s) CSTOP 100", "DESCRIBE 55 FILTER(.01=2-%(patientId)s) CSTOP 100", "DESCRIBE 52 FILTER(.02=2-%(patientId)s) CSTOP 100", "DESCRIBE 74 FILTER(2=2-%(patientId)s) CSTOP 100", "DESCRIBE 63-%(patientId)s) CSTOP 100", "DESCRIBE 9000011 FILTER(.02=9000001-%(patientId)s) CSTOP 100", "DESCRIBE 8925 FILTER(.02=9000001-%(patientId)s) CSTOP 100"]

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
        
    """
    What's available to graph for a patient?
    """
    def reportPatientData(self, patientId):
    
        totalAssertions = 0
        noIncomplete = 0
        noRecords = 0
        noTopRecords = 0
        reportMU = ""
        recordsPerYear = defaultdict(int)
        recordsPerType = set()
        
        for noQueries, queryTemplate in enumerate(self.queryTemplates, 1):
        
            # Hack for CG Test system til fix to reflect dinum's
            if re.match(r'DESCRIBE 63', queryTemplate):
                query = queryTemplate % {"patientId": "4"}
            else:      
                query = queryTemplate % {"patientId": patientId}
            queryURL = self.fmqlEP + "?" + urllib.urlencode({"fmql": query}) 
            reply = json.loads(urllib2.urlopen(queryURL).read())
            dr = DescribeReply(reply)
            if dr.stopped():
                

    for dr in cacheIterator(patientId, describeReplyEnhancer=describeReplyEnhancer):
        
        mu = "\n=============================================\n"
        mu += "Type: " + dr.fileType[1] + " (" + dr.fileType[0] + ")\n"
        mu += "From FMQL: " + dr.query() + "\n"
        mu += "Number of records: " + str(len(dr.records())) + "\n"
        recordsPerType.add((dr.fileType, len(dr.records())))
        for i, record in enumerate(dr.records(), 1):
            mu += reportRecord(record)  
            noRecords += len(record.contains()) + 1
            noTopRecords += 1
            totalAssertions += record.numberAssertions() 
            recordDates = record.dates()
            for year in set(dt.year for dt in recordDates):
                recordsPerYear[year] += 1
            if record.isComplete:
                noIncomplete += 1      
        if record.fileType == "2" and not patientMU:
            patientMU = mu
        else:
            reportMU += mu

    recordsPerTypeMU = "\nRecords per type\n"
    for i, (typ, no) in enumerate(sorted(list(recordsPerType), key=lambda x: x[1], reverse=True), 1):
        recordsPerTypeMU += "\t" + str(i) + ". " + typ[1] + " (" + typ[0] + "): " + str(no) + "\n"

    recordsPerYearMU = "\nRecords per year\n"
    years = sorted(recordsPerYear.keys())
    for i, year in enumerate(sorted(recordsPerYear.keys()), 1):
        recordsPerYearMU += "\t" + str(i) + ". " + str(year) + ": " + str(recordsPerYear[year]) + "\n"
    recordsPerYearMU += "\n"

    totalsMU = "\nTotal number top records: " + str(noTopRecords) + "\n" 
    totalsMU = "\nTotal all records (includes contained): " + str(noRecords) + "\n"       
    totalsMU += "No Incomplete/stopped: " + str(noIncomplete) + "\n"            
    totalsMU += "Total assertions: " + str(totalAssertions) + "\n"

    reportMU = "==== PATIENT " + patientId + " =====\n" + totalsMU + recordsPerTypeMU + recordsPerYearMU + patientMU + reportMU
    open(RDFDIR + "patient" + patientId + "Report.txt", "w").write(reportMU)
            
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
