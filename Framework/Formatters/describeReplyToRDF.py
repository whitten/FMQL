#!/usr/bin/env python

from rdfBuilder import RDFBuilder
import codecs
import re
# 2.7? Needed? As in C. cjson is faster still: http://j2labs.tumblr.com/post/4262756632/speed-tests-for-json-and-cpickle-in-python
try:
    import simplejson as json
except ImportError:
    import json
from collections import defaultdict
from datetime import datetime
from operator import itemgetter
from describeResult import DescribeReply
from describeTypeResult import FieldInfo

"""
One of a series of formatters of Records (toHTML, toText ...). This makes RDF from  Records extracted from FMQL Describe replies.

How to use? See example code in "main" at the bottom of this file.
"""
                   
class DescribeReplyToRDF:
    """
    - Serialize records as RDF
    - support caller serializing the labels and types of referenced resources and of all types seen.
    """
    def __init__(self, rdfb, fms="vs", systemBase="http://livevista.caregraf.info/"):
        """
        Ex/ systemBase = http://www.examplehospital.com/vista/, fms="vs" etc.
        """
        self.rdfb = rdfb
        self.systemBase = systemBase
        self.fms = fms
                
    def processReply(self, describeReply):
        for record in describeReply:
            self.__processRecord(record)

    def __processRecord(self, record):  
                
        # Already there
        if not self.rdfb.startNode(self.systemBase + record.id, ""):
            return
                            
        self.rdfb.addURIAssertion("rdf:type", {"type": "uri", "value": ("http://datasets.caregraf.org/%s/" % self.fms) + record.fileType})
                               
        self.rdfb.addAssertion("rdfs:label", {"value": record.label, "type": "literal"})
                
        if record.container:
            self.rdfb.addAssertion("fms:context", {"value": self.systemBase + record.container.id, "type": "uri"})
            self.rdfb.addAssertion("fms:contextIndex", {"value": record.index, "type": "literal", "datatype": "xsd:integer"}) 
            
        for field, fieldValue in record:
        
            pred = field.lower() + "-" + record.fileType
            
            # Issue: NCName	::= (Letter | '_') (NCNameChar)* in http://www.w3.org/TR/1999/REC-xml-names-19990114/#NT-NCName BUT JSON allows a number etc first. We insert a _ if there is a number first. TBD: best done in FMQL to be consistent.
            if re.match(r'\d', pred):
                pred = "_" + pred            
        
            if fieldValue.type == "URI":
                fieldValue = fieldValue.asReference() # awkward: Coded -> Reference needs to be smoother TODO
                uriBase = "http://datasets.caregraf.org/%s/" % self.fms if fieldValue.builtIn else self.systemBase
                self.rdfb.addURIAssertion(pred, {"value": uriBase + fieldValue.value})
                # if fieldValue.sameAs:   
                #    pass
                continue
                
            self.rdfb.addLiteralAssertion(pred, {"value": str(fieldValue.value), "datatype": fieldValue.datatype} if fieldValue.datatype else {"value": str(fieldValue.value)})
            
        # close Resource Definition
        self.rdfb.endNode()
            
        # Can recurse through the hierarchy of records in records
        for crecord in record.contains():
            if not crecord.isComplete: # skip the stopped
                continue
            self.__processRecord(crecord)
            
class DescribeRepliesToSGraph:
    """
    This builds on DescribeReplyToRDF to produce a self contained (all URI's labeled) graph from one or more Describe Replies. These replies may be about a Patient or a Ward or System information.
    
    TODO: 
    - FIX TO DescribeReply: needs to count Enum values in outside references
    - add support for SAMEAS described things ie/ even though described, still sameas
    - add a Dataset/Graph header
    - SUPPORT exposure of outside refs ie/ to non same as types
    - DO DUMMY/SIMPLE rdfb
    """
    def __init__(self, fms="vs", systemBase="http://livevista.caregraf.info/", k3Base="http://schemes.caregraf.info/"):
                    
        self.fms = fms
        self.systemBase = systemBase 
        self.k3Base = k3Base              
        self.rdfb = RDFBuilder(["%s" % fms, "http://datasets.caregraf.org/%s/" % fms], "http://datasets.caregraf.org/%s/" % fms, extraNSInfos=[["fms", "http://datasets.caregraf.org/fms/"], ["xsd", "http://www.w3.org/2001/XMLSchema#"], ["dc", "http://purl.org/dc/elements/1.1/"]])
        self.fileTypes = set()
        self.outsideReferences = set()
        self.described = set()
        
    def processReply(self, describeReply):
        
        drrdf = DescribeReplyToRDF(self.rdfb, self.fms, self.systemBase)
        drrdf.processReply(describeReply)
        
        self.fileTypes |= describeReply.fileTypes()
        # Track outside references as go, removing if completely define a record
        self.outsideReferences |= describeReply.outsideReferences()
        self.described |= set(record.asReference() for record in describeReply.records())
        
    def done(self):
    
        self.outsideReferences -= self.described
            
        sameAss = set()
        sameAss2 = defaultdict(list)
        for reference in self.outsideReferences:
            if not self.rdfb.startNode(self.systemBase + reference.id, ""):
                raise Exception("Unexpected to try to reference define the fully defined: " + str(reference))
            self.rdfb.addLiteralAssertion("rdfs:label", {"value": reference.label})
            self.rdfb.addURIAssertion("rdf:type", {"value": ("http://datasets.caregraf.org/%s/" % self.fms) + reference.fileType})
            if reference.sameAs:
                eSameAs = self.__expandSameAsURI(reference.sameAs)
                self.rdfb.addURIAssertion("owl:sameAs", {"value": eSameAs})
                sameAss.add((eSameAs, reference.sameAsLabel))
                sameAss2[eSameAs].append(reference.sameAsLabel)
            self.rdfb.endNode()  
            
        # These used to be tagged with rdf:type fms:OutsideConcept
        for sameAs in sameAss:
            if not self.rdfb.startNode(sameAs[0], ""):
                continue # for now skip second definition of same sameas ex/ > 1 80 to an ICD. Should fix TODO intercept before this point
                # raise Exception("Unexpected to try to reference define the defined: " + str(sameAs))
            self.rdfb.addLiteralAssertion("rdfs:label", {"value": sameAs[1]})   
            self.rdfb.addURIAssertion("rdf:type", {"value": "http://datasets.caregraf.org/fms/CommonConcept"})         
            self.rdfb.endNode()  
            
        for fileType in self.fileTypes:
            self.rdfb.startNode(("http://datasets.caregraf.org/%s/" % self.fms) + fileType[0], "")
            self.rdfb.addLiteralAssertion("rdfs:label", {"type": "literal", "value": fileType[1]})
            self.rdfb.addURIAssertion("rdf:type", {"type": "uri", "value": "owl:Class"})
            self.rdfb.endNode()          
            
        return self.rdfb.done()
        
    # Tmp before FMQL V1
    SCHEMEMNMAP = {"ICD9": "ICD9CM", "PROVIDER": "HPTC"}
    def __expandSameAsURI(self, sameAsURI):
        # in mixed files (local only and sameas'ed terminologies), the local only entries are marked "LOCAL" in their same as fields
        if sameAsURI == "LOCAL":
            return ""
        uriMatch = re.match(r'([^:]+):(.+)$', sameAsURI)
        if not uriMatch:
            return ""
        # TODO: what if define. Should process this earlier
        if uriMatch.group(1) == "LOCAL":
            return self.systemBase + uriMatch.group(2)
        # Tmp map til FMQL v1
        schemeMN = self.SCHEMEMNMAP[uriMatch.group(1)] if uriMatch.group(1) in self.SCHEMEMNMAP else uriMatch.group(1)
        id = uriMatch.group(2)
        if schemeMN == "ICD9CM":
           id = re.sub(r'\_$', '', re.sub(r'\.', '_', id)) # code form to kGraf form, no trailing .'s
        return self.k3Base + schemeMN.lower() + "/" + id
                                    
# ############################# Test Driver ####################################

import sys
import urllib
import urllib2

# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"     

def main():
       
    print "RDFing patient 9"
    
    gdRDF = DescribeRepliesToSGraph(fms="vs", systemBase="http://livevista.caregraf.info/")
    
    # Will add to this list ...
    QUERIES = ["DESCRIBE 2-9", "DESCRIBE 52 FILTER(2=2-9)"]
    
    for query in QUERIES:
        
        queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": query}) 
        reply = json.loads(urllib2.urlopen(queryURL).read())
        dr = DescribeReply(reply)
        gdRDF.processReply(dr)
                        
    with codecs.open("vsPatientGraph9.rdf", "w", encoding="utf-8") as resultFile:
        resultFile.write(gdRDF.done().getvalue())
        resultFile.close()
        
    print "graph written out as vsPatientGraph9.rdf"
    
if __name__ == "__main__":
    main()
