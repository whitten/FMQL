#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2015 caregraf
#

import os
import codecs
import re
import json
from collections import defaultdict
from datetime import datetime
from operator import itemgetter
import sys
import os
import commands
import StringIO
from collections import OrderedDict

from fmaf.describeResult import DescribeReply
from fmaf.fileManInfo import FileManInfo

"""
QUICK FIXES:
- support new CodedFieldValue (vs treating as reference)
  MOVE FROM HACK BELOW
- add and then remove override loading (will hide schema-enabled overrides 
behind Record/Field entities ie/ reflecting that eventually the remote side
will do the right work)
  MOVE FROM HACK BELOW
- need to revisit the mongo implications of the @list qualifier. Perhaps
make optional so a Mongo friendly form won't have it (or a context).
- clean notes below (mongo etc references)
"""

"""
FileMan JSON-LD Format:
- nested, not flattened form so all top level resources are of the same type
  - note that an RDF serialization (ttl etc) will produce a flattened representation
- expanded form for all subject references: inline this distinguishes references from simple (literal) values. It also provides a place for a referenced entity's label as well as equivalence assertions for meta data
- compact form for booleans and numbers as @value will preserve their typing all the way through to RDF
- _id Mongo supported as option
- expanded form for dateTimes as not intrinsically handled (may revisit)
- base context 
  - aliases all "@" properties, removing the MongoDB disallowed "@"
  - as FileMan has one large namespace, uses the relevant FileMan model (vs or chcss) as the default namespace for keys 
- rdfs:label is used as the name key for every type of entity
Supports easy Mongo insertion:
- loop @graph elements changing "id" to "_id"
- only pass contents of @graph to Mongo insert
Big TODO: neater JSON but proper RDF too 
- remove context from here: reference back (need utility)
  i/e context2.json ... note: must add in definition of system
- context per type doing name normalization
- consider passing context ref in HTTP header
- context hierarchy possible?
... from FMQL utils, need to be able to get appropriate name when serializing ie/ simplest per type name possible that could expand in context
... could define type of datetypes out of band based on this
- System @base ... inline system id (dmis0056:...) which will expand in context
  - goal: COMPACT with "right" context has no effect on raw form
  - will rely on new MUMPS util to return system id (see VistA and CHCS)
  - for now, set up in Python
TODO/CONSIDER:
- dropped inline "list" qualifier for more natural JSON. May need to add back - FileMan multiples need more consideration
- revisit casting ints to int: do more formally (FMQL 1.2) [rolled back this time - int will show in RDF]
- nixed graph id (was from query) as now have query parameters. May revisit.
- BSON option for dateTimes
- type as {"id": ... "label": type label} ?
- removing ids from multiples/blank nodes. Reduce all but the larger examples to simple blank nodes 
- one BNode in multiple: consider not having a list. Means must check as processing
- removing meaningless numeric or date labels: <http://livevista.caregraf.info/20-96> rdfs:label "2" .
- consider using "vs" explicitly for field types to make merging with other data easier?
Bigger TODO/Context:
- reframe with a context that leverages unique or normalized fields ie/ zip etc normalized to vsn:zip ... ditto for city etc.
"""                
class DescribeReplyToJLD:
    """
    """
    def __init__(self, fms="vs", systemBase="http://livevista.caregraf.info/", useMongoResourceId=False, nameMeta={}, multipleMeta={}):
        """
        Ex/ systemBase = http://www.examplehospital.com/vista/, fms="vs" etc.
        """
        self.base = systemBase
        self.fms = fms
        self.useMongoResourceId = useMongoResourceId
        self.TYPEFMSNS = self.fms + ":"
        # NOTE: built JSON in memory - decided not to do StringIO.StringIO
        self.__jsonLD = OrderedDict()
        thisdir = os.path.dirname(os.path.abspath(__file__)) + "/"
        self.__jsonLD["@context"] = json.load(open(thisdir + self.fms + "fmcontextBase.json"), object_pairs_hook=OrderedDict)
        self.__jsonLD["@context"]["@base"] = systemBase
        self.__jsonLD["@graph"] = []
        self.__sameAsNSSeen = set()
        
        self.__nameMeta = nameMeta
        self.__multipleMeta = multipleMeta

    FMSNS = "" # self.fms + ":"
    TYPEFMSNS = "" # override ... self.fms + ":" # do qualify types
    KGRAFNS = "http://schemes.caregraf.info/"
    
    MONGOATID = "_id" # if want to force MongoIds    
    ATID = "id"
    ATTYPE = "type"
    RDFSLABEL = "label"
    ATLIST = "list"
    ATVALUE = "value"
    SAMEAS = "sameAs" # owl:sameAs
    SAMEASLABEL = "sameAsLabel" # fmso:sameAsLabel
    
    def processReply(self, describeReply):
        # not id'ing by query so no id ... may reconsider later
        # self.__jsonLD[self.ATID] = describeReply.queryAsId()
        for record in describeReply:
            self.__processRecord(record)
        for sameAsNS in self.__sameAsNSSeen:
            if sameAsNS not in self.__jsonLD["@context"]:
                self.__jsonLD["@context"][sameAsNS] = self.KGRAFNS + sameAsNS + "/"
        self.__jsonLD["generatedAt"] = datetime.now().strftime("%Y-%m-%d") 
        self.__jsonLD["query"] = OrderedDict()
        queryParams = describeReply.queryParams() 
        for queryParam in queryParams:
            self.__jsonLD["query"]["fmql:" + queryParam.lower()] = queryParams[queryParam]
            
    def __processRecord(self, record, container=None):  
                
        resource = OrderedDict([(self.MONGOATID if container == None and self.useMongoResourceId else self.ATID, record.id)])
        
        # Note - .title() turns PATIENT to Patient. Same is applied in .label
        # of fileInfo but iffy as each doing it independently. TODO: FMQL should
        # return what's needed natively.
        resource[self.ATTYPE] = self.TYPEFMSNS + FileManInfo.normalizeLabel(record.fileTypeLabel.title(), False) + "-" + record.fileType
                        
        if container is not None:
            container.append(resource)
        else:
            self.__jsonLD["@graph"].append(resource)
            
        """
        No label for Blank Nodes (never pointed to!). Nixes 'silly' date labels
        which are rare for top/first class records.
        """
        if not record.container:                                                               
            resource[self.RDFSLABEL] = record.label
                    
        for field, fieldValue in record:
        
            qpred = self.__qpred(field, record)
            
            fv = self.__processFieldValue(qpred, fieldValue)

            if fv: # None if ref is invalid
                resource[qpred] = fv 
                                                        
        for cfield in record.cfields:
            completeCRecords = [crecord for crecord in record[cfield] if crecord.isComplete]
            if not len(completeCRecords):
                continue
            qpred = self.__qpred(cfield, record)
            """
            Removing "list" qualifier. May reconsider 
                list = []
                resource[qpred] = list
                # http://json-ld.org/spec/latest/json-ld/#sets-and-lists
                self.__jsonLD["@context"][qpred] = {"@container": "@list"}
                # Note: this is overloading what JSON-LD really says. Every key expands to a list by default. 
                if record.isSimpleList(cfield):
                    self.__jsonLD["@context"][qpred]["@type"] = "@id"
            """
            # resource[qpred] = {self.ATLIST: []}
            # list = resource[qpred][self.ATLIST]
            list = []
            resource[qpred] = list
            for crecord in completeCRecords:
                # TODO: will go off multiple list (or no need as JSON will follow
                # this going forward ie/ will inline SINGLE VALUE MULTIPLES
                # "list" included in cnode's value if simple - @id tells me to look for the reference. Note that in inline version will just inline label and id of referenced item ie/ no sharing.
                if record.isSimpleList(cfield):
                    # get one and only field name - usually matches cfield itself but
                    # not always ex/ CHCS 100251_1 has "lab_tests" with "lab_test"
                    ccfield = crecord.fields()[0]
                    cfv = self.__processFieldValue(qpred, crecord[ccfield])
                    if cfv: 
                        list.append(cfv)
                    continue
                self.__processRecord(crecord, list)
                
    # rely on DescribeResult following new field normalization rules (sync with schema)
    # that will eventually push back into MUMPS
    def __qpred(self, field, record):
        
        """
        TODO TMP: Hacky here - move back into FieldValue/Record but must also 
        move the globalName back there too which will be qualified in place. 
        Then in here can add FMSNS
        
        Here we override the fully qualified name.
        """
        fieldId = record.fieldInfo(field)[2]
        if record.fileType in self.__nameMeta and fieldId in self.__nameMeta[record.fileType]:
            return self.__nameMeta[record.fileType][fieldId]["NAME"]
        
        qpred = self.FMSNS + field.lower() + "-" + record.fileType
            
        # Issue: NCName	::= (Letter | '_') (NCNameChar)* in http://www.w3.org/TR/1999/REC-xml-names-19990114/#NT-NCName BUT JSON allows a number etc first. We insert a _ if there is a number first. TBD: best done in FMQL to be consistent.
        if re.match(r'\d', qpred):
            qpred = "_" + qpred            

        return qpred
                
    # TODO: make neat - ie/ split three variations properly
    def __processFieldValue(self, field, fieldValue):
                        
        if fieldValue.type == "URI":
                
            fieldValue = fieldValue.asReference() # awkward: Coded -> Reference needs to be smoother TODO
            
            # TODO: should have been caught in FMQL ie/ invalid IENs
            if not fieldValue.valid:
                return None
            
            """
            Let's turn ENUMS into plan strings
            
            TODO: make ENUM a first class thing (not a reference) in FieldValue
            
            Note: current CodedValue says it is a "URI"! need to change that
            make. Can still do asReference but it would have to change properly.
            """
            if fieldValue.fmFileType == 0: # means CODED!
                value = fieldValue.label
            else:
            
                # REM: @base sets up default base
                uriPrefix = self.TYPEFMSNS if fieldValue.builtIn else ""
            
                value = OrderedDict([(self.ATID, uriPrefix + fieldValue.value), (self.RDFSLABEL, fieldValue.label)])
                                
                if fieldValue.sameAs:
                # Special for ICD9CM ... dot and space for _ (TODO: look into codes with spaces)
                    sameAsId = fieldValue.sameAs.lower() 
                    if re.match(r'icd9cm', sameAsId):
                        sameAsId = re.sub(r'[\. ]', '_', sameAsId)
                    value[self.SAMEAS] = sameAsId
                    if fieldValue.sameAsLabel: # as optional
                        value[self.SAMEASLABEL] = fieldValue.sameAsLabel
                    self.__sameAsNSSeen.add(sameAsId.split(":")[0])
                                                                            
        elif fieldValue.datatype == "xsd:boolean":
             value = bool(fieldValue.value) # compact form
        elif fieldValue.datatype == "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral": # will come out in FMQL V1.2
            value = str(fieldValue.value)
        # Go to plain xsd:date if no time and nix Z
        # Not doing gYear etc so only typing as a date or dateTime if proper form
        elif fieldValue.datatype == "xsd:dateTime":            
            fvalue = str(fieldValue.value)
            dataType = ""
            if re.search(r'T00:00:00Z$', fvalue):
                dateValue = fvalue.split("T")[0]
                try:
                    datetime.strptime(dateValue, "%Y-%m-%d")  
                except:
                    pass
                else:
                    dataType = "xsd:date"                    
            else:
                dateValue = fvalue[:-1] # drop Z
                try: # 24 hour clock - double back if need be
                    datetime.strptime(dateValue, "%Y-%m-%dT%H:%M:%S")  
                except:
                    pass
                else:
                    dataType = "xsd:dateTime"
            if not dataType:
                value = dateValue
            else:
                value = OrderedDict([(self.ATVALUE, dateValue), (self.ATTYPE, dataType)])
        elif fieldValue.datatype:
            value = OrderedDict([(self.ATVALUE, str(fieldValue.value)), (self.ATTYPE,  fieldValue.datatype)])
        else: 
            """
            try:
                value = int(str(fieldValue.value))
            except:
            """
            value = str(fieldValue.value)

        return value
        
    def json(self):
        print self.__jsonLD.keys()
        return self.__jsonLD
                    
    def flushJSON(self, where):
        return json.dump(self.__jsonLD, open(where, "w"), indent=2)
                
# ############################# Test Driver ####################################

import sys
import urllib
import urllib2
try:    
    from jldToRDF import jldToRDF
except:
    pass
    
# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"   

def test(query):
    try:
        os.makedirs("TESTTTLS")
    except:
        pass
    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": query}) 
    reply = json.loads(urllib2.urlopen(queryURL).read())
    dr = DescribeReply(reply)
    drToJLD = DescribeReplyToJLD(fms="vs", systemBase="http://livevista.caregraf.info/")
    drToJLD.processReply(dr)
    jld = drToJLD.json()
    print json.dumps(jld, indent=2)
    # get file name from type and either offset or afterien or absolute
    if "fmql:uri" in jld["query"]:
        fileName = jld["query"]["fmql:uri"]
    else:
        fileName = jld["query"]["fmql:type"] + "--" + (jld["query"]["fmql:afterien"] if "fmql:afterien" in jld["query"] else jld["query"]["fmql:offset"])
    jldToRDF(drToJLD.json(), fileName, "TESTTTLS/")

from datetime import datetime

def main():
       
    print "LDing around patient 9"
    
    test("DESCRIBE 2-9")
    test("DESCRIBE 120_5 FILTER(.02=2-9&.01>2008-04-01)")
    test("DESCRIBE 2 AFTERIEN 8 LIMIT 1")
    test("DESCRIBE 9000011 FILTER(.02=9000001-9)")
    
if __name__ == "__main__":
    main()
