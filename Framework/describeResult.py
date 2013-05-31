#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

import datetime
import re
from collections import defaultdict
from describeTypeResult import FieldInfo

"""
Quick TODO:
- internally supports RDF serialization (two flavors of coded - may hide completely)
- support HTML too by fields (so can walk twice seeing what's there - or have on top)

Accessors and utilities for processing an FMQL DESCRIBE Reply. 
- Distinguishes Record, Reference, CodedValue, Date, StringOrNumeric
- Easy to see all the contents of a reply:
  - the tree of records it contains
  - is it complete (not CSTOPs on contained data)
  - all its references including whether they represent local representations of common resources (sameas)
  - all of its date values in order
- Supports the partial transformation of FileMan data arrangements (boolean coded values, list multiples) where these are indicated in the FileMan schema.

TODO:
- typLabel = lsearch.group(1) if lsearch else "CHBNODE" # fix for 9999 synth ... special handling for VistA 63.04 ... fix for that
- impl: consider custom dict (derived class of dict)
- IEN ordering for multiples
- listMultiple: will go away for data once FMQL returns a new type, list (vs cnodes), for lists of values ie/ no checking necessary. Becomes just a Schema issue.
- split assertion object types down for better reports [BOOLEAN, POINTER from FieldInfo]
- when FMQL fixed:
  - should be xsd:dateTime
  - nix fileInfoRepository once singletons and enums are labeled in data.
  - note: ambiguous field naming will be handled seamlessly
- __setitem__ for adding new properties ex/ fms:patient for new direct references to a patient's description. These as added as a prelude to generating RDF.
- see jsona
"""

class DescribeReply(object):
    """
    A describe reply has one or more results.
    """
    def __init__(self, reply, fileInfoRepository=None):
        if "results" not in reply:
            raise Exception("No results in reply")
        self.__reply = reply
        self.__fileInfoRepository = fileInfoRepository
        
    def __iter__(self):
        for record in self.records():
            yield record
                     
    @property
    def fileType(self):
        # TODO: change Record to return type in an id, label tuple
        if "TYPE" in self.__reply["fmql"]:
            return (self.__reply["fmql"]["TYPE"], self.__reply["fmql"]["TYPELABEL"])
        # Single DESCRIBE
        singletonURI = self.__reply["results"][0]["uri"] 
        return (singletonURI["value"].split("-")[0], singletonURI["label"].split("/")[0])
        
    def fileTypes(self):
        """
        All file types (1 main and subordinates) seen in this reply
        """
        fileTypes = set()
        for record in self.records():
            fileTypes |= record.fileTypes()
        return fileTypes
        
    def outsideReferences(self):
        """
        Outside references across all the records (and their subordinates) described in this reply
        """
        references = set()
        recordsAsRefs = set()
        for record in self.records():
            recordsAsRefs.add(record.asReference())
            references |= record.references()
        references -= recordsAsRefs
        return references
                
    def records(self):
        """
        Basis of and peer of __iter__
        
        Note that as FMQL returns an array of Results in IEN order, this will also be in IEN (often temporal) order.
        
        TODO: for DESCRIBE IN, will want the container record noted (and the index).
        """
        return [Record(result, fileInfoRepository=self.__fileInfoRepository) for result in self.__reply["results"]]  
        
    def fieldInfos(self):
        """
        Used by generators of tabular reports which needs to know the number of distinct, non container fields
        
        Note: unlike records FieldInfos, this doesn't contain cnodes (TODO: consider changing Record's)
        
        Another variation: for all fields, see which ones are mandatory ie/ 
            for record in records ... allFields 
        """
        fieldInfos = set()
        for record in self:
            fieldInfos |= set((fieldInfo for fieldInfo in record.fieldInfos() if fieldInfo[1] != "cnode"))
        return sorted(list(fieldInfos), key=lambda x: float(x[2]))
                
    def query(self):
        if "URI" in self.__reply["fmql"]:
            query = "DESCRIBE %(URI)s" % self.__reply["fmql"]
        else:
            query = "DESCRIBE %(TYPE)s" % self.__reply["fmql"]
            if "FILTER" in self.__reply["fmql"]:
                query += " FILTER(%(FILTER)s)" % self.__reply["fmql"]
            if self.__reply["fmql"]["LIMIT"] != "-1":
                query += " LIMIT %(LIMIT)s OFFSET %(OFFSET)s" % self.__reply["fmql"]
        if "CSTOP" in self.__reply["fmql"]:
            query += " CSTOP %(CSTOP)s" % self.__reply["fmql"]            
        return query

class Record(object):
    """    
    Files are made of records. Some files contain others (multiples), so with FMQL, any record may be the top of a tree of contained ("in context") records. Most of these contained records are just qualifiers (logs of who changed a record for example) but some are first class items arranged in a hierarchal as opposed to a graph pattern (lab or unit dose).
    
    Simplest traversal (for top level fields)
        for field, value in record:
            print field, value
            
    and for contained records
        for crecord in record.contains():
            print crecord
    
    TODO: fileInfoRepository is a temporary add on that augments the meta returned in FMQL replies with schema information they don't yet embed. Need to pass in whole repository as a single Record may embed many types (multiples).
    """
    def __init__(self, result, container=None, index=-1, fileInfoRepository=None):
        self.__result = result
        if container and index == -1:
            raise Exception("If a contained record then need both container and index")
        self.__container = container # another record or none
        self.__index = index # position of contained record in order
        self.__fileInfoRepository = fileInfoRepository # only used for coded values and multiple questions
        
    def __iter__(self):
        for fieldInfo in self.fieldInfos():
            if fieldInfo[1] == "cnodes":
                continue
            yield fieldInfo[0], self[fieldInfo[0]]
        
    def __getitem__(self, field):
        """Safe return of simple value - if field is not there returns empty string"""
        # TODO: consider removing support for getting contained nodes by field name
        if field not in self.__result:
            return ""
        if self.__result[field]["type"] == "cnodes":
            return [Record(cnode, self, i) for i, cnode in enumerate(self.__result[field]["value"], 1, self.__fileInfoRepository)]
        # TODO: unicode all the way through
        # For simple, non date values need to account for non ASCII where FMQL (TODO) seems to encode wrong on Cache. ex/ \xabH1N1 Vaccine\xbb for 44_2-9921254/reason_for_appointment
        self.__result[field]["value"] = self.__result[field]["value"].encode("ascii", "ignore")        
        if self.__result[field]["fmType"] == "3":
            fieldId = self.fieldInfo(field)[2]
            fieldInfo = self.__fileInfoRepository.fileInfo(self.fileType).fieldInfo(fieldId) if self.__fileInfoRepository else None
            # TODO: fieldInfo from Record itself should be enough ie/ enough meta to do everything OR format of coded answer (boolean or enum form) should be in the response
            return CodedValue(self.__result[field], self.fileType, fieldInfo)
        if self.__result[field]["fmType"] == "1":
            return DateValue(self.__result[field])
        if self.__result[field]["fmType"] in ["2", "4"]:
            return StringOrNumericValue(self.__result[field])
        if self.__result[field]["type"] == "uri":
            return Reference(self.__result[field])
        return Literal(self.__result[field])
        
    def __contains__(self, field):
        return True if field in self.__result else False
                
    def __str__(self):
        indent = ""
        for i in range(1, self.level):
            indent += "\t\t"
        mu = indent + self.fileTypeLabel + " (" + self.id + ")" + (" -- Level: " + str(self.level) if indent != "" else "") + "\n"
        indent += "\t"
        if self.container:
            mu += indent + "fms:index: " + str(self.index) + "\n"
        for fieldInfo in self.fieldInfos():
            if fieldInfo[1] == "cnodes":
                mu += indent + fieldInfo[0] + "\n"
                if "stopped" in self.__result[fieldInfo[0]]:
                    mu += indent + "\t" + "** STOPPED **\n"
                    continue
                for crecord in [Record(cnode, self, i, self.__fileInfoRepository) for i, cnode in enumerate(self.__result[fieldInfo[0]]["value"], 1)]:
                    mu += str(crecord)
                continue
            mu += indent + "%s: %s\n" % (fieldInfo[0], self[fieldInfo[0]])
        return mu
        
    @property
    def id(self):
        """
        Note that 'id' makes more sense than uri as not fully qualified. Client needs to know what FileMan system it is getting data from.
        
        TODO: id with .: 8052-3051024.0452 for http://datasets.caregraf.org/chcss/8052 [annotate change] ie/ replace . with _? (no as won't work for current multiples)
        """
        return self.__result["uri"]["value"]
        
    @property
    def label(self):
        return self.__result["uri"]["label"].split("/")[1]
        
    @property
    def fileType(self):
        return self.__result["uri"]["value"].split("-")[0]
                        
    @property
    def fileTypeLabel(self):
        return self.__result["uri"]["label"].split("/")[0]
                        
    @property
    def sameAs(self):
        return self.__result["uri"]["sameAs"] if "sameAs" in self.__result["uri"] else ""
        
    @property
    def sameAsLabel(self):
        return self.__result["sameAsLabel"] if "sameAsLabel" in self.__result else ""
        
    def asReference(self):
        return Reference(self.__result["uri"])
        
    def fileTypes(self):
        fileTypes = set()
        fileTypes.add((self.fileType, self.fileTypeLabel))
        for field, value in self.__result.iteritems():
            if field == "uri":
                continue
            if not (value["type"] == "cnodes" and "stopped" not in value):
                continue
            for i, cnode in enumerate(value["value"], 1):
                cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                fileTypes |= cRecord.fileTypes()
                break
        return fileTypes        
                
    def fields(self):
        return [field for field, fmId in sorted([(field, self.__result[field]["fmId"]) for field in self.__result if field != "uri"], key=lambda x: float(x[1]))]
                
    def fieldInfos(self):
        """
        Schema from the reply: better than generic "keys()"
        """
        return sorted([(field, self.__result[field]["type"], self.__result[field]["fmId"], FieldInfo.FIELDTYPES[self.__result[field]["fmType"]] if "fmType" in self.__result[field] else "") for field in self.__result if field != "uri"], key=lambda x: float(x[2]))  
        
    def fieldInfo(self, field):
        for fieldInfo in self.fieldInfos():
            if fieldInfo[0] == field:
                return fieldInfo
        return None
        
    @property
    def container(self):
        """
        If one then a multiple ie/ equivalent to isMultiple()
        """
        return self.__container # may be None
        
    @property
    def index(self):
        return self.__index # may be -1 (goes with container)
        
    @property
    def level(self): # how many levels DOWN is a record. Top is at 1
        # only relevant if contained ie/ a multiple
        level = 1
        container = self.__container
        while container:
            level += 1
            container = container.container
        return level
        
    def contains(self, fileType=""):
        """
        Note that this will build a tree which you can walk ie.
        [x, y, z]
            [a, b]
                [h, i] etc.
                
        Note: containment depth = max level of contained record ...
                
        One application of this is to decide if a multiple is "TOO POPULAR" to just be a contained or "in context" record. An example is 63.04 in VistA FileMan, a lab record that should be a first class file that points to PATIENT (2).
        """
        contains = []
        for field, value in self.__result.iteritems():
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for i, cnode in enumerate(value["value"], 1):
                        cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                        if fileType and cRecord.fileType != fileType:
                            continue
                        contains.append(cRecord)
        return contains
        
    @property
    def isComplete(self):
        # is this record or any of its contained records incomplete (ie/ cstopped)
        for field, value in self.__result.iteritems():
            if value["type"] == "cnodes":
                if "stopped" in value:
                    return False
                for i, cnode in enumerate(value["value"], 1):
                    cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                    if not cRecord.isComplete:
                        return False
        return True
                
    def references(self, fileTypes=None, sameAsOnly=False):
        """
        Includes references from contained records but DOES NOT include coded values (though it may going forward)
        
        Note: a record (or sub records) may include a reference to itself.
        """
        references = set()
        for field, value in self.__result.iteritems():
            if field == "uri":
                continue
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for i, cnode in enumerate(value["value"], 1):
                        cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                        references |= cRecord.references(fileTypes, sameAsOnly)
                continue
            if value["type"] != "uri":
                continue
            reference = Reference(value)
            if fileTypes and reference.fileType not in fileTypes:
                continue
            if sameAsOnly and reference.sameAs == "":
                continue
            references.add(reference)
        return references
        
    def dates(self):
        """
        All dates in this record - including contained dates, in order. Duplicates removed. To get first: dates()[0], dates()[-1] is latest.
        """
        dates = []
        for field, value in self.__result.iteritems(): # REPLACE with iter()
            if field == "uri":
                continue
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for i, cnode in enumerate(value["value"], 1):
                        cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                        dates.extend(cRecord.dates())
                continue
            # TODO - bug to fix - should be xsd:dateTime
            if not (value["type"] == "typed-literal" and value["datatype"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#dateTime"):
                continue
            dtValue = DateValue(value)
            if not dtValue.isValid:
                continue # bad date
            dates.append(dtValue.dateTimeValue)
        return sorted(list(set(dates))) # remove dups, low to high
              
    def numerics(self):
        """
        Return any string or numeric field as a number, if that is possible. Used to:
        - QA schema definition of number: some contain strings
        - isolate numeric ids in string or number fields. Many are IENs
        
        Ex/ DIAGNOSIS in 100417. It is billed as FREE TEXT but is a code (note: normally a float) ex/ (u'diagnosis', 'FREE TEXT', 724.2)
        """
        numericValues = []
        for fieldInfo in self.fieldInfos():
            if fieldInfo[3] in ["FREE TEXT", "NUMERIC"]: # NUMERIC or STRING
                snValue = self[fieldInfo[0]]
                if snValue.isNumeric:
                    numericValues.append((fieldInfo[0], fieldInfo[3], snValue.value))
        return numericValues
              
    def numberAssertions(self, assertionObjectType=""):
        """
        How many triples/statements does this "tree" of records represent ie/ counts contained record references too
        
        TODO: may split by Coded, Boolean etc
        """
        no = 0
        for field, value in self.__result.iteritems():
            if field == "uri":
                continue
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for i, cnode in enumerate(value["value"], 1):
                        cRecord = Record(cnode, self, i, self.__fileInfoRepository)
                        no += cRecord.numberAssertions(assertionObjectType)
                continue
            no += 1
        return no
        
class FieldValue(object):
    
    def __init__(self, result):
        self._result = result
        self._type = ""
        
    def __str__(self):
        return self._result["value"]
        
    @property
    def fmType(self):
        return FieldInfo.FIELDTYPES[self._result["fmType"]]
        
    @property
    def type(self):
        return self._type
        
    @property
    def value(self):
        return self._result["value"]
        
class Literal(FieldValue):

    # TODO: consider merging all literal variations into this one class

    def __init__(self, result):
        FieldValue.__init__(self, result)
        if result["type"] not in ["literal", "typed-literal"]:
            raise Exception("Must create Literals with Literals")
        self._type = "LITERAL"
        self._datatype = "" if "datatype" not in result else result["datatype"]
        
    @property
    def datatype(self):
        return self._datatype

class Reference(FieldValue):
    
    def __init__(self, result):
        FieldValue.__init__(self, result)
        if result["type"] != "uri":
            raise Exception("Must create references with URIs")
        self._type = "URI"
        
    def __str__(self):
        """
        Some labels are strange as .01 is not a display thing: ex/ NAME COMPONENTS/2 vs NEW PERSON/MANAGER,SYSTEM ... TODO: FMQL
        """
        mu = self.label + " ({system}:" + self.id + ")"
        mu += " <=> " + self.sameAsLabel + " (" + self.sameAs + ")" if self.sameAs else ""
        return mu
        
    def __hash__(self):
        return self.id.__hash__()
        
    def __eq__(self, other):
        return self.id == other.id
        
    def __cmp__(self, other):
        if other.fmFileType != self.fmFileType:
            if self.fmFileType < other.fmFileType:
                return -1
            return 1
        if self.ien < other.ien:
            return -1
        elif self.ien == other.ien:
            return 0
        return 1
        
    def asReference(self):
        return self # to be compatible with CodedValue - TODO

    @property
    def id(self): # copy of .value
        return self._result["value"]
        
    @property
    def ien(self):
        """
        # Form: 9999999999_6304-1_1_4 ie/ 1st under 1st under 4th. For now, only doing last => order will only work in the context records from one walk.
        # TODO: expand multiple id comparison
        # TODO: what about built-in's/coded values
        """
        ien = self.id.split("-")[1]
        if re.search(r'\_', ien):
            ien = re.split(r'\_', ien)[0]
        return float(ien)
                
    @property
    def label(self):
        return self._result["label"].split("/")[1]
        
    @property
    def fileType(self):
        return self._result["value"].split("-")[0]

    @property
    def fmFileType(self):
        if re.search(r'_E$', self.fileType):
            return 0
        return float(re.sub(r'\_', '.', self.fileType)) 

    @property
    def fileTypeLabel(self):
        return self._result["label"].split("/")[0]
        
    @property
    def sameAs(self):
        """
        Unlike 'internal'/local ids, sameAs is a full qualified URI
        """
        return self._result["sameAs"] if "sameAs" in self._result else ""
        
    @property
    def sameAsLabel(self):
        return self._result["sameAsLabel"] if "sameAsLabel" in self._result else ""
        
    @property
    def builtIn(self):
        return True if re.search(r'_E$', self.fileType) else False
        
class CodedValue(Literal):
    """
    TODO: 
    - FMQL - must add more information to a coded value - specifically what is the index of the value and MN too.
    - Change instantiation - just have CODED References and CODED Literals
    """
    def __init__(self, result, fileType, fieldInfo=None):
        Literal.__init__(self, result)
        if result["fmType"] != "3":
            raise Exception("Must create CodedValues with CodedValues!")
        self.__fieldInfo = fieldInfo
        if self.isBoolean:
            self._datatype = "xsd:boolean" 
        else:
            self._type = "URI"
            self.__fileType = fileType # needed to make ID
        
    def __str__(self):
        mu = str(self.value)
        mu += " (" + self._result["value"] + ") [BOOLEAN]" if self.isBoolean else " [CODED VALUE]"
        return mu

    @property
    def value(self):
        if self.isBoolean: # TODO: what if value is NOT in schema range?
            return self.__fieldInfo.booleanOfValue(self._result["value"])      
        return self._result["value"]
    
    @property
    def fmValue(self):
        return self._result["value"]
        
    @property
    def isBoolean(self):
        """
        Temporary need to pass in fieldInfo. 
        """
        if not self.__fieldInfo:
            return False
        return self.__fieldInfo.isBooleanCoded
        
    def asReference(self):
        if self.isBoolean:
            raise Exception("Not a Reference")
        fileType = self.__fileType + "_" + re.sub(r'\.', '_', self._result["fmId"]) + "_E"
        uriValue = fileType + "-" + re.sub(r'[^\w]', '_', self._result["value"])
        return Reference({"value": uriValue, "type": "uri", "fmId": self._result["fmId"]})
                
class DateValue(Literal):

    def __init__(self, result):
        Literal.__init__(self, result)
        if result["fmType"] != "1":
            raise Exception("Must create Date with Dates!")
        if self.isValid:
            self._datatype = "xsd:dateTime"

    def __str__(self):
        mu = self._result["value"]
        mu += " [DATE]" if self.isValid else " [INVALID DATE]"
        return mu
        
    @property
    def isValid(self):
        try:
            self.dateTimeValue
        except Exception as e:
            return False
        else:
            return True

    @property
    def dateTimeValue(self):
        # return datetime.datetime if possible - otherwise string
        # Not done as no natural Python class (TODO: do own class with just month/year)
        # noDayMatch = re.match(r'(\d{4}\-\d{2})\-00', dateValue)
        # Also no date.strptime so "need" :00... etc for hours and minutes
        # Month not in 1-12; alpha num values (ex/ ); bad minutes (2006-05-15T08:60:00Z. Bad minutes.); or no day ie/ 2002-09-00T00:00:00Z
        return datetime.datetime.strptime(self._result["value"], '%Y-%m-%dT%H:%M:%SZ')
        
class StringOrNumericValue(Literal):
    """
    FileMan schema's tend to be "free and easy" with the type "NUMERIC" which sometimes contains a string value and sometimes with "FREE TEXT" which sometimes contains a numeric value. In both cases, the value may be an identifier - something not distinguished in the schema. This class helps deal with the ambiguity.
    """
    def __init__(self, result):
        Literal.__init__(self, result)
        if result["fmType"] not in ["2", "4"]:
            raise Exception("Can only handle FREE TEXT and NUMERIC")
        
    def __str__(self):
        mu = self._result["value"]
        mu += " [NUMERIC in STRING]" if self._result["fmType"] == "4" and self.isNumeric else ""
        mu += " [STRING in NUMERIC]" if self._result["fmType"] == "2" and not self.isNumeric else ""
        return mu
            
    @property
    def value(self):
        try:
            floatVal = float(self._result["value"])
        except:
            return self._result["value"]
        else:
            intVal = int(floatVal)
            val = int(floatVal) if int(floatVal) == floatVal else floatVal
            return val            
             
    @property
    def isNumeric(self):
        try:
            floatVal = float(self._result["value"])
        except:
            return False
        else:
            return True
        
# ############################# Test Driver ####################################

import sys
import urllib
import urllib2
import json

# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"     

def main():

    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "DESCRIBE 2-9"}) 
    reply = json.loads(urllib2.urlopen(queryURL).read())
    dr = DescribeReply(reply)
    for record in dr.records():
        print "Basics 2-9:"
        print record.id, record.label, record.fileType, record.fileTypeLabel
        if not record.isComplete:
            print
            print "*** Not complete - stopped somewhere"
            print
        print 
        print "Number Assertions:", record.numberAssertions()
        print
        print "Contents:"
        print record
        print 
        print "Its references:"
        for i, reference in enumerate(record.references(), 1):
            print i, reference
        print
        print "Its numerics:"
        for i, numeric in enumerate(record.numerics(), 1):
            print i, numeric
        print
        print "Its dates:"
        for i, dt in enumerate(record.dates(), 1):
            print i, dt
        print 
        print "Its 'common' (sameAs'ed) references:"
        for i, reference in enumerate(record.references(sameAsOnly=True), 1):
            print reference
        print 
        print "Its people references (200):"
        for i, reference in enumerate(record.references(fileTypes=["200"]), 1):
            print "\t", i, reference
        print 
        print "Its fields (meta):"
        for i, fieldInfo in enumerate(record.fieldInfos(), 1):
            print "\t", i, fieldInfo
        print
        print "It contains:"
        for i, cRecord in enumerate(record.contains(), 1):
            print i, cRecord

if __name__ == "__main__":
    main()
