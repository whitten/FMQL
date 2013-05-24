#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

from describeTypeResult import FieldInfo

"""
- Distinguishes Record, Reference, CodedValue
- Easy to see all the contents of a reply:
  - the tree of records it contains
  - is it complete (not CSTOPs on contained data)
  - all its references including whether they represent local representations of common resources (sameas)
- Supports the partial transformation of FileMan data arrangements (boolean coded values, list multiples)

TODO:
- make an __iter__ so for field in Record print field works
- multiple id references. ex/ fill number in activity log of Prescription. Fill is id'ed with a number AND other numerics ie/ help distinguish ids from numerics from ... ROUTINE that distinguishes numbers the same across a tree.
- date as typed
- see jsona
"""

class DescribeReply(object):
    """
    A describe reply has one or more results.
    """
    def __init__(self, reply):
        self.__reply = reply
        
    def records(self):
        return [Record(result) for result in self.__reply["results"]]    

class Record(object):
    """    
    Files are made of records. Some files contain others (multiples). 
    """
    def __init__(self, result, container=None):
        self.__result = result
        self.__container = container # another record or none
        
    def __getitem__(self, field):
        """Safe return of simple value - if field is not there returns empty string"""
        if field not in self.__result:
            return ""
        if self.__result[field]["type"] == "cnodes":
            return [Record(cnode, self) for cnode in self.__result[field]["value"]]
        if self.__result[field]["fmType"] == "3":
            return CodedValue(self.__result[field])
        if self.__result[field]["type"] == "uri":
            return Reference(self.__result[field])
        # For simple, non date values need to account for non ASCII where FMQL (TODO) seems to encode wrong on Cache. ex/ \xabH1N1 Vaccine\xbb for 44_2-9921254/reason_for_appointment
        return self.__result[field]["value"].encode("ascii", "ignore")
        
    def __contains__(self, field):
        return True if field in self.__result else False
                
    def __str__(self):
        indent = ""
        for i in range(1, self.level):
            indent += "\t\t"
        mu = indent + self.fileTypeLabel + " (" + self.id + ")" + (" -- Level: " + str(self.level) if indent != "" else "") + "\n"
        indent += "\t"
        for fieldInfo in self.fieldInfos():
            if fieldInfo[1] == "cnodes":
                mu += indent + fieldInfo[0] + "\n"
                if "stopped" in self.__result[fieldInfo[0]]:
                    mu += indent + "\t" + "** STOPPED **\n"
                    continue
                for record in [Record(cnode, self) for cnode in self.__result[fieldInfo[0]]["value"]]:
                    mu += str(record)
                continue
            mu += indent + "%s: %s\n" % (fieldInfo[0], self[fieldInfo[0]])
        return mu
        
    @property
    def id(self):
        """
        Note that 'id' makes more sense than uri as not fully qualified. Client needs to know what FileMan system it is getting data from.
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
        
    def fieldInfos(self):
        """
        Schema from the reply: better than generic "keys()"
        """
        return sorted([(field, self.__result[field]["type"], self.__result[field]["fmId"], FieldInfo.FIELDTYPES[self.__result[field]["fmType"]] if "fmType" in self.__result[field] else "") for field in self.__result if field != "uri"], key=lambda x: float(x[2]))  
        
    @property
    def container(self):
        return self.__container # may be None
        
    @property
    def level(self): # how many levels DOWN is a record
        level = 1
        container = self.__container
        while container:
            level += 1
            container = container.container
        return level
        
    def isListMultiple(self, fileInfo):
        """
        Temporary - need to pass in fileInfo as data from FMQL is not enough (need a "listMultiple" setting.)
        """
        pass
        
    def contains(self, fileType=""):
        """
        Note that this will build a tree which you can walk ie.
        [x, y, z]
            [a, b]
                [h, i] etc.
        """
        contains = []
        for field, value in self.__result.iteritems():
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for cnode in value["value"]:
                        cRecord = Record(cnode, self)
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
                for cnode in value["value"]:
                    cRecord = Record(cnode)
                    if not cRecord.isComplete:
                        return False
        return True
                
    def references(self, fileTypes=None, sameAsOnly=False):
        references = {} # make sure unique
        for field, value in self.__result.iteritems(): # REPLACE with iter()
            if field == "uri":
                continue
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for cnode in value["value"]:
                        cRecord = Record(cnode)
                        for reference in cRecord.references(fileTypes, sameAsOnly):
                            references[reference.id] = reference
                continue
            if value["type"] != "uri":
                continue
            reference = Reference(value)
            if fileTypes and reference.fileType not in fileTypes:
                continue
            if sameAsOnly and reference.sameAs == "":
                continue
            references[reference.id] = reference
        return references.values() 
        
    @property
    def numberAssertions(self):
        """
        How many triples/statements does this "tree" of records represent
        """
        no = 0
        for field, value in self.__result.iteritems():
            if field == "uri":
                continue
            if value["type"] == "cnodes":
                if "stopped" not in value:
                    for cnode in value["value"]:
                        cRecord = Record(cnode)
                        no += cRecord.numberAssertions
                continue
            no += 1
        return no

class Reference(object):

    def __init__(self, result):
        if result["type"] != "uri":
            raise Exception("Must create references with URIs")
        self.__result = result
        
    def __str__(self):
        """
        Some labels are strange as .01 is not a display thing: ex/ NAME COMPONENTS/2 vs NEW PERSON/MANAGER,SYSTEM ... TODO: FMQL
        """
        mu = self.label + " (" + self.id + ")"
        mu += " <=> " + self.sameAsLabel + " (" + self.sameAs + ")" if self.sameAs else ""
        return mu

    @property
    def id(self):
        return self.__result["value"]
                
    @property
    def label(self):
        return self.__result["label"].split("/")[1]
        
    @property
    def fileType(self):
        return self.__result["value"].split("-")[0]

    @property
    def fileTypeLabel(self):
        return self.__result["label"].split("/")[0]
        
    @property
    def sameAs(self):
        """
        Unlike 'internal'/local ids, sameAs is a full qualified URI
        """
        return self.__result["sameAs"] if "sameAs" in self.__result else ""
        
    @property
    def sameAsLabel(self):
        return self.__result["sameAsLabel"] if "sameAsLabel" in self.__result else ""
        
class CodedValue(object):
    """
    TODO: FMQL - must add more information to a coded value - specifically what is the index of the value and MN too.
    """
    def __init__(self, result):
        if result["fmType"] != "3":
            raise Exception("Must create CodedValues with CodedValues!")
        self.__result = result
        
    def __str__(self):
        mu = self.id + " [CODED VALUE]"
        return mu

    @property
    def id(self):
        return self.__result["value"]
        
    @property
    def index(self, fieldInfo):
        """
        Temporary need to pass in fieldInfo. From fieldInfo can get index from id.
        """
        pass
        
    def isBoolean(self, fieldInfo):
        """
        Temporary need to pass in fieldInfo. 
        """
        pass
        
    def booleanValue(self, fieldInfo):
        """
        Temporary need to pass in fieldInfo. 
        
        Return the boolean.
        """
        if not self.isBoolean(fieldInfo):
           raise Exception("Not a boolean coded value") 
        
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
        print "Number Assertions:", record.numberAssertions
        print
        print "Contents:"
        print record
        print 
        print "Its references:"
        for i, reference in enumerate(record.references(), 1):
            print i, reference
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
        print "It contains 2.06:"
        crecord = record.contains("2_06")[0]
        print crecord

if __name__ == "__main__":
    main()