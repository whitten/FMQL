#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2016 caregraf
#

import os
import re
from collections import defaultdict, OrderedDict
import json

"""
FileInfo and FieldInfo provides access to the native DD's of FileMan. They are
instantiated from FMQL DescribeType replies. 

Context: maximal exposure of DD content - will enhance selectively, turning
intrinsic information (input transform for date range) into structured information.
But the DD is NOT a model per se. A (native) model can be generated but that's
a separate step.

EVOLVING TO BE DDE, not DD maker: DD should be exposed natively, nicely where
RAW == SERIALIZE below but DDE still needed here.

Built upon by ClassInfo (and PropertyInfo) to make native and normalized models.

NOW TODO: 
- CHCS / VISTA SAME AS IN JSON DEFNS

TODO: HERE:
- EXTRA DATA: IA + package.json (OSEHRA) adding Package info
- SAMEAS formally exposed (out of procedural code)
- transforms: process into refined types for dates/blank nodes etc
- if allow Real Type then how to change value form too ex/ NUMERIC -> POINTER
means IEN must be qualified ie/ "# of file entry" isn't enough. ie/ must 
set POINTER_RANGE too

TODO: MUMPS TO CHANGE: so SERIALIZE == RAW AND 
- POINTER RANGE needs label as well as id (to be self contained)
- put back SET OF CODES and BOOLEAN as hard code enhancement inside (or may
take out and put into model section?)
  - some BOOLEANS not caught so QA is needed too
- sameAs inside the extras (not just hard coded)
- FMTYPE: return VISTA or CHCS in FM meta (about call)
- DINUM support ***
"""

class FileManInfo:
    """
    Gathers file definitions of a particular FileMan
    """
    def __init__(self, fileInfos, id, fmType, description=""):
        """
        - fmType is temporary - should come from FileInfos
        """
        self.fileInfosIndexed = {}

        self.__id = id
        self.__fmType = fmType
        self.__description = description

        for fi in fileInfos:

            self.fileInfosIndexed[fi.id] = fi

    @property
    def id(self):
        return self.__id
 
    @property
    def fmType(self):
        return self.__fmType

    @property
    def description(self):
        return self.__description

    @property
    def size(self):
        return len(self.fileInfosIndexed)

    def __iter__(self):
        oids = sorted(self.fileInfosIndexed.keys(), key=lambda id: float(re.sub("_", ".", id)))
        for id in oids:
            yield self.fileInfo(id)

    def fileInfo(self, fid):
        if re.sub(r'\.', '_', fid) not in self.fileInfosIndexed:
            return None
        return self.fileInfosIndexed[re.sub(r'\.', '_', fid)]
        
    def serialize(self):
        # Rem: fmType, not type - 'type' is reserved for @type
        srl = object_pairs_hook=OrderedDict([("id", self.id), ("fmType", self.fmType)])
    
        if self.description:
            srl["description"] = self.description
        
        srl["files"] = [fi.serialize() for fi in self]
        
        return srl
                
    def toJSONLD(self):                
        return json.dumps(self.serialize(), indent=4) 
        
    @staticmethod
    def normalizeLabel(label, lower=True):
        """
        NOTE: in here and NOT in the model as want to record name uniqueness at
        a FileMan level. How the model treats that uniqueness is then up to it.
        ... may reconsider and put name uniqueness completely into DataModel.
        
        Static to allow the same normalization to be used without creating the 
        full fileInfo

        For now, going a lot stricter than JSON standards 
        ie/ excluding all but A-Za-z\d_\-
        No $, ., #, %, (), [], {}, :, &, @, ', / or comma

        ex/ Utility doesn't like ":" in a field name ex/ ROUTINES (RN1:RN2) -> ROUTINES (RN1-RN2)
        ex/ ALIAS FMP/SSN ... don't want / in a URL'ed id
        
        NOTE: will go to MUMPS with DESCRIBE TYPE returning property name for field (according to File Type Tree). Means no need for this.
        
        ex/ 2/ place_of_birth_city from place of birth [city] - dropping specials
        
        underscore case, not camel case. No hungarian notation (sorry Windows C people) 
        """
        # FIELDTOPRED Match
        # - get rid of - inside ie/ not replaced so k2-phone -> k2phone
        # - space, / become _'s
        # - no multiple __'s
        # ... perhaps should change so k2phone_number -> k2_phone_number ie/ treat - like / and space
        
        nname = re.sub(r'[ \/]', '_', re.sub(r'[^A-Za-z\d\_ \/]', '', re.sub(r'\#', 'number', label)))
        
        if lower:
            nname = nname.lower()

        # Need to _ before \d
        if re.match(r'\d', nname):
            nname = "_" + nname
        return nname       
    
# ########################### Basic FileInfo and FieldInfo ###############

class FileInfo(object):

    """
    A simple facade for easy access to an FMQL Describe Type result, FMQL's representation of the contents
    of a FileMan file's DD definition.
    
    It highlights:
    - list multiple: multiples with only one field - these 'sub files' represent simple lists of items
    - does a field "own" its name ie/ is it the first user of a name. Remember that 
    - set of codes: some are just booleans (Y or Y/N). These are distinguished from "proper" sets of codes.
    
    Fixes FMQL quirks:
    - file id returned in . form in Schema but Data uses _ form. _ form used here
    - lot's of "detail" stuffed into one 'detail' field. Expanded in FieldInfo
        
    TODO: 
    - handle "corrupt" file and "corrupt" field info properly for easy reports
    - dinum etc and a lot more nuance in fields including ranges [for fuller schema defn]
    """ 
    def __init__(self, describeTypeResult, fmType="VISTA"):
        if "error" in describeTypeResult:
            raise Exception("Can't access error results")
        self.__result = describeTypeResult
        self.__fmType = fmType
        
    def __str__(self):
        mu = "File: " + self.label + " (" + self.id + ")\n"
        mu += "Count (FileMan): " + self.count + "\n" if self.count else ""
        if self.location:
            mu += "Location: " + self.location + "\n"
            mu += "Global: " + self.gbl + "\n"
        mu += "Parent: " + self.parent + "\n" if self.parent else ""
        mu += "Description: " + self.description + "\n" if self.description else ""
        if self.isListMultiple:
            mu += "List Multiple\n"
            mu += "\ttype: " + self.fieldInfos()[0].type + "\n"
            return mu
        # The following won't apply to list multiples
        mu += "Files referenced (not including multiples and theirs): " + str(len(self.references())) + "\n"
        fieldInfos = self.fieldInfos()
        mu += "Number of fields: " + str(len(fieldInfos)) + "\n"
        mu += "Number of indexed fields: " + str(len([fieldInfo for fieldInfo in fieldInfos if fieldInfo.index])) + "\n"
        fieldInfosByType = defaultdict(list)
        for fieldInfo in fieldInfos:
            # Want to distinguish boolean and non boolean enum
            ftype = fieldInfo.type
            if ftype == FieldInfo.FIELDTYPES["3"] and fieldInfo.type == "BOOLEAN":
                ftype = "SET OF CODES [BOOLEAN]"
            fieldInfosByType[ftype].append(fieldInfo)
        for i, fieldType in enumerate(fieldInfosByType, 1):
            no = len(fieldInfosByType[fieldType])
            mu += "Number of " + fieldType + ": " + str(no) + " (" + str((no*100)/len(fieldInfos)) + "%) " + "\n"
        mu += "** Duplicates one or more field names **\n" if self.hasDuplicatedFieldLabels else ""
        return mu
           
    @property
    def id(self):
        return re.sub(r'\.', '_', self.__result["number"])
        
    @property
    def numericId(self):
        """
        Ids should be .\d+ or \d+ in FileMan but have seen hard coded
        examples that use Alphanumerics
        
        For now, leave as string as want .01 and not 0.01
        """
        try:
            int(self.__result["number"])
        except:
            try: 
                float(self.__result["number"])
            except:
                return ""
            else:
                return self.__result["number"]                
        else:
            return self.__result["number"]
           
    @property
    def label(self):
        return self.__result["name"].title()
                        
    @property
    def description(self):
        return self.__result["description"]["value"] if "description" in self.__result else ""
        
    @property
    def sameAs(self):
        """
        From DD SameAs
        
        Will use path expressions to identify the code and the hardcoded ns. May be more
        than one with the first being tried first. Can apply on Server Side.
        
        TODO: load from JSON at first
        """
        raise Exception("Not yet implemented")
        
    @property
    def count(self):
        return self.__result["count"] if "count" in self.__result else ""
        
    @property
    def applicationGroups(self):
        # TODO: id these into something meaningful
        return self.__result["applicationGroups"].split(";") if "applicationGroups" in self.__result else ""
        
    @property
    def lastIEN(self):
        return self.__result["lastIEN"] if "lastIEN" in self.__result else ""
    
    @property        
    def location(self):
        if "location" in self.__result:
            return self.__result["location"]
        return ""
        
    @property
    def gbl(self):
        """
        Base global ie DPT or ... Way to gather files that all share an global!
        
        Can't use 'global' as Python uses it
        """
        if not self.location:
            return ""
        return re.match(r'\^([^\(]+)', self.location).group(1)
       
    @property
    def parent(self):
        # Test if multiple this way too
        if "parent" in self.__result:
            return re.sub(r'\.', '_', self.__result["parent"]) 
        return ""
            
    @property
    def isListMultiple(self):
        """
        TODO: FMQL - embed number of fields in the FieldInfo of a multiple field. That way, can know if it is a list multiple.
        """
        if not self.parent:
            return False
        if len(self.fieldInfos()) == 1:
            return True
        return False
        
    @property
    def isBlankNodeMultiple(self):
        raise Exception("Not yet implemented - may do from inputTransform if any")
        
    def multiples(self):
        return [fieldInfo.multipleId for fieldInfo in self.fieldInfos() if fieldInfo.type == "MULTIPLE"]
        
    def references(self):
        """
        Note: unlike DESCRIBE for data, DESCRIBE TYPE does not return the (meta) data about contained entities. You need to explicitly go through DescribeTypeReply's for contained multiples to get all the references of the tree represented by this type of record.
        ie/ 
        allReferences(fId, fileInfoManager):
            references = fileInfoManager.fileInfo(fId).references()
            for cFId in fi.multiples():
                references |= allReferences(cFId, fileInfoManager)
            return references
        """
        return set(range for fieldInfo in self.fieldInfos() if fieldInfo.type in ["POINTER", "VARIABLE-POINTER"] for range in fieldInfo.ranges())
                
    def fieldInfos(self): # only non corrupt
        """
        Iterator of field information - returns tuples that describe each field
        """
        try:
            return self.__fieldInfos
        except:
            pass
        self.__fieldInfos = []
        
        """
        ?: may move into model ie/ as put properties into class, consider if
        name unique in context of class or not.
        """
        namesSoFar = set()
        nnamesSoFar = set() # may have unique name but not when normalization is taken into account
        for fieldResult in self.__result["fields"]:
            if "corruption" in fieldResult: # corruption (may be a name, maybe not)
                continue
            nname = FileManInfo.normalizeLabel(fieldResult["name"])                
            self.__fieldInfos.append(FieldInfo(self.id, fieldResult, fieldResult["name"] not in namesSoFar, nname not in nnamesSoFar, self.__fmType))
            namesSoFar.add(fieldResult["name"])
            nnamesSoFar.add(nname)
        return self.__fieldInfos
        
    def fieldInfo(self, id):
        for fieldInfo in self.fieldInfos():
            if fieldInfo.id == id:
                return fieldInfo
        return None
        
    @property
    def hasDuplicatedFieldLabels(self):
        """
        REM: FileMan only enforces uniqueness for field ids, not for field names.
        """
        for fieldInfo in self.fieldInfos():
            if not fieldInfo.isLabelOwner:
                return True
        return False
        
    @property
    def hasDuplicatedNFieldLabels(self):
        """
        REM: name may be unique but what about the normalized name that leads to a property?
        """
        for fieldInfo in self.fieldInfos():
            if not fieldInfo.isNLabelOwner:
                return True
        return False
        
    def corruptFields(self):
        raise Exception("Not yet implemented")
        
    # ##################### Native Serialization to JSON-LD ################
    
    def serialize(self):
        
        srl = OrderedDict([("id", self.id), ("label", self.label)])
        
        # Explicitly left out ("classId", self.classId) as should be just in DM
        
        if self.numericId:
            srl["numericId"] = self.numericId
                                                                            
        if self.description:
            srl["description"] = self.description
            
        # These two are source system specific
        if self.count:
            srl["count"] = self.count
        if self.lastIEN:
            srl["lastIEN"] = self.lastIEN
            
        if self.location:
            srl["location"] = self.location
            srl["global"] = self.gbl
            
        if self.parent:
            srl["parent"] = {"id": self.parent} # will embed label when have it
            if self.isListMultiple:
                srl["isListMultiple"] = True
                
        if self.applicationGroups:
            srl["applicationGroups"] = self.applicationGroups
            
        srl["fields"] = [fieldInfo.serialize() for fieldInfo in self.fieldInfos()]          
        
        return srl 
        
    def toJSONLD(self):                
        return json.dumps(self.serialize(), indent=4)     
        
class FieldInfo(object):

    # TODO Til FMQL uses .8. ie/ I FLDFLAGS["D" S FLDTYPE=1 ; Date 
    # will look into file .81/^DI(.81,"C","D")
    # TODO: add "BOOLEAN" ... "FREE TEXT" -> STRING, FORMATTED STRING, POINTER etc.
    FIELDTYPES = {
        "1": "DATE-TIME",
        "2": "NUMERIC",
        "3": "SET OF CODES",
        "4": "FREE TEXT",
        "5": "WORD-PROCESSING",
        # BC, DC and Cp# will decide the type of a computed field ie/ computed field type
        # ie/ don't expect to see COMPUTED as a field type
        "6": "COMPUTED", 
        "7": "POINTER",
        "8": "VARIABLE-POINTER",
        "9": "MULTIPLE",
        # Q C***, K VISTA
        "10": "MUMPS", 
        "11": "IEN", # IEN match in .001
        # Both attributed by FMQL and BC for Computed
        "12": "BOOLEAN" # SET UP IN FMQL - SET OF CODES turned into boolean in schema
    }

    # a name owner is the FIRST field in the file to use a name. All others don't 'own' the name
    def __init__(self, fileId, describeFieldResult, isLabelOwner, isNLabelOwner, fmType="VISTA"):
        self.__fileId = fileId
        self.__result = describeFieldResult   
        self.__isLabelOwner = isLabelOwner
        self.__isNLabelOwner = isNLabelOwner
        self.__fmType = fmType
        self.type
        
    def __str__(self):
        mu = "Field: " + self.label + " (" + self.id + ")\n"
        return mu
        
    @property
    def raw(self):
        """
        NOTE: once FMQL/raw exposure matches the result of 'serialize', 'serialize' will
        go away as then raw is enough
        """
        return self.__result
                
    @property
    def id(self):
        """
        Fully qualified of FILEID-FIELDID with . -> _
        """
        return self.__fileId + "-" + re.sub(r'\.', '_', self.__result["number"])

    @property
    def numericId(self):
        """
        Ids should be .\d+ or \d+ in FileMan but have seen hard coded
        examples that use Alphanumerics
        
        For now, leave as string as want .01 and not 0.01
        """
        try:
            int(self.__result["number"])
        except:
            try: 
                float(self.__result["number"])
            except:
                return ""
            else:
                return self.__result["number"]                
        else:
            return self.__result["number"]
        
    @property
    def fileId(self):
        return self.__fileId
        
    @property
    def label(self):  
        # Note: only container would know if ambiguous
        return self.__result["name"].title()
        
    @property
    def isLabelOwner(self):
        """
        Owner is the first (order by id) user of a name in a file
        """
        return self.__isLabelOwner
                
    @property
    def isNLabelOwner(self):
        return self.__isNLabelOwner

    @property
    def description(self):
        return self.__result["description"]["value"] if "description" in self.__result else ""
    
    @property 
    def type(self):
        """
        Accounts for COMPUTED - makes range concrete but also ensures a computation is there. Will avoid with FMQL v1.1. where can drop Computed as a "type"
        """
        # FMQL v1.1 - remove this override of FMQL MUMPS type assignment
        if self.__fmType != "VISTA":
            # MUMPS_FLAG = "K" if self.__fmType == "VISTA", "Q" for C***
            # K is C*** means consistency check.
            # FMQL V1.1 will fix but now, absence of K means making MUMPS!
            # Careful - C*** 1.23/.01 has F and Q but really just a string
            # but note that it can be either MUMPS or a String (Q or F) if you
            # read its comment.
            if self.__result["type"] == "4" and (re.search(r'Q', self.__result["flags"]) and not re.search(r'F', self.__result["flags"])): # ie defaulted to MUMPS as no Q etc.
                return self.FIELDTYPES["10"]
            # Reverse application_filter-757_2
            if self.__result["type"] == "10": # based on K - wrong: crude but default to String
                return self.FIELDTYPES["4"]  
        # FMQL returns "COMPUTED" for type of computed field. Break out to subtype     
        # Important: Cm == is NOT computed multiple. m means multi-line as in multi-line computed. These are strings.
        # Note: ignoring M (as to enter values into a multiple as MP is just a pointer.         
        if self.__result["type"] == "6": # 6 as has "C"
            # Need computation for computed types - otherwise how does user know?
            if not re.search(r'C', self.__result["flags"]):
                raise Exception("FMQL Bug - marked as computed " + self.id + " but C not in flags")
            if not self.computation:
                raise Exception("Computed field " + self.id + " has no computation")
            # DC, BC, Cp{fid}
            # Test ex/ CHCSS:1 Number-Meaningful 411
            if re.search(r'B', self.__result["flags"]):
                return self.FIELDTYPES["12"]
            # Before V1.1, FMQL already noted this as D so won't get here
            # Note: there are exs of DC in C*** but all corrupt? Don't show
            if re.search(r'D', self.__result["flags"]):
                return self.FIELDTYPES["1"]
            # C*** has Cmp for multiple pointer. VistA has Cp for computed pointer
            if re.search(r'Cm?p[\d\.]+', self.__result["flags"]):
                return self.FIELDTYPES["7"] # need to take care of range below
            return self.FIELDTYPES["4"] # default to String      
        return self.FIELDTYPES[self.__result["type"]]
        
    @property
    def realType(self):
        """
        Not overloading "type" as dictionary will still keep it for backward
        compatibility. 'realType' is just an annotation added in (like BOOLEAN)
        """
        try:
            return self.__realType
        except:
            return ""
    
    @realType.setter     
    def realType(self, value):
        """
        Prime example is that a NUMERIC is really an IEN but problem is, IEN to what?
        ie/ just resetting type is not enough.
        
        NOTE: DataModel doesn't use this yet but it should.
        """
        self.__realType = value
        
    @property
    def flags(self):
        """
        RAW DATA - really want internal and expose it below
        """
        return self.__result["flags"]
        
    @property
    def required(self):
        """
        TODO: "R" for Required - need to apply
        """
        raise Exception("Not yet implemented")
                        
    @property
    def location(self):
        """
        Computed/IEN have no location.
        """
        if "location" in self.__result:
            return self.__result["location"]
        return ""

    @property
    def index(self):
        return self.__result["index"] if "index" in self.__result else ""
        
    @property
    def inputTransform(self):
        """
        An INPUT transform is M code for a particular field that is executed to determine if the data for that field is valid.
        
        TODO: process to determine range of #'s, dates etc
        """
        return self.__result["inputTransform"] if "inputTransform" in self.__result else ""

    @property
    def triggers(self):
        """
        See if BUG in FMQL - 2-_096 is 2-096 from 2/096
        """
        if "triggers" not in self.__result:
            return None
        triggerIds = []
        for tpi in self.__result["triggers"].split(","):
            triggerIds.append(re.sub(r'\.', '_', re.sub(r'\/', '-', tpi)))
        return triggerIds
        
    @property
    def crossReferenceCount(self):
        return self.__result["crossReferenceCount"] if "crossReferenceCount" in self.__result else ""

    @property
    def computation(self):
        """
        If present then computed field - note: can have BOOLEAN/COMPUTATION etc
        ... Model will ignore these for now.
        """
        return self.__result["computation"] if "computation" in self.__result else ""

    @property
    def multipleId(self):
        return re.sub(r'\.', '_', self.__result["details"]) if self.__result["type"] == "9" else ""  

    def ranges(self):
        """
        TODO: FMQL - should record NAME of file pointed to as well as its id and
        this goes for Cp{id} too. Right now, can't use name-based ids.
        
        TODO: not catching "0" or invalid file id as range. Need to - at least note corruption.
        
        Testing examples:
        - Computed Multiple Pointer: CHCSS:diagnosis-2_9505
        """
        # Can be COMPUTED Cp{id} too
        if self.type not in ["POINTER", "VARIABLE-POINTER"]:
            return []
        if self.__result["type"] == "7":
            return [re.sub(r'\.', '_', self.__result["details"])]
        if self.__result["type"] == "6": # must be Cp{id} or Cmp{id} (CHCS only)
            return [re.sub(r'\.', '_', re.search(r'Cm?p([\d\.]+)', self.__result["flags"]).group(1))]
        # 8 VARIABLE POINTER         
        return [re.sub(r'\.', '_', vrFileId) for vrFileId in self.__result["details"].split(";")]
        
    @property
    def rangeType001(self):
        """
        IEN FLAGS:
        - NJ12,0 ... 999999999999 is max; NJ2,0 ... 999 but X>100 for NJ3,0
        (ie/ loop IEN fields for patterns)
        
        Note on "IEN" == .001/NUMBER field
            in VistA FM (but not C***), .001 is reserved as a field for IENs. It serves both as an identifier for "IEN" in the FileMan API (ie/ ask for IEN just like any other field) and in its computation, it sets a range for IENs. Not that not all files (284 out of 5743 in FOIA 2013) have this field. Even where they don't, FileMan's API treats the keyword "NUMBER" to mean IEN - even if the IEN is a date or other format.
        """
        if self.__result["type"] != "11":
            raise Exception("Not an .001")
        # Most .001's are #'s with computations like 'K:+X'=X!(X>999999)!(X<1)!(X?.E1"."1N.N) X'
        if re.match(r'N', self.__result["flags"]):
            return "NUMBER"
        # ex/ 2_98 Field: Appointment Date/Time
        if re.match(r'D', self.__result["flags"]):
            return "DATE"
        return "OTHER" # TBD: range check
        
    def codes(self):
        """
        Singleton coded values (bound only): len(codes) == 1
        """
        if self.__result["type"] not in ["3", "12"]:
            return None
        codes = []
        for enumValue in self.__result["details"].split(";"):
            if not enumValue:
                continue
            if re.search(r':', enumValue):
                enumMN = enumValue.split(":")[0]
                enumLabel = enumValue.split(":")[1]
            else:
                # TBD: FMQL TODO - RPMS "BORDERLINE" has no :
                enumMN = enumValue
                enumLabel = enumValue  
            codes.append({"mn": enumMN, "expanded": enumLabel}) 
        return codes
        
    def booleanOfValue(self, value):
        """
        Takes value and turns into Boolean equivalent
        
        May deprecate as FMQL makes Boolean transformation
        """
        if self.type != "BOOLEAN":
            raise Exception("Not boolean coded")
        if value in ["Y", "YES", "1", self.__result["name"]]:
            return True
        return False
        
    # ##################### Native Serialization to JSON-LD ################
    
    def serialize(self):
        """
        Will go away once raw == result of this serialization ie/ FMQL/on 
        the metal supports 'proper' object serialization
        """
        srl = OrderedDict([("id", self.id), ("label", self.label)])
                                    
        # Should be mandatory but just in case                          
        if self.numericId:
            srl["numericId"] = self.numericId
                                                                            
        if self.description:
            srl["description"] = self.description
            
        srl["type"] = self.type
        
        srl["flags"] = self.flags
        
        if self.location:
            srl["location"] = self.location
            
        if self.inputTransform:
            srl["inputTransform"] = self.inputTransform
            
        if self.index:    
            srl["index"] = self.index
            
        if self.triggers: # TODO: format in accessor above
            srl["triggers"] = self.triggers
            # Test: chcss:lower_bound-59_208 ... > 1 trigger
            # Test invalid triggered: zip_code-8221_4 (bypassed)
                    
        if self.crossReferenceCount: # should be 0?
            srl["crossReferenceCount"] = self.crossReferenceCount
            
        # .001 seems to always have an Input Transform
        if self.numericId == ".001":
            if self.type != "IEN":
                raise Exception("Expected all .001 to be IEN")
            srl["range001"] = self.rangeType001
            return srl
            
        # DONE! Accounting for in MULTIPLE Class BUT does it know its
        # containing field? Or if ListMultiple?
        if self.type == "MULTIPLE":
            srl["multipleId"] = {"id": self.multipleId}
            return srl
                
        # Note: if a type (BOOLEAN/POINTER etc) is computed (has a computation) then
        # do model shouldn't show it and it won't have any details
        if self.computation:
            srl["computation"] = self.computation
            return srl
                
        if self.type == "SET OF CODES": # 3
            srl["codeValues"] = self.codes()
        elif self.type == "BOOLEAN": # 12
            srl["codeValues"] = self.codes()
        elif self.type == "POINTER":            
            srl["pointerRange"] = [{"id": id} for id in self.ranges()] # leaving as list for compatibility with VPTR
        elif self.type == "VARIABLE-POINTER":
            srl["pointerRange"] = [{"id": id} for id in self.ranges()]
            
        return srl

    def toJSONLD(self):                
        return json.dumps(self.serialize(), indent=4)        
        
# ############################# Simple Population from Cache ####################

def loadFileManInfoFromCache(cacheLocation, name, fmType, description=""):
    # TODO: change to get name etc from about
    fis = []
    for replyFile in os.listdir(cacheLocation):
        if not re.search(r'SCHEMA\_', replyFile):
            continue
        reply = json.loads(open(cacheLocation + replyFile).read())
        fi = FileInfo(reply, fmType)
        fis.append(fi)
    fileManInfo = FileManInfo(fis, name, fmType, description)
    if fileManInfo.size == 0:
        raise Exception("No schemas in cache - exiting")
    return fileManInfo
           
# ############################# Test Driver ####################################

import sys
import urllib
import urllib2

# FMQLEP = "http://www.examplehospital.com/fmqlEP"
FMQLEP = "http://livevista.caregraf.info/fmqlEP"     

import os
import sys
import json
from fileManInfo import FileManInfo, loadFileManInfoFromCache 

DEFAULT_CACHE_BASE = "/data/" # /data/VISTAAB etc ie/ where data cached - work off cache below

# Ex/ python dataModel.py OSEHRA VISTA /data/
def main():

    if len(sys.argv) < 3:
        print "need to specify both the system name (ex/ OSEHRA) and type (VISTA or CHCS)  - exiting"
        return
        
    sysName = sys.argv[1]
    sysType = sys.argv[2]
        
    cacheBase = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_CACHE_BASE

    print
    print "Generating model.json for", sysName
    print
    
    baseLocation = cacheBase + sysName
    cacheLocation = baseLocation + "/JSON/"
    if not os.path.isdir(cacheLocation):
        raise Exception("Can't find cache location")
    # Third argument, description, not passed in
    fileManInfo = loadFileManInfoFromCache(cacheLocation, sysName, sysType)
                
    if not os.path.isdir(baseLocation + "/META"):
        os.mkdir(baseLocation + "/META")
        
    ddFile = baseLocation + "/META/dd.json"
    print "DD written out to", ddFile
    open(ddFile, "w").write(fileManInfo.toJSONLD())

    return

    # Simpler

    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "DESCRIBE TYPE 2"}) 
    reply = json.loads(urllib2.urlopen(queryURL).read())
    fi = FileInfo(reply)
        
if __name__ == "__main__":
    main()
