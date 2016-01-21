#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2015-2016 caregraf
#

"""
A data model is different from a data dictionary. The native model of VISTA is implemented over the data dictionary. A series of enhancers add new meta data that lead to more
refined data models.

To get the native model, just instantiate from FileInfo and serialize. For a 
normalized model, enhance the native model before serializing.

Follows nuance of schema.org's class and property definitions (https://schema.org/Class)

TODO (FIRST):
- mandatory/required and overloading invalid settings
- input transform work leveraged for better typing (will be done in DDE)
- overloading names based approaches (start of DMN)
- PIKS approaches

See below for enhancements supported ie/ to go from DM to DMN.
"""

import re
from collections import OrderedDict
from fileManInfo import FileManInfo

class DataModel(object):
    def __init__(self, fileManInfo):
        self.__fileManInfo = fileManInfo
        fileInfosById = dict((fi.id, fi) for fi in self.__fileManInfo)
        self.__topClassInfos = [ClassInfo(fileInfo, fileInfosById) for fileInfo in self.__fileManInfo if not fileInfo.parent]
        self.__topClassInfosById = dict((ci.id, ci) for ci in self.__topClassInfos)
            
    def topClassInfos(self):
        return self.__topClassInfos
        
    def topClassInfoById(self, classId):
        if classId in self.__topClassInfosById:
            return self.__topClassInfosById[classId]
        return None
        
    def serialize(self):
        srl = object_pairs_hook=OrderedDict([("id", self.__fileManInfo.id)])
    
        if self.__fileManInfo.description:
            srl["description"] = self.__fileManInfo.description
        
        srl["classes"] = [classInfo.serialize() for classInfo in self.topClassInfos()]
        
        return srl
                
    def toJSONLD(self):              
        return json.dumps(self.serialize(), indent=4)

class ClassInfo(object):
    """
    Todo:
    - more on hierarchy and embedded classes
    
    Note: schema.org (https://schema.org/Class) Class doesn't
    add anything. 
    """

    def __init__(self, fileInfo, fileInfosById):
        self.__fileInfo = fileInfo
        self.__fileInfosById = fileInfosById
        
    def __str__(self):
        """
        TODO: expand for nice pretty print
        """
        mu = "Class: " + self.label + " (" + self.id + ")\n"
        mu += "Description: " + self.description + "\n" if self.description else ""
        propertyInfos = self.propertyInfos()
        mu += "Number of properties: " + str(len(propertyInfos)) + "\n"
        return mu
        
    @property
    def id(self):    
        """
        ClassId, by default, is <upper case label - fileId>
        
        This and label can be reset in order to make cleaner models
        """
        try:
            return self.__explicitId
        except:
            """
            Represents a Class or Type id for JSON serialization. FMQL JSON is based on these
            ids.
        
            Patient/2 -> Patient-2 ie/ upper case unlike field names -> property names
            """
            return FileManInfo.normalizeLabel(self.__fileInfo.label, False) + "-" + self.__fileInfo.id
        
    @id.setter 
    def id(self, value):
        self.__explicitId = value
        
    @property
    def fmDDId(self):
        return "fmdd:" + self.__fileInfo.id 
        
    @property
    def label(self):
        try:
            return self.__explicitLabel
        except:
            return self.__fileInfo.label
    
    @label.setter
    def label(self, value):
        self.__explicitLabel = value
        
    @property
    def description(self):
        try:
            return self.__explicitDescription
        except:
            return self.__fileInfo.description # can be ""
    
    @description.setter
    def description(self, value):
        self.__explicitDescription = value
        
    @property
    def sameAs(self):
        """
        From DD SameAs
        """
        raise Exception("Not yet implemented")
                    
    @property        
    def piks(self):
        try:
            return self.__piks
        except:
            return ""
                
    @piks.setter
    def piks(self, value):
        self.__piks = value
                
    @property
    def clinicalClassId(self):
        """
        TMP until do DMN separate from DM
        """
        try:
            return self.__clinicalClassId
        except:
            return ""
                        
    @clinicalClassId.setter
    def clinicalClassId(self, value):
        self.__clinicalClassId = value
        
    def isHierarchy(self):  
        """
        If one or more properties have (embedded) class ranges
        
        Note: feeds into what to do with embedded classes including whether they
        should have ids etc
        """
        raise Exception("Not yet implemented")     
        
    def propertyInfos(self):
        """       
        Note: no COMPUTED properties (ie/ from FieldInfos with computed values)
        as not yet returning them.
         
        TODO: change so properties can be deleted/changed/even created beyond
        FileMan.
        """
        return [PropertyInfo(fieldInfo, self.__fileInfosById) for fieldInfo in self.__fileInfo.fieldInfos() if not fieldInfo.computation]
        
    def serialize(self):
       
        srl = OrderedDict([("id", self.id), ("fmDD", self.fmDDId), ("label", self.label)])
                                                                            
        if self.description:
            srl["description"] = self.description
                                
        if self.piks != "":
            srl["piks"] = self.piks
            
        if self.clinicalClassId:
            srl["clinicalClass"] = {"id": self.clinicalClassId}   
                        
        srl["properties"] = [propertyInfo.serialize() for propertyInfo in self.propertyInfos()]    
                                                
        return srl
        
    def toJSONLD(self):                
        return json.dumps(self.serialize(), indent=4)        
        
class PropertyInfo(object):
    """
    Basis of PropertyInfo is FieldInfo. Enhancers establish:
    - "nice" global names
    - suppress .001 (not really a property)
    - note relationships between properties (base properties)
    - establish all new properties not backed by FileMan
    - suppress redundant or unneeded properties
    ... enhancement turns a DM (native data model) into a DMN
    (normalized data model)
    ... ALL ENHANCEMENT STEPS must be explicit.
    
    From schema.org property (https://schema.org/Property)
    - add: inverseOf (have sameAs already) 
    """

    def __init__(self, fieldInfo, fileInfosById):
        if fieldInfo.computation:
            raise Exception("Computed field - can't turn into Property (yet)")
        self.__fieldInfo = fieldInfo
        self.__fileInfosById = fileInfosById # to distinguish multiples
        
    def __str__(self):
        mu = "Property: " + self.label + " (" + self.id + ")\n"
        return mu
                
    @property
    def id(self):    
        
        def calculatePropertyId():
            """
            Property name is unique in the file, lowercase 
            normalized (for JSON-LD etc) and in the context of the file
            (ie/ not unique across all files so must suffix with fileId)
                
            Note: only the quote and the equal sign are excluded, so anything, except " and = are acceptable BUT other schemes are stricter.
            """
            if self.__fieldInfo.isNLabelOwner: # Not just name owner but normalized name owner
                return FileManInfo.normalizeLabel(self.__fieldInfo.label) + "-" + self.__fieldInfo.fileId
            # Not owner so must add the fieldId as well as the fileId to the name!
            return FileManInfo.normalizeLabel(self.__fieldInfo.label) + "-" + self.__fieldInfo.id
        
        """
        PropertyInfo allows this id to be reset (as well as the property name) in order
        to make cleaner models
        """
        try:
            return self.__explicitId
        except:
            return calculatePropertyId()
        
    @id.setter 
    def id(self, value):
        self.__explicitId = value
        
    @property
    def fmDDId(self):
        return "fmdd:" + self.__fieldInfo.id
        
    @property
    def label(self):
        try:
            return self.__explicitLabel
        except:
            return self.__fieldInfo.label
    
    @label.setter
    def label(self, value):
        self.__explicitLabel = value
        
    @property
    def domains(self):
        """
        By default, only one domain but may allow many. Examples like
        'date_created' or 'name'.
        """
        try:
            return self.__explicitDomains
        except:
            domainFileId = self.__fieldInfo.fileId
            domainClassId = ClassInfo(self.__fileInfosById[domainFileId], self.__fileInfosById).id
            return [domainClassId]

    @domains.setter
    def domains(self, values):
        self.__explicitDomains = values
        
    @property
    def description(self):
        try:
            return self.__explicitDescription
        except:
            return self.__fieldInfo.description # can be ""
                
    @description.setter
    def description(self, value):
        self.__explicitDescription = value
        
    @property
    def type(self):
        try:
            return self.__explicitType
        except:
            pass
        t, r = self.__typeAndRange()
        if t:
            return t
        return "" # if corrupt (multiple)
        
    @property
    def indexed(self):
        if self.__fieldInfo.index:
            return True
        return False
        
    @type.setter
    def type(self, value):
        self.__explicitType = value
        
    @property
    def range(self):
        t, r = self.__typeAndRange()
        return r # none if none    
        
    def __typeAndRange(self):
        """
        Type and Range are bound up together
        
        Types:
        - STRING (merges FREE TEXT and WORD PROCESSING)
        - NUMERIC
        - POINTER (merges VARIABLE POINTER and (FIXED) POINTER
        - OBJECT (multiple reference - zero or more)
        - ENUMERATION (SET OF CODES)
        - BOOLEAN (broken out of ENUMERATION on the metal)
        - DATETIME
        
        REM: no "COMPUTED" as that gets into source and not type
        """            
                
        # May revisit - pass on data analysis (Issue: )
        if self.__fieldInfo.type in ["WORD-PROCESSING", "FREE TEXT"]:
            return "STRING", None
            
        # NOTE: shows FMQL meta nicer if includes name of file along with id
        # in the range
        if re.search(r'POINTER', self.__fieldInfo.type):
            fileIds = self.__fieldInfo.ranges()
            try:
                classRefs = [{"id": ClassInfo(self.__fileInfosById[fileId], self.__fileInfosById).id} for fileId in self.__fieldInfo.ranges()]
                return "POINTER", classRefs[0] if len(classRefs) == 1 else classRefs
            except:
                print "Warning: bad file(s) referenced", fileIds, "from", self.__fieldInfo.fileId, ":", self.__fieldInfo.id
                return None, None                
                
        # TODO: isListMultiple ie/ array of simple type too
        if re.search(r'MULTIPLE', self.__fieldInfo.type):
            try:
                mFileInfo = self.__fileInfosById[self.__fieldInfo.multipleId]
            except:
                print "Warning: multiple referenced", self.__fieldInfo.multipleId, "is unknown - from", self.__fieldInfo.fileId, ":", self.__fieldInfo.id
                return None, None
            else:
                # going to grab the type of that one field
                if mFileInfo.isListMultiple:
                    oneFieldInfo = mFileInfo.fieldInfos()[0]
                    if oneFieldInfo.type == "MULTIPLE":
                        raise Exception("Didn't expect MULTIPLE as single field of Single Value List: " + mFileInfo.id)
                    mPropInfo = PropertyInfo(oneFieldInfo, self.__fileInfosById)
                    return [mPropInfo.type], mPropInfo.range if mPropInfo.range else None
                else:
                    return "[OBJECT]", ClassInfo(mFileInfo, self.__fileInfosById)
                             
        if re.search(r'SET OF CODES', self.__fieldInfo.type):
            return "ENUMERATION", [cval["expanded"] for cval in self.__fieldInfo.codes()]
        
        return self.__fieldInfo.type, None  
        
    # TODO:
    @property
    def isMandatory(self):
        """
        From "R" flag in DD but must check against real data
        """
        raise Exception("Not yet implemented") 
        
    @isMandatory.setter
    def isMandatory(self, value):
        """
        Need to override FM defaults as see patterns
        """
        raise Exception("Not yet implemented")
        
    def serialize(self):
        srl = OrderedDict([("id", self.id), ("fmDD", self.fmDDId), ("label", self.label)])
    
        if self.description:
            srl["description"] = self.description
            
        if not self.type:
            srl["corrupt"] = True
            return srl
            
        # REM: @type is reserved
        srl["datatype"] = self.type
        
        if self.indexed:
            srl["indexed"] = True
        
        if self.range:
            if srl["datatype"] != "[OBJECT]":
                srl["range"] = self.range
            else:
                srl["range"] = self.range.serialize()
                            
        return srl
        
    def toJSONLD(self):                
        return json.dumps(self.serialize(), indent=4)        
        
# ############################# Driver ####################################
    
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
    
    dataModel = DataModel(fileManInfo)
            
    if not os.path.isdir(baseLocation + "/META"):
        os.mkdir(baseLocation + "/META")
            
    modelFile = baseLocation + "/META/model.json"
    print "Schema written out to", modelFile
    open(modelFile, "w").write(dataModel.toJSONLD())
        
if __name__ == "__main__":
    main()