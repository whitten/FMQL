#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it under the terms of 
# the GNU Affero General Public License version 3 (AGPL) as published by the Free Software 
# Foundation.
# (c) 2015 caregraf
#

import re 
import json
from datetime import datetime
try:
    from pymongo import MongoClient
except:
    raise Exception("You must install the package pymongo - http://api.mongodb.org/python/current/installation.html")

"""
Companion for fmqlToMongo.py - see notes there on shape of MongoDB

INSTALL MONGO: http://docs.mongodb.org/manual/installation/

REM: before running - start local server ...
> ./mongod --dbpath data/filemandb & 
"""

MONGODBNAME = "filemandb"
MONGODB_URI = 'mongodb://localhost' # :27017/filemandb 

COLLECTIONS = {
    "PATIENTS": "2",
    "PROBLEMS": "9000011",
    "VITALS": "120_5",
    "PRESCRIPTIONS": "52"
}

def reportMongo():

    # REM: server must be started on params of MONGODB_URI
    try:
        client = MongoClient(MONGODB_URI)
    except Exception, err:
        print 'Error: %s' % err
        return

    print
    print "======= Report on Contents of MongoDB", MONGODBNAME, "========"

    print
    db = client[MONGODBNAME]
    print "Using DB", db.name
    
    print "Collections supported (mirror FileMan)", db.collection_names(include_system_collections=False)
    
    if len(db.collection_names(include_system_collections=False)) < len(COLLECTIONS):
        raise Exception("Did you run fmqlToMongo.py? Wrong number of collections in db - exiting")

    print
    useCollection = "PATIENTS"
    ftCollection = db[COLLECTIONS[useCollection]] 
    print "=== Collection", useCollection, "(", COLLECTIONS[useCollection], ") has", ftCollection.count(), "entries"

    # find first
    print
    print "Find first one"
    descr = ftCollection.find_one()
    print "\t", descr["name-2"], descr["_id"]
    for key, value in descr.iteritems():
        if key in ["_id", "name-2"]:
            continue
        print "\t\t", key, value
    print "Note that records are dictionaries so their keys come back in any order unless you specify one"
    print
        
    print
    useCollection = "PROBLEMS"
    ftCollection = db[COLLECTIONS[useCollection]] 
    print "=== Collection", useCollection, "(", COLLECTIONS[useCollection], ") has", ftCollection.count(), "entries"
    
    # find first
    print
    print "Find first one"
    for key, value in ftCollection.find_one().iteritems():
        print "\t", key, value
    print
        
    # find by id
    testId = "9000011-4"
    print "Lookup specific problem by id -", testId
    for descr in ftCollection.find({"_id": testId}):
        for key, value in descr.iteritems():
            print "\t", key, value
    print

    print "Count of problems of Patient 9", ftCollection.find({"patient_name-9000011.id": "9000001-9"}).count()
    print
    
    testId = "200-52"    
    print "'Project' specific keys of problems recorded by a specific provider 'NOTHER,NADA' with id 200-52", testId
    PROJECT_KEYS = {"_id": 1, "date_entered-9000011.value": 1, "diagnosis-9000011.sameAs": 1, "diagnosis-9000011.sameAsLabel": 1, "status-9000011.label": 1, "condition-9000011.label": 1}
    for i, descr in enumerate(ftCollection.find({"patient_name-9000011.id": "9000001-9", "recording_provider-9000011.id": "200-52"}, PROJECT_KEYS), 1):
        print "\t", i, descr["_id"]
        for key, value in descr.iteritems():
            if key == "_id":
                continue
            print "\t\t", key, value
    print
        
    # Didn't filter out HIDDEN with FMQL - doing in Mongo
    print "All but the 'HIDDEN' problems"
    PROJECT_KEYS = {"_id": 1, "date_entered-9000011.value": 1, "diagnosis-9000011.sameAs": 1, "diagnosis-9000011.sameAsLabel": 1, "status-9000011.label": 1, "condition-9000011.label": 1}
    for i, descr in enumerate(ftCollection.find({"patient_name-9000011.id": "9000001-9", "condition-9000011.label": {"$ne": "HIDDEN"}}, PROJECT_KEYS), 1):
        print "\t", i, descr["_id"]
        for key, value in descr.iteritems():
            if key == "_id":
                continue
            print "\t\t", key, value
    print

    print "Distinct doctors who wrote problems for patient 9"
    print ftCollection.find({"patient_name-9000011.id": "9000001-9"}).distinct("recording_provider-9000011.label")
    print
    
    print "Distinct patients with agent orange exposure (only one now and he's our guy, patient 9)"
    print ftCollection.find({"agent_orange_exposure-9000011": True}).distinct("patient_name-9000011")
    print
        
    print "All problems sorted by status (note: by default, sorted by 'date entered')"
    PROJECT_KEYS = {"_id": 1, "date_entered-9000011.value": 1, "recording_provider-9000011": 1, "diagnosis-9000011.sameAs": 1, "diagnosis-9000011.sameAsLabel": 1, "status-9000011.label": 1}
    for i, descr in enumerate(ftCollection.find({"patient_name-9000011.id": "9000001-9"}, PROJECT_KEYS).sort("status-9000011.label", 1), 1):
        print "\t", i, descr["_id"]
        for key, value in descr.iteritems():
            if key == "_id":
                continue
            print "\t\t", key, value
    print  
    
    print
    useCollection = "VITALS"
    ftCollection = db[COLLECTIONS[useCollection]] 
    print "=== Collection", useCollection, "(", COLLECTIONS[useCollection], ") has", ftCollection.count(), "entries"
    
    print
    useCollection = "PRESCRIPTIONS"
    ftCollection = db[COLLECTIONS[useCollection]] 
    print "=== Collection", useCollection, "(", COLLECTIONS[useCollection], ") has", ftCollection.count(), "entries"  
    print
    
    """
    Vitals:
    - limits and 
    """
    
    return
    
    """
    NEXT: indexing - http://api.mongodb.org/python/2.0.1/tutorial.html
    
    ... only printing out - will add indexes elsewhere.
    """
    print "No index stats"
    print ndc.find({"activeIngredient.substance.id": testSubstance}).explain()["cursor"]
    print ndc.find({"activeIngredient.substance.id": testSubstance}).explain()["nscanned"], "which is every doc!"

# ############################# Demo Driver ####################################

def main():

    reportMongo()
    
if __name__ == "__main__":
    main()
