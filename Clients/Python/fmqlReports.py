#!/usr/bin/env python

#
# FileMan report generators to illustrate the use of an FMQL Endpoint.
# v0.9
#
# An FMQL endpoint is "restful" - queries are made in HTTP GETs. Responses are
# JSON (application/json). With FMQL, JSON becomes FileMan's export format.
#
# In v0.9, the endpoint supports 7 query types, 4 for FileMan data and 
# 3 for its schema Data.
#
# Data:
# 1. DESCRIBE fully describes any node in FileMan has two variations: 
#    - Describe(uri) describes a particular node. For example, DESCRIBE 2-2 
#    describes the second node in PATIENT (2).
#    - Describe(typeId) describes all nodes of a particular type
# 2. SELECT lists all nodes of a particular type. For example, SELECT 2
#    lists all the patients in a VistA.
# 3. COUNT: counts all nodes of a particular type. For example, COUNT 2
#    counts all the patients in a VistA
# 4. COUNT REFS: counts all references of different types to particular nodes. For example, COUNT REFS 2-2 lists all references to this node by type.
# Describe and Select support LIMIT (how many), OFFSET (from what point) and 
# FILTER (criteria to apply)
#
# Schema:
# 1. DESCRIBE TYPE: describes any FileMan (file) type. For example, 
# "DESCRIBE TYPE 2" asks about the PATIENT type (2).
# 2. SELECT TYPES: lists all (file) types.
# 3. SELECTALLREFERRERSTOTYPE: lists all types that refer to a type. For example, 
#    "SELECTALLREFERRERSTOTYPE 2" returns all types that refer to PATIENT.
#
# Form of JSON in responses:
# - the non-schema queries (Describe, Select, Count) return a superset of
#   SPARQL JSON (see: http://www.w3.org/TR/rdf-sparql-json-res/)
#   - the key addition is "label". FMQL adds this display label to all values of type "uri"
# - the schema queries return custom formats.
#
# Note: to see how to call the endpoint from a browser/in Javascript, see the 
# source of fmRambler.html in /usr/local/fmql.
#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it under the terms of 
# the GNU Affero General Public License version 3 (AGPL) as published by the Free Software 
# Foundation.
# (c) 2010-2011 caregraf.org
#

import re 
import urllib, urllib2
import json

# These settings should match your FMQL configuration 
FMQLEP = "http://www.examplehospital.com/fmqlEP" # Endpoint address
# FMQLEP = "http://vista.caregraf.org/fmqlEP" # Endpoint address of Caregraf demo VistA
BASEURL = "http://www.examplehospital.com/fmql/" 

# ############# the patients in your VistA ################
#
# What patients are known to a VistA system? What's in the "PATIENT" (2) file?
#
# EXAMPLE OF:
# - how to get information about a type (of file) with "Describe Type",
# - get a list of its instances with "Select"
# - get details on each of those instances with "Describe"
# - how errors are signalled: the reply has an "error" entry
#
# Note the nature of important FileMan files. Lot's of links. This is "linked data". 
# Only "codes"/"dictionaries" don't link out.
#
def reportPatients():
    print "\r=== A report on the patient's in a VistA according to PATIENT (2) ===\r"
    query = {"fmql": "DESCRIBE TYPE 2"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    patientDescription = json.loads(urllib2.urlopen(queryURL).read())
    if patientDescription.has_key("error"):
        print "Got error %s trying to get definition of PATIENT - exiting" % patientDescription["error"]
        return
    print "Type %s (%s)\r" % (patientDescription["name"], patientDescription["number"])
    # Setting limit to 100. Your VistA may be big. Don't want to get everything.
    query = {"fmql": "SELECT 2 LIMIT 100"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    reply = json.loads(urllib2.urlopen(queryURL).read())
    if reply.has_key("error"):
        print "Got error %s trying to get contents of PATIENT - exiting" % reply["error"]
        return
    print "There are %d patients\r" % len(reply["results"])
    for result in reply["results"]:
        print "\r----- Details on patient %s -----\r" % result["uri"]["label"]
        query = {"fmql": "DESCRIBE %s" % result["uri"]["value"]}
        queryURL = FMQLEP + "?" + urllib.urlencode(query)
        reply = json.loads(urllib2.urlopen(queryURL).read())
        if reply.has_key("error"):
            print "Got error %s for %s - skipping" % (reply["error"], uri)
            continue
        for pred in reply["results"][0]:
            typedValue = reply["results"][0][pred]
            if typedValue["type"] == "cnodes": # stay simple. Skip embedded lists.
                continue
            print "\t%s: %s\r" % (pred, typedValue["value"])

# ############################ the Institutions in your VistA #####################
#
# The Institutions defined in your VistA. In most, the VA's default institutions
# are still there. Shouldn't these be cleaned up?
#
# EXAMPLE OF:
# the same as "reportPatients". So why not make a template function that can report 
# on any file and then call that. We could have reported on patients (2) with this
# template. 
#
def reportInstitutions():
    reportOnType("4")

def reportOnType(typeId):
    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "DESCRIBE TYPE %s" % typeId})
    typeDescription = json.loads(urllib2.urlopen(queryURL).read())
    if typeDescription.has_key("error"):
        print "Got error %s trying to get definition of type %s - exiting" % (typeDescription["error"], typeId)
        return
    print "\r\r===== A report on %s (%s) =====\r" % (typeDescription["name"], typeDescription["number"])
    print "%s\r" % typeDescription["description"]["value"]
    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "SELECT %s" % typeId})
    reply = json.loads(urllib2.urlopen(queryURL).read())
    if reply.has_key("error"):
        print "Got error %s trying to get contents of %s - exiting" % (typeDescription["NAME"], reply["error"])
        return
    print "There are %d items\r" % len(reply["results"])
    for result in reply["results"]:
        print "\r----- Details on %s -----\r" % result["uri"]["label"]
        queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "DESCRIBE %s" % result["uri"]["value"]})
        reply = json.loads(urllib2.urlopen(queryURL).read())
        if reply.has_key("error"):
            print "Got error %s for %s - skipping" % (reply["error"], result["uri"]["value"])
            continue
        for pred in reply["results"][0]:
            typedValue = reply["results"][0][pred]
            if typedValue["type"] == "cnodes": # stay simple. Skip embedded lists.
                continue
            print "\t%s: %s\r" % (pred, typedValue["value"])

# ############################ Patient Referrers ###########################
#
# VistA has 2 "anchors" for Patient data: PATIENT (2) and IHR PATIENT (9000001). Most
# of the information about a particular patient points INTO her record in these files.
# (the inward reference is the most common information arrangement in FileMan and typical 
#  of graphs in general).
#
# For example, Vital sign records for a patient are in GMRV VITAL MEASUREMENT (120.5). 
# Entries in this file point into PATIENT (2) to identify the patient they apply to.
# Problems (9000011) work the same way - except they identity a patient by reference
# to IHS PATIENT (9000001).
#
# Note: RPMS has the same patient files. However they use the prefix "VA" with file 2
# and drop "IHS" from file 9000001
#
# EXAMPLE OF:
# - Describe all of a type
# - using a filter
# - navigating patient data
# 
def reportProblemsOfPatient():
    patientId = "9" # nineth patient but this could be any. Be sure to pick a much tested fellow to see something interesting.
    print "\r\r===== Problems of patient %s =====\r" % (patientId)
    # field .02 (see the Problem schema at schema/900001) points to the patient's record
    query = {"fmql": "DESCRIBE 9000011 FILTER(.02=9000001-%s)" % patientId}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    reply = json.loads(urllib2.urlopen(queryURL).read())
    if reply.has_key("error"):
        print "Got error %s trying to get patient %s's %s entry - exiting" % (reply["error"], patientId, anchorType)
        return
    if not len(reply["results"]):
        print "\tNo problems for patient %s" % patientId
    print "\tThere are %d\r" % len(reply["results"])

# ############################ the types in your VistA ###########################
#
# What (file) types are in there?
#
# EXAMPLE OF:
# using "select Types". This query type lists all the (file) types in a VistA. The 
# report is the same as the first page of the Schema client
# 
def reportTypes():
    queryURL = FMQLEP + "?" + urllib.urlencode({"fmql": "SELECT TYPES"})
    reply = json.loads(urllib2.urlopen(queryURL).read())
    print "\r\r========= All Types in the System (can take a moment) =======\r"
    if reply.has_key("error"):
        print "Got error %s trying to get all types - exiting" % reply["error"]
        return
    print "There are %d\r" % len(reply["results"])
    for i in range(len(reply["results"])):
        print "Entry %d\r" % i 
        for field in reply["results"][i]:
            print "\t%s: %s\r" % (field, reply["results"][i][field])

# ############################ Walk a big file ###########################
#
# What packages do you have? BUILD (9.6) tells you. It's big, has lot's of entries.
# Let's walk the file, bit by bit
#
# EXAMPLE OF:
# - limit and offset to walk a file
# 
def reportBuilds():
    offset = 0
    limit = 1000 # 1000 at a time
    total = 0
    print "\r\r===== Builds in a VistA, %d at a time =====\r" % (limit)
    while True:
        query = {"fmql": "SELECT 9_6 LIMIT %s OFFSET %s" % (limit, offset)}
        queryURL = FMQLEP + "?" + urllib.urlencode(query)
        reply = json.loads(urllib2.urlopen(queryURL).read())
        print "Got next %d" % int(reply["count"])
        total = total + int(reply["count"])
        if int(reply["count"]) != limit:
            break
        offset = offset + limit
    print "All walked - total %d" % total
    # now let's just ask for a count!
    query = {"fmql": "COUNT 9_6"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    reply = json.loads(urllib2.urlopen(queryURL).read())
    print "Count counts %s entries" % reply["count"]    

# ############################# Main Driver ####################################

import sys
import getopt
import time

def main():

    reportPatients()
    reportInstitutions()
    reportProblemsOfPatient()
    reportTypes()
    reportBuilds()

if __name__ == "__main__":
    main()
