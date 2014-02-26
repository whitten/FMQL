#!/usr/bin/env python

#
# The following runs FMQL queries against Caregraf's Demo VistA ("livevista") which returns FileMan data as JSON-LD. 
# 
# There are many ways to arrange ("frame") the same content in JSON-LD. FMQL returns a 
# tree shaped framing which as the Framing spec 
# (http://json-ld.org/spec/latest/json-ld-framing/) says
#
#   "A JSON-LD document is a representation of a directed graph. A single directed graph can have many different serializations, each expressing exactly the same information. Developers typically work with trees, represented as JSON objects"
#
# Javascript Equivalent Coming
#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it under the terms of 
# the GNU Affero General Public License version 3 (AGPL) as published by the Free Software 
# Foundation.
# (c) 2014 caregraf
#

import re 
import urllib, urllib2
import json

try: # for showing how to reframe replies
    from pyld import jsonld
except:
    pass

# These settings should match your FMQL configuration 
# FMQLEP = "http://www.examplehospital.com/fmqlEP" # Endpoint address
FMQLEP = "http://livevista.caregraf.info/fmqlEP" # Endpoint address of Caregraf demo VistA

# ##############################################################
#
# Return the description of a single patient in the Patient File (2)
#

"""
Patient record of patient 9
"""
def reportPatient9():

    print "\r=== Demographics patient 9 (Chris Jones in CG Demo VistA) ===\r"
    
    query = {"fmql": "DESCRIBE 2-9", "format": "JSON-LD"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    jreply = json.loads(urllib2.urlopen(queryURL).read())
    if jreply.has_key("error"):
        print "Got error %s trying to get definition of PATIENT - exiting" % patientDescription["error"]
        return
        
    # Take out the graph - it is id'ed by the FMQL that led to it which makes tracing easier. It is also dated.
    print
    print "FMQL returned Graph on", jreply["generatedAt"]
    print
    
    printDescriptions(jreply)
        
    print
    print "... note that properties can appear in any order. If you want a logical presentation for a large data type like PATIENT (2), you'll need to define an order, here, in the reporter or use JSON-LD toolchains to reframe the content more logically."
    
"""
Problems of Patient 9

Note: ignoring CONDITION: HIDDEN in this illustration. In real systems, 
these are filtered out.
"""
def reportProblems9():
    """
    Print out the problems of patient 9. This shows how the FMQL graph distinguishes references to terminologies like VA VUIDs or ICD9 from references to other data within a VistA.
    """    
    print "\r=== A report problems (Chris Jones in CG Demo VistA) ===\r"
    
    query = {"fmql": "DESCRIBE 9000011 FILTER(.02=9000001-9)", "format": "JSON-LD"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    jreply = json.loads(urllib2.urlopen(queryURL).read())
    if jreply.has_key("error"):
        print "Got error %s trying to get problems of PATIENT - exiting" % patientDescription["error"]
        return
        
    # Take out the graph - it is id'ed by the FMQL that led to it which makes tracing easier. It is also dated.
    graph = jreply["@graph"]
    print
    print "FMQL returned Graph on", jreply["generatedAt"]
    print
    
    printDescriptions(jreply)
    
    # Let's do some stats ...
    problemDescriptions = jreply["@graph"]
    print
    print "---- Some Numbers ----"    
    print "This patient has", len(problemDescriptions), "problems"
    # Another way to deal with active, an enumeration, is to key off its label: graph[problemDescription["status-9000011"]["label"] == "ACTIVE"
    print "Of which", sum(1 for problemDescription in problemDescriptions if problemDescription["status-9000011"]["id"] == "vs:9000011__12_E-ACTIVE"), "are active"
    # Optional predicate 'service_connnected' - first check if there and then check the value
    print sum(1 for problemDescription in problemDescriptions if "service_connected-9000011" in problemDescription and problemDescription["service_connected-9000011"] == True), "are service connected"
    print "Distinct diagnoses", len(set(problemDescription["diagnosis-9000011"]["id"] for problemDescription in problemDescriptions if "diagnosis-9000011" in problemDescription))
    
    # Let's print the still active diagnoses
    print
    print "---- Active Problems (remember this is just demo data!) ----"
    for i, pd in enumerate([pd for pd in problemDescriptions if pd["status-9000011"]["id"] == "vs:9000011__12_E-ACTIVE"], 1):
        print "\t", i, pd["diagnosis-9000011"]["sameAsLabel"], "-", pd["diagnosis-9000011"]["sameAs"]
        
"""
Vitals of Patient 9 between particular dates

"""       
def reportVitals9():

    print "\r=== A report vitals (Chris Jones in CG Demo VistA) ===\r"

    # Vitals of patient 9 from April 2008 on
    query = {"fmql": "DESCRIBE 120_5 FILTER(.02=2-9&.01>2008-04-01)", "format": "JSON-LD"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    jreply = json.loads(urllib2.urlopen(queryURL).read())
    if jreply.has_key("error"):
        print "Got error %s trying to get vitals of PATIENT - exiting" % patientDescription["error"]
        return

    print "FMQL returned Graph on", jreply["generatedAt"]
    print
   
    printDescriptions(jreply)
    
# ############################ Utility #################

def printDescriptions(jreply):
    """
    Crude - doesn't dive down into contained nodes - should
    """

    # As Graph gives descriptions in an ordered array, the returned entries will reflect the order in the file.
    descriptions = jreply["@graph"]
        
    for i, description in enumerate(descriptions, 1):
        
        print "\r---- Description ", i, "----"
        print "about"
        print "\t", description["label"], "-", description["id"]
        print "type"
        print "\t", description["type"]
        print
    
        for predicate in description:
    
            # We printed the basic properties already
            if predicate in ["id", "type", "label"]:
                continue 
        
            value = description[predicate]
        
            print predicate
        
            # Structured Value: reference or typed literal
            if type(value) == dict:
                # A reference - print its id and label
                if "id" in value:
                    print "\t", value["id"], "-", value["label"]
                    if "sameAs" in value:
                        print "\t\t", value["sameAs"], value["sameAsLabel"]
                # A typed value (date)
                else:
                    print "\t", value["value"], "-", value["type"]
            elif type(value) == list:
                # A list of contained resources: going to be crude. Just printing a label
                for containedDescription in value:
                    print "\t\t", containedDescription["label"], containedDescription["id"]
            # Simple Value
            else:
                print "\t", value
                
# ############################ Using pyld ################
#
# To use json-ld's Python toolkit, install pyld
#

def usePyld():

    try:
        jsonld
    except:
        print "=== can't do pyld demos' as package pyld isn't installed"
        return

    # Grab the vitals
    query = {"fmql": "DESCRIBE 120_5 FILTER(.02=2-9&.01>2008-04-01)", "format": "JSON-LD"}
    queryURL = FMQLEP + "?" + urllib.urlencode(query)
    jreply = json.loads(urllib2.urlopen(queryURL).read())
    json.dump(jreply, open("fmql_FMQL.json", "w"), indent=2)

    # Let's produce different forms of JSON-LD (and RDF) from this 
    
    # 1. Expanded form
    print "pyld expand ..."
    expanded = jsonld.expand(jreply)
    json.dump(expanded, open("pyld_EXP_FMQLEX.json", "w"), indent=2)
    
    # 2. Compact it - using the basic context
    print "pyld compact ..."
    compact = jsonld.compact(jreply, json.load(open("vsfmcontextBase.json")))
    json.dump(compact, open("pyld_COMP_FMQLEX.json", "w"), indent=2)

    # 3. Dump RDF -- only nquads are supported ... others return errors
    print "pyld tordf ..."
    open("pyld_RDF_FMQLEX.rdf", "w").write(jsonld.to_rdf(jreply, {"format": "application/nquads"}))
    
    print
    print "For more JSON to RDF, see the utility 'jldToRDF.py'"
    print
    
# ############################# Demo Driver ####################################

def main():

    reportPatient9()

    reportProblems9()
    
    reportVitals9()
    
    usePyld()

if __name__ == "__main__":
    main()
