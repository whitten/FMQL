#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

import sys, os
import json
import urllib, urllib2
import getopt
import traceback
import time
from datetime import datetime
import cgi
import re

sys.path.append('../Framework')
from brokerRPC import VistARPCConnection

"""
FIX UP FOR FOIA - indexed queries to test things (in place of patient, use meta data):
- 5 County indexed by State C
  SELECT 5_1 FILTER(1=5-6)
- 50_6 ... lookup by VUID ie/ not a pointer but it is indexed.
[county for state too but stick to drugs]

State 5 - has multiple: county = 5.01 ie 5_01 IN 

County 5_1 - indexes state using C: issue - seems empty
"""

"""
 FMQL Query Processor Unit Tests

 Two sorts of test:
 - generic: no assertions particular to a system (count patients)
 - system specific: assertions specific to a system (number of patients is 50000)
 
 To run the system specific tests on your system, you must change
 the constants set at the beginning of the system test section.

 Note: these tests run directly against the FMQL RPC and not the Apache
 resident endpoint.
"""

#
# FMQL Tests [Configure for specific system after first system-only run]
#

SYSTEMONLY = False # Change once system specific numbers below are set

def runTest(rpcc, testGroupName, testId, testDef):
    print "========= Test %s: %s ========" % (testGroupName, testId)
    print testDef["description"]
    start = datetime.now()
    try: 
        reply = rpcc.invokeRPC("CG FMQL QP", [testDef["fmql"]])
        end = datetime.now()
        delta = end-start
        jreply = json.loads(reply)
        end = datetime.now()
        jdelta = end-start
        print "... test took: %s/%s" % (delta, jdelta)
    except Exception as e:
        traceback.print_stack()
        print "ERROR: %s" % e
        print "REPLY", reply
        return 0
    if "error" in testDef:
        if "error" in jreply:
            print "Received Expected Error %s" % str(jreply["error"])
            print "PASSED"
            return 1
        print jreply
        print "ERROR"
        return 0
    if "dump" in testDef:
        print jreply
    if "count" in testDef:
        fmqlCount = "-1"
        if "count" in jreply:
            fmqlCount = jreply["count"]
        elif "total" in jreply:
            fmqlCount = jreply["total"]
        elif "results" in jreply:
            fmqlCount = str(len(jreply["results"]))
        if SYSTEMONLY or testDef["count"] == "PRINT":
            # print jreply
            print "Got count of %s" % fmqlCount
        elif "error" in jreply or fmqlCount != testDef["count"]:
            print jreply
            print "ERROR: COUNT %s doesn't match returned count %s!" % (testDef["count"], fmqlCount)
            return 0
    if "test" in testDef and not SYSTEMONLY:
        try:
            exec testDef["test"]
        except Exception as e:
            testResult = False
            print "Test failed to execute. Reply has unexpected form"
            print e
        if testResult:
            print "PASSED"
            return 1
        print "ERROR"
        return 0
    print "PASSED"
    return 1
    
# Utility to make testing DESCRIBE TYPE easy - comes back as array
def schemaField(jreply, fieldNumber):
    for fieldInfo in jreply["fields"]:
        if fieldInfo["number"] == fieldNumber:
            return fieldInfo
    return None
    
TESTSETS = []

# ####################### Generic/Cross System #############
#
# Tests on basic FMQL that don't depend on System content.
# Will print some counts to allow system specific tests to
# be configured
#

# Generic (Schema etc tests)
GENERICTESTS = {
    "name": "BASIC",
    "definitions": [
        {   
            "description": "Schema: Select Types",
            "fmql": "SELECT TYPES",
        },
        {
            "description": "Schema: DescribeType 2",
            "fmql": "DESCRIBE TYPE 2",
        },
        {
            "description": "Schema: Select Type Refs 2",
            "fmql": "SELECT TYPE REFS 2",
        },
        {
            "description": "Count Patients",
            "fmql": "COUNT 2",
            "count": "PRINT",
        },
        {
            "description": "Count States",
            "fmql": "COUNT 5",
            "count": "PRINT",
        },
        {
            "description": "Count Institution",
            "fmql": "COUNT 4",
            "count": "PRINT",
        },
    ]
}

TESTSETS.append(GENERICTESTS)

# Add:
# - invalid pointer in filter (.01=X and not X-Y)
NEGATIVETESTS = {
    "name": "NEGATIVETESTS - always invalid queries",
    "definitions": [
        {
            "description": "Select: Bad file",
            "fmql": "SELECT 999999999999999999",
            "error": ""
        },
        # Note: not doing typeId only to Describe as QP catches that.
        {
            "description": "Describe: Good file, bad ID",
            "fmql": "DESCRIBE 2-99999999999999999",
            "error": ""
        },
        { 
            "description": "Describe: Good file, 0 (bad) ID (0 can be tricky as it is in the array - as meta data)",
            "fmql": "DESCRIBE 2-0",
            "error": ""
        },
        {
            "description": "Describe: Bad file, bad ID",
            "fmql": "DESCRIBE 999999999999999999-99999999999999999",
            "error": ""
        },
        { 
            "description": "COUNT REFS: Good file, bad ID",
            "fmql": "COUNT REFS %s" % "2-99999999999999999",
            "error": ""
        },
        {
            "description": "COUNT REFS: Bad file, bad ID",
            "fmql": "COUNT REFS %s" % "9999999999999999-99999999999999999",
            "error": ""
        },
        {
            "description": "Select: BNode file",
            "fmql": "SELECT 63_04",
            "error": ""
        },
        {
            "description": "DescribeType: Bad file",
            "fmql": "DESCRIBE TYPE 9999999999999999",
            "error": ""
        },
        {
            "description": "Select Type Refs: Bad file",
            "fmql": "SELECT TYPE REFS 9999999999999999",
            "error": ""
        },
        {
            "description": "Select Type Refs: BNode file",
            "fmql": "SELECT TYPE REFS 63_04",
            "error": ""
        },
    ]
}

TESTSETS.append(NEGATIVETESTS)

########################## Schema Specifics (OSEHRA Test FOIA) ################

SSCHEMASETS = []

# TODO: upgrade to test if FMQL response has BADTOO:true etc
# TODO: add for FILE=.109 ; allow .11 on but no .001 -> .1
TESTSCHEMATESTS = {
    "name": "SELECT TYPES in all its forms",
    "definitions": [
        {
            "description": "SELECT TYPES - top and subs",
            "fmql": "SELECT TYPES", 
            "count": "5810", # FOIA VISTA Mar 2014
        },
        {
            "description": "SELECT TYPES TOPONLY",
            "fmql": "SELECT TYPES TOPONLY",
            "test": "testResult=(len(jreply['results']) == int(jreply['topCount']))" 
        },
        { # ADD distinction of fields bad vs bad files
            "description": "DESCRIBE BADTYPES",
            "fmql": "DESCRIBE BADTYPES",
            "test": "testResult=(len(jreply['results']) == int(jreply['badCount']) and len(jreply['results']) == 20 and sum(1 for badFieldRes in jreply['results'] if badFieldRes.has_key('badfields')) == 3)" 
        },
        {
            "description": "SELECT TYPES POPONLY",
            "fmql": "SELECT TYPES POPONLY",
            "count": "1294"
        },
    ]
}

SSCHEMASETS.append(TESTSCHEMATESTS)

# Boolean 'set of codes' apply fixed maps (Y->true etc) to turn binary and unary valued set of codes into booleans. This cuts down on the number of coded-values/enums needed in a graph or its schema.
BOOLEANCODETESTS = {
    "name": "Boolean Code Tests",
    "definitions": [
        {
            "description": "1:YES;0:NO - 52/10.1",
            "fmql": "DESCRIBE TYPE 52",
            "test": "testResult=(jreply['fields'][12]['type'] == '12')"
        },
        {
            "description": "1:YES - 52/34.1",
            "fmql": "DESCRIBE TYPE 52",
            "test": "testResult=(schemaField(jreply,'34.1')['type'] == '12')"
        },
        {
            "description": "0:NO;1:YES (order different) - 52/116",
            "fmql": "DESCRIBE TYPE 52",
            "test": "testResult=(schemaField(jreply,'116')['type'] == '12')"
        },
        {
            "description": "MALE:FEMALE SEX is Enum not boolean - 2/.02",
            "fmql": "DESCRIBE TYPE 2",
            "test": "testResult=(schemaField(jreply,'.02')['type'] == '3')"
        },
        {
            "description": "YES/NO/UNKNOWN is Enum not boolean - 2/3025",
            "fmql": "DESCRIBE TYPE 2",
            "test": "testResult=(schemaField(jreply,'.3025')['type'] == '3')"
        },
        {
            "description": "1;Yes:0;No (lowers) - 120.86/1",
            "fmql": "DESCRIBE TYPE 120_86",
            "test": "testResult=(schemaField(jreply,'1')['type'] == '12')"
        }
    ]
}

SSCHEMASETS.append(BOOLEANCODETESTS)

TESTSETS.extend(SSCHEMASETS)

# ####################### (Specific) Data Tests ###########################

SDATASETS = []
SDATASETS2 = []

# Set the following per test system
CNT_PATIENTS = "39"
TESTPATIENTID = "2-9"
TESTIHSPATIENTID = "9000001-9"
TESTOPATIENTID = "2-6" # Order patient different in CG Demo
CNT_CHLAB="19" # no of CH labs of test patient (was 38!)
CHLAB_URIFORMAT = "^63_04\-[^_]+_\d+$"
CNT_VITALSOFTP = "290" # no of vitals of test patient
CNT_VITALSOFTPNEIE="260" # Not entered in error
CNT_VITALSFROM2008ON="104" # Vitals from 2008 on
CNT_HVITALSOFTPNEIE="12" # Height Vitals of patient
CNT_ORDERSOFOTP = "5"
CNT_ACCESSIONS = "15"
TESTPROBLEMDIAGNOSIS="80-62"

# OTHERS TO ADD:
# - 80_3-2 ... MUMPS (6) is .01 value. Seems to show properly
# - typeID: ["9002313.55"] has BAD IEN (1VA) in my system. I skip it properly ie. no entries show.

TESTSTATEID = "5-33" # new hampshire
CNT_STATE_REFS = "617"

TESTSTATETESTS = {
    "name": "Ramble Test State %s" % TESTSTATEID,
    "definitions": [
        {
            "description": "Describe %s" % TESTSTATEID,
            "fmql": "DESCRIBE " + TESTSTATEID, 
        },
        {
            "description": "COUNT REFS to %s" % TESTSTATEID,
            "fmql": "COUNT REFS %s" % TESTSTATEID,
            "count": CNT_STATE_REFS,
        },
    ]
}

SDATASETS.append(TESTSTATETESTS)

"""
TODO - 68 doesn't seem to fit FOIA

# Special case: "B" index 2 and 68 overloaded with alias names
# 68 has "SEND","SEND OUT" for same record in B index. Only exercised with >
# as it forces full walk of B
PATIENTALIASTEST = {
    "name": "Ensure don't count aliases of 'B'",
    "definitions": [
        {
            "description": "Patient Alias should be skipped",
            "fmql": "SELECT 68 FILTER(.01>R)",
            "count": "4"
        }
    ]
}

SDATASETS.append(PATIENTALIASTEST)
"""

CNT_COUNTY_CA = "58"

# Note - was 63 - now state 5 (6 == CA). 
# TODO - add back lost: 2 commented out test on URI format for CNode
CNODETESTS = {
    "name": "CNODE",
    "definitions": [
        {
            "description": "Describe all counties in CA",
            "fmql": "DESCRIBE 5_01 IN 5-6",
            "count": CNT_COUNTY_CA
        },
        {
            "description": "Count all counties in CA",
            "fmql": "DESCRIBE 5_01 IN 5-6",
            "count": CNT_COUNTY_CA
        },
        {
            "description": "Select all counties in CA - count em",
            "fmql": "SELECT 5_01 IN 5-6",
            "count": CNT_COUNTY_CA
        },
        {
            "description": "Count all counties in CA, Offset %d" % (int(CNT_COUNTY_CA)-9),
            "fmql": "COUNT 5_01 IN 5-6 OFFSET " + str(int(CNT_COUNTY_CA)-9),
            "count": str(9)
        },
        {
            "description": "Select counties in CA, Limit 10",
            "fmql": "SELECT 5_01 IN 5-6 LIMIT 10",
            "count": "10"
        },
        {
            "description": "Select 2_141 - expect 0",
            "fmql": "SELECT 2_141 IN " + "2-1",
            "count": "0"
        },
        # TODO: find mult in mult in basic setup
        # {
        #    "description": "Select sub sub node 70_03 - expect error",
        #    "fmql": "SELECT 70_03 IN 70-1",
        #    "error": ""
        # },
        {
            "description": "Describe a Node (5-6) without its cnodes",
            "fmql": "DESCRIBE 5-6 CSTOP 0",
            "test": "testResult = (jreply['results'][0]['%s'].has_key('stopped'))" % "county"
        },
        {
            "description": "Describe a Node (5-6) without its sub nodes, setting limit = number of subnodes (limit threshold test)",
            "fmql": "DESCRIBE %s CSTOP %s" % ("5-6", CNT_COUNTY_CA),
            "test": "testResult = (jreply['results'][0]['%s'].has_key('stopped'))" % "county"
        },
        {
            "description": "Describe a Node (5-6) with its sub nodes",
            "fmql": "DESCRIBE %s CSTOP %s" % ("5-6", int(CNT_COUNTY_CA)+1),
            "test": "testResult = ('%s' in jreply['results'][0])" % "county",
        },
        # {
        #    "description": "Stop/Limit CNode within CNode - recursion test",
        #    "fmql": "DESCRIBE %s CSTOP 10" % CTRLUDR32TESTID,
        #    "test": "testResult = ('stopped' in jreply['results'][0]['error_number']['value'][0]['variables_and_data'])"
        # },
        {
            "description": "Describe first 2 C Nodes of a Node (5-6). LIMIT test",
            "fmql": "DESCRIBE 5_01 IN 5-6 LIMIT 2",
            "count": "2"
        },
        {
            "description": "Describe a Node without a CSTOP. 10 (default) is imposed by MUMPS QP",
            "fmql": "DESCRIBE %s" % "5-6",
            "test": "testResult = (jreply['fmql']['CSTOP'] == '10')"
        },
        {
            "description": "CNode - list (ie simple list)/array vs bnode or bnodes",
            "fmql": "DESCRIBE 50_68-8525", # BUPROPION HCL 75MG TAB
            "test": "testResult = (jreply['results'][0]['secondary_va_drug_class']['list'] == True)"
        }
    ]
}

SDATASETS.append(CNODETESTS)

# FILTER TESTS
# use mixture of 5 (state), 5.1 (county) and 50.68 (VA Product) and 2 for indexed time
# - using Counties (5.1) of State (5) as it is indexed (ala Vitals of Patient)
# note: if patient data, use Vitals (120.5)
# - TBD: ] test for names
# - Without index, use SELECT 50_67 FILTER(!bound(7)) LIMIT 10 - also for expired dates

CNT_COUNTIES_ILLINOIS_WITH_SEER_CODE = "29"

FILTERTESTS = {
    "name": "FILTER",
    "definitions": [
        {
            "description": "All of type with specific field value - counties with state Utah (note: state field is indexed)",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49)", # Note: index is corrupt as count state illinois (17) doesn't work
            "count": "29"
        },
        {
            "description": "All of type with a value not bound - counties with no seer county code",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49&!bound(2))",
            "count": "0" # all have 'seer county code' - alaska's (2) 2 don't
        },
        {
            "description": "All of type with a value bound - counties with seer county code",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49&bound(2))",
            "count": CNT_COUNTIES_ILLINOIS_WITH_SEER_CODE # all of them 
        },
        {
            "description": "All of type with a bound value and a field with a value - utah county with seer and name washington",
            "fmql": "SELECT 5_1 FILTER(1=5-49&bound(2)&.01='WASHINGTON')",
            "count": "1" # very precise so maybe not best test but works
        },
        {
            "description": "Filtered node with field value > letter - utah counties with names beginning with E on",
            "fmql": "COUNT 5_1 FILTER(1=5-49&&.01>E)",
            "count": "22"
        },
        {
            "description": "Nodes without bound CNODE - VA Drug product with secondary class",
            "fmql": "COUNT 50_68 FILTER(bound(16))",
            "count": "13" # only 13
        },
        {
            "description": "Nodes with bound CNODE",
            "fmql": "COUNT 50_68 FILTER(!bound(16))",
            "count": "23682"
        },
        {
            "description": "Contained Node with specific field value - of three active ings in va product, only one has this value",
            "fmql": "DESCRIBE 50_6814 IN 50_68-18 FILTER(1='2')",
            "count": "1"
        },      
        {
            "description": "Contained Node with bound value - cook county is containment area, only county in illinois",
            "fmql": "COUNT 5_01 IN 5-17 FILTER(bound(3))",
            "count": "1"
        },  
        { # TODO - should raise error
            "description": "Bad filter (no such field)",
            "fmql": "SELECT 5 FILTER(.999999=X)",
            "count": "0"
        },
        {
            "description": "Day filter - patient born after a day (born the next day)",
            "fmql": "SELECT 2 FILTER(.03>1969-07-07)",
            "count": "1"
        }, # TODO: add test for time of day ie/ TX...
        {
            "description": "Filter a year - patient born between 1960 and 1970",
            "fmql": "SELECT 2 FILTER(.03>1940&&.03<1960)",
            "count": "13"
        },
        {
            "description": "Filter with embedded bracket - name of Armed Forces 'state'",
            "fmql": "DESCRIBE 5 FILTER(.01='ARMED FORCES AMER (EXC CANADA)')",
            "count": "1",
        },
        {
            "description": "Filter with embedded & - must escape &.",
            "fmql": "COUNT 50_68 FILTER(.01='MENINGOCOCCAL POLYSACCHARIDE VACCINE GROUPS A \& C COMB. INJ')",
            "count": "2"
        }, # TODO: get a \\ ie/ escape escape?
        { 
            "description": "Filter with embedded ( and & - only escape & - FMQL TODO",
            "fmql": "COUNT 50_68 FILTER(.01='INFLUENZA VIRUS VACCINE,TRIVALENT TYPES A\&B ('90-'91) INJ')",
            "count": "1"
        },
        {
            "description": "Filter with embedded /",
            "fmql": "COUNT 50_68 FILTER(.01='ATROPINE SO4 2MG/0.7ML INJ')",
            "count": "1"
        }
    ]
}

SDATASETS.append(FILTERTESTS)

# LIMIT and OFFSET TESTS (When Patients, used Vitals, Counties of state is similar)
LIMITTESTS = {
    "name": "LIMIT/OFFSET",
    "definitions": [
        {
            "description": "LIMIT filtered list of type - Counties",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49) LIMIT 5",
            "count": "5"
        },
        {
            "description": "LIMIT filtered list of type, offset - Counties",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49) LIMIT 5 OFFSET 3",
            "count": "5"
        },
        {
            "description": "Filtered list of type, offset one off end",
            "fmql": "DESCRIBE 5_1 FILTER(1=5-49&bound(2)) OFFSET " + str(int(CNT_COUNTIES_ILLINOIS_WITH_SEER_CODE)-1),
            "count": "1"
        },
        {
            "description": "Filtered list of type, offset 1 past end",
            "fmql": "SELECT 5_1 FILTER(1=5-49&bound(2)) OFFSET " + str(int(CNT_COUNTIES_ILLINOIS_WITH_SEER_CODE)+1),
            "count": "0"
        },
        {
            "description": "Count filtered list of type, offset 5",
            "fmql": "SELECT 5_1 FILTER(1=5-49&bound(2)) OFFSET 5",
            "count": str(int(CNT_COUNTIES_ILLINOIS_WITH_SEER_CODE)-5)
        },
        {
            "description": "SELECT filtered list of type limit 2 offset 3",
            "fmql": "SELECT 5_1 FILTER(1=5-49) LIMIT 2 OFFSET 5",
            "count": "2"
        },
    ]
}

SDATASETS.append(LIMITTESTS)

# AFTERIEN - alternative to OFFSET 
AFTERIENTESTS = {
    "name": "AFTERIEN",
    "definitions": [
        {
            "description": "Counties of state, AFTER 6th last IEN",
            "fmql": "SELECT 5_1 FILTER(1=5-49) AFTERIEN 249",
            "count": "5"
        },
        {
            "description": "Counties of state, AFTER 6th last IEN, limit 2",
            "fmql": "SELECT 5_1 FILTER(1=5-49) AFTERIEN 249 LIMIT 2",
            "count": "2"
        },
        {
            "description": "Counties of state, AFTER 6th last IEN, set OFFSET but expect it to be reset",
            "fmql": "SELECT 5_1 FILTER(1=5-49) AFTERIEN 249 OFFSET 22",
            "test": "testResult=(jreply['fmql']['OFFSET']=='0')"
        },
        {
            "description": "Counties of state, AFTER last IEN, expect no entries",
            "fmql": "SELECT 5_1 FILTER(1=5-49) AFTERIEN 254", # last IEN in set is 254
            "count": "0"
        },
    ]
}

SDATASETS.append(AFTERIENTESTS)

"""
TODO: find similar date issue outside HL7

FORMATTESTS = {
    "name": "Format (date) tests",
    "definitions": [
        {
            "description": "Date with trailing '1' which is rendered as 1, not 01 in FileMan's internal format. Is it flipped in the XML?", 
            "fmql": "DESCRIBE 79_3-14",
            "test": "testResult = (jreply['results'][0]['exception_date_time']['value']=='2005-04-18T08:06:01Z')",
        },
    ]
}

SDATASETS.append(FORMATTESTS)
"""

# No Index Max tests. There is a limit on filtered selects when the filter doesn't assert
# the value of an index.
# 
# Note: TBD: may change definition of no index max so an error is returned.
#
NOIDXMXTESTS = {
    "name": "No Index MX tests - using VA Product with non indexed links",
    "definitions": [
        {
            "description": "No Index Max set to 1 - there are many more to count", 
            "fmql": "COUNT 50_68 FILTER(.05=11-2) NOIDXMAX 1",
            "count": "-1"
        },
        {
            "description": "No index max set to 1 but only one in the list (there are 39). Filter on name. Expect result of 1 as .01 name is indexed.", 
            "fmql": "COUNT 50_68 FILTER(.01='ATROPINE SO4 0.4MG TAB') NOIDXMAX 1",
            "count": "1"
        },
        # {
        #    "description": "No index max for array kicks in for < filter, even on an idx (won't for follow on > filter ie/ < can't use indexes",
        #    "fmql": "SELECT 79_3 FILTER(.01<2005-10) NOIDXMAX 10",
        #    "count": "-1"
        # },
        # {
        #    "description": "No index max doesn't matter for indexed > filter. Will pull index",
        #    "fmql": "SELECT 79_3 FILTER(.01>2005-10) NOIDXMAX 10",
        #    "test": "testResult= (len(jreply['results']) > 0)"
        # },
    ]
}

SDATASETS.append(NOIDXMXTESTS)

# ORDER BY and No B IDX (means can't order) tests
"""
Note - shows order by is too index dependent and that either it should fail if no index OR make one

TODO: to be "bullet proof", move off patients
"""
ORDERBYTESTS = {
    "name": "No B Index tests",
    "definitions": [
        {
            "description": "2 with ORDER BY (Z should be last)",
            "fmql": "SELECT 2 ORDERBY .01",
            "test": "testResult = (jreply['results'][len(jreply['results'])-1]['uri']['value']=='2-11')",
        },
        {
            "description": "2 without ORDER BY (Z should be first)",
            "fmql": "SELECT 2",
            "test": "testResult = (jreply['results'][len(jreply['results'])-1]['uri']['value']=='2-25')",
        },
        {
            "description": "8985_1 has no simple, inline B Index. It can be listed but the list is in IEN order. Note: ICD Diagnosis 80 has no B Index either!",
            "fmql": "SELECT 8985_1 ORDERBY .01",
            "test": "testResult = (jreply['results'][0]['uri']['value']=='8985_1-1')",
        }
    ]
}

SDATASETS.append(ORDERBYTESTS)

SELECTPREDTESTS = {
    "name": "Select Field/Pred Tests",
    "definitions": [
        {
            "description": "Select field for 10 records",
            "fmql": "SELECT 50_68 FIELD .05 LIMIT 10",
            "test": "testResult = (jreply['count']=='10' and 'va_generic_name' in jreply['results'][0])",
        },
    ]
}

SDATASETS.append(SELECTPREDTESTS)

"""
# Special for Orders - TODO: need to add to base FOIA as this is special
ORDERTESTS = {
    "name": "ORDERS",
    "definitions": [
        {
            "description": "Select orders, filtered. Test if hard coded filter works by setting noidxmx low",
            "fmql": "SELECT 100 FILTER(.02=" + TESTOPATIENTID + ") NOIDXMAX 1",
            "count": CNT_ORDERSOFOTP
        },
        {
            "description": "Describe orders, filtered. Same as Select but Describe",
            "fmql": "DESCRIBE 100 FILTER(.02=" + TESTOPATIENTID + ")",
            "count": CNT_ORDERSOFOTP
        },
        {
            "description": "Count orders - filtered on non-index, noidxmx low. Should be refused.", 
            "fmql": "COUNT 100 FILTER(1=200-1) NOIDXMAX 1",
            "count": "-1"
        }
    ]
}

SDATASETS.append(ORDERTESTS)
"""

"""
Two Provider Narrative 9999999_27 tests fixed with arrays
until fill arrays in test system. Test where narrative maps
and where it doesn't. By side effect, testing when 757_01 maps
and when it doesn't.

HPTC: problem of many not in kgraf - Osteopath/207P00000X is but many others are not! ... find out range match and range not.

OPEN>S ^AUTNPOV(352,0)="Chronic Fatigue Syndrome^"
OPEN>S ^AUTNPOV(352,757)=304659    

OPEN>S ^AUTNPOV(11,0)="HAIR TOO LONG^" 

OPEN>S SAMEAS("URI")="LOCAL"

OPEN>D RESOLVE9999999dot27^FMQLSSAM(11,.SAMEAS) 

OPEN>ZWR SAMEAS
SAMEAS("URI")="LOCAL"

OPEN>D RESOLVE9999999dot27^FMQLSSAM(352,.SAMEAS)

OPEN>ZWR SAMEAS
SAMEAS("LABEL")="Fatigue Syndrome, Chronic"
SAMEAS("URI")="VA:757-9335"
"""
# SAMEAS Tests
SAMEASTESTS = {
    "name": "SAMEAS",
    "definitions": [
        {
            "description": "VUID FILE: most vitals have VUIDs. Picking Vital (AB Girth 10)", 
            "fmql": "DESCRIBE 120_51-10",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] != 'LOCAL')",
        },
        {
            "description": "VUID FILE: most GMR Allergies have VUIDs", 
            "fmql": "DESCRIBE 120_82-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] != 'LOCAL')",
        },
        {
            "description": "VUID FILE: GMR Allergy with no VUID (only a few)",
            "fmql": "DESCRIBE 120_82-1558",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOCAL')",
        },
        {
            "description": "ICD (same goes for CPT 81).",
            "fmql": "DESCRIBE 80-1",
            "test": "testResult = (('ICD9CM:' + jreply['results'][0]['code_number']['value']) == jreply['results'][0]['uri']['sameAs'])",
        },
        {
            "description": "Major Concept sameas out",
            "fmql": "DESCRIBE 757-9335",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:757-9335')",
        },
        {
            "description": "Expression to Major Concept",
            "fmql": "DESCRIBE 757_01-304659",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:757-9335')",
        },
        {
            "description": "Provider Narrative with Expression to major concept",
            "fmql": "DESCRIBE 9999999_27-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:757-63679')",
        },
        {
            "description": "Provider Narrative with no expression, marked local",
            "fmql": "DESCRIBE 9999999_27-8",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOCAL')",
        },        
        {
            "description": "VUID FILE: VA Generic (50_6) has VUID. Same goes for 50_68, 50_416, 50_605",
            "fmql": "DESCRIBE 50_6-95",
            "test": "testResult = (('VA:' + jreply['results'][0]['vuid']['value']) == jreply['results'][0]['uri']['sameAs'])",
        },
        # 50 and 50_7 not in default base OSEHRA FOIA
        # {
        #    "description": "Drug (50) with map",
        #    "fmql": "DESCRIBE 50-1",
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:4000987')",
        # },
        # {
        #    "description": "Drug (50) local only",
        #    "fmql": "DESCRIBE 50-9",
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOCAL')",
        # },          
        # {
        #    "description": "Pharmacy Orderable (50.7) with map",
        #    "fmql": "DESCRIBE 50_7-1",
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:4000987')",
        # },
        # {
        #    "description": "Pharmacy Orderable (50.7) maps to 50 but it is local only so get LOCAL:50-X",
        #    "fmql": "DESCRIBE 50_7-7",
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOCAL:50-9')",
        # }, 
        #   For these labs manually fixed 60 and 64 as follows:
        #   - S ^LAB(60,5721,64)=666 (WBC)
        #   - S ^LAB(60,5482,64)=1918 and S ^LAM(1918,9)="787^^^^" (MCV)
        #
        # Default 60 lacks any links - TODO: add
        # 
        # {
        #    "description": "Lab 60 to WKLD 64 and no further",
        #    "fmql": "DESCRIBE 60-5721", # WBC
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'VA:wkld85030')",
        # }, 
        # {
        #    "description": "Lab 60 all the way to LOINC",
        #    "fmql": "DESCRIBE 60-5482",
        #    "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOINC:787-2')",
        # }, 
        # Add: 50_7 LOCAL ONLY (ie/ no 50)
        # Add: 71, 790_2 - needs CPT filled. Browse of system with CPT works V0.9
    ]
}

SDATASETS.append(SAMEASTESTS)

#
# NOTE: \x and \u interchange for <255
#
# jdata = {"stuff": "some   other   " + unichr(234)}
# in = json.dumps(jdata)
# ... {"stuff": "some\tother\t\u00ea"} <--- serialized string 
# out = json.loads(in)
# ... {u'stuff': u'some\tother\t\xea'} <--- printed python
# print out["stuff"]==jdata["stuff"]
# ... True
# x = u'\xea' y = u'\u00ea'
# print x==y <-------- down just to the character
# ... True 
# print re.search(r'\xea', y) - matches but re.search(r'\u00ea' doesn't
# as it seems to be treated as a string sequence.
# Go beyond 255 and \x is gone, \u is in.
# print repr(unichr(255)) print repr(unichr(256))
# ... u'\xff' ... u'\u0100'
#
# TBD: more arabic tests ...
# X = "{\"test\": \"\ud8a7\uD984\uD8B3\uD8A7\uD985\uD8A7\uD984\uD987\uD8A7\uD8A8\uD98A\uD984\u01FF\"}"
# are valid unicode (surrogates) but flipped by loads. Need to work out why GT/M exports surrogates.
#

MUMPSCODETESTID = "68-11"
MUMPSCODETEST="(re.match(r'I \$P\(\^LRO\(68,LRAA,1,LRAD,1,LRAN,0\),U,2\)=62.3 S LRTEST=\"\" D \^LRMRSHRT', jreply['results'][0]['ver_code']['value']))"
CTRLUDR32TESTID="3_075-60698" # Note in FOIA VISTA - need substitute TODO
# Escape Decimal 27 == u'\x1b'
CTRLUDR32TEST="re.search(r'\x1b', jreply['results'][0]['error_number']['value'][1]['variables_and_data']['value'][88]['data_value']['value'])"

CHARACTERTESTS = {
    "name": "Character set tests - make sure proper data is transferred",
    "definitions": [
        {
            "description": "MUMPS code",
            "fmql": "DESCRIBE " + MUMPSCODETESTID,
            "test": "testResult = " + MUMPSCODETEST
        },
        # CTRL < 32 TEST (3_075-60698)
        # Escape Decimal 27 == u'\x1b'
        # print jreply['results'][0]["error_number"]["value"][1]["variables_and_data"]["value"][88]["data_value"]
        # NOTE: need CSTOP as relying on a large set of bnodes
        # {
        #    "description": "CTRL Chars < 32 rendered as \u00XX",
        #    "fmql": "DESCRIBE %s CSTOP 500" % CTRLUDR32TESTID,
        #     "test": "testResult = " + CTRLUDR32TEST
        # }
        # \" TEST (... see in 3_075 too)

        # \\\ TEST ?
    
        # \r\n\... < 32 TEST .... try find in 8925. \t too

        # >127 TEST
    ]
}

SDATASETS.append(CHARACTERTESTS)

# . IEN tests. This is to support date stamps mainly but the test
# system doesn't have date stamped (labs) IENs yet. For now, test
# with _11 which has some.
DOTIENTESTS = {
    "name": "Dot IEN Tests",
    "definitions": [
        {
            "description": "Basic describe of element with . IEN",
            "fmql": "DESCRIBE _11-.1102",
            "test": "testResult=(jreply['results'][0]['uri']['value']=='_11-.1102')"
        },
        {
            "description": "Select first 5 of file with . IEN",
            "fmql": "SELECT _11 LIMIT 5",
            "test": "testResult=((jreply['count']=='5') and (jreply['results'][0]['uri']['value']=='_11-.001'))",
        },
        {
            "description": "Describe record with .IEN CSTOP",
            "fmql": "DESCRIBE _11-.1102 CSTOP 0",
            "test": "testResult=('stopped' in jreply['results'][0]['crossreference_values'])",
        },
        {
            "description": "Count CNodes inside a .IEN'ed record",
            "fmql": "COUNT _114 IN _11-.1102",
            "count": "1",
        }
    ]
}

SDATASETS.append(DOTIENTESTS)

# .001's are special - reaches into Schema view displaying them and data where value is a pointer or a date. Current test system has no example of the pointer form which is in three places in C***'s Lab schema.
# SELECT 50_6 FIELD .001 LIMIT 10
OO1IENTESTS = {
    "name": ".001 IEN Tests",
    "definitions": [
        {
            "description": "SCHEMA 50_6 - .001 shows up",
            "fmql": "DESCRIBE TYPE 50_6",
            "test": "testResult=(jreply['fields'][0]['number'] == '.001')"
        },
        {
            "description": "DATA 50_6-95 - .001 field is numeric and ignored",
            "fmql": "DESCRIBE 50_6-95",
            "test": "testResult=('number' not in jreply['results'][0])",
        },
        { # 2.98 may be better candidate if fill in scheduling
            "description": "SCHEMA 3_07 - .001 field is Date",
            "fmql": "DESCRIBE TYPE 3_07",
            "test": "testResult=((jreply['fields'][0]['flags'] == 'D') and (jreply['fields'][0]['number'] == '.001'))",
        },
        {
            "description": "DATA 3_07 - .001 field is Date exposed",
            "fmql": "DESCRIBE 3_07 LIMIT 1",
            "test": "testResult=(('date_time' in jreply['results'][0]) and (jreply['results'][0]['date_time']['fmType'] == '1'))",
        },
        {
            "description": "DATA 3_07 - even its .001 can't be isolated in SELECT FROM",
            "fmql": "SELECT 3_07 FIELD .001 LIMIT 2",
            "test": "testResult=('date_time' not in jreply['results'][0])",
        },
        {
            "description": "Schema 2006_552 - .001 is a pointer",
            "fmql": "DESCRIBE TYPE 2006_552",
            "test": "testResult=(jreply['fields'][0]['details'] == '2')",
        }
        # TODO: add ex of this pointer being resolved. Only one in FOIA - not yet populated in test system. 2006_552
        
    ]
}

SDATASETS.append(OO1IENTESTS)

TESTSETS.extend(SDATASETS)

####################### Explicit Tests ####################

def orderPatientReturnTest(qp):
    fileId = "2"
    print "Going to ensure returns file %s in order" % fileId
    reply = qp.processQuery("SELECT " + fileId)
    jreply = json.loads(reply)
    print "Checking %d results" % len(jreply["results"])
    lastResult = None
    passed = True
    for result in jreply["results"]:
        if lastResult and lastResult["uri"]["label"] > result["uri"]["label"]:
            print "ORDER PROBLEM: %s (%s) after %s (%s)" % (result["uri"]["value"], result["uri"]["label"], lastResult["uri"]["value"], lastResult["uri"]["label"])
            passed = False
        lastResult = result
    print "---- done: passed - %s ----" % passed

# Caused problems when FMQL used GETS: URL: 790_2-28

# ##########################################################################

class DefaultLogger:
    def __init__(self):
        pass
    def logInfo(self, tag, msg):
        # self.__log(tag, msg)
        pass
    def logError(self, tag, msg):
        self.__log(tag, msg)
    def __log(self, tag, msg):
        print "Test QP -- %s %s" % (tag, msg)

def main():
 
    opts, args = getopt.getopt(sys.argv[1:], "")
    if len(args) < 5:
        print "Enter <host> <port> <access> <verify> (access/verify for FMQL RPC) <fmql host>"
        print "Ex: localhost 9201 'QLFM1234' 'QLFM1234!!' http://www.examplehospital.com/fmqlEP"
        return

    fmqlEP = args[4] + "/fmqlEP"

    try:
        rpcc = VistARPCConnection(args[0], int(args[1]), args[2], args[3], "CG FMQL QP USER", DefaultLogger())
    except Exception as e:
        print "Failed to log in to VistA (bad parameters?): %s ... exiting" % e
        return

    times = []

    try:

        fails = 0
        total = 0
        testNo = 0
        stopOnFail = True
        for i, testSet in enumerate(TESTSETS, 1):
            for j, testDef in enumerate(testSet["definitions"], 1):
                total += 1
                if not runTest(rpcc, testSet["name"], str(i) + ":" + str(j), testDef):
                    fails += 1
                    if stopOnFail:
                        break
        print "=== All Done: %d of %d failed ===" % (fails, total)

    except Exception as e:
        print "Exception %s" % e
        traceback.print_exc()

if __name__ == "__main__":
    main()
