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
import getopt
import traceback
import time
from datetime import datetime
import cgi
import re

from testQP import runTest, schemaField

sys.path.append('../Framework')
from fmqlQP import FMQLQP
from cacheObjectInterface import CacheObjectInterface

"""
Unit tests for FMQL running on C***
"""

CTESTSETS = []

"""
90 of 2408 files have .001 - 9 are Date (D) or Pointer (P)
1 3_081 .001 Date/Time S %DT="ETX" D ^%DT S X=Y K:Y<1 X D 
2 19_41 .001 Date/Time S %DT="ETXR" D ^%DT S X=Y K:Y<1 X D 
3 19_42 .001 Date/Time S %DT="ETXR" D ^%DT S X=Y K:Y<1 X DX 
4 19_44 .001 Entry Time S %DT="ESTXR" D ^%DT S X=Y K:Y<1 X D 
5 19_45 .001 Entry Time S %DT="ESTX" D ^%DT S X=Y K:Y<1 X D 
6 19_5 .001 Start Date/Time S %DT="ESTXR" D ^%DT S X=Y K:Y<1 X D 
7 68_32 .001 Test  P60' 
8 63_794 .001 Lab Test Processed  P60' 
9 63_07 .001 Test  P60' 
"""
OO1IENTESTS = {
    "name": ".001 IEN Tests",
    "definitions": [
        {
            "description": "3_081 - .001/IEN is Date",
            "fmql": "DESCRIBE 3_081 LIMIT 1",
            "test": "testResult=('date_time' in jreply['results'][0])"
        },
        {
            "description": "63_07 - .001/IEN is Lab 60",
            "fmql": "DESCRIBE 63_04 IN 63-1094787 LIMIT 1 CSTOP 100",
            "test": "testResult=('test' in jreply['results'][0]['result']['value'][0])"
        }
    ]
}

CTESTSETS.append(OO1IENTESTS)

SAMEASTESTS = {
    "name": "SAMEAS",
    "definitions": [
        {
            "description": "ICD9CM SAMEAS 80", 
            "fmql": "DESCRIBE 80-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'ICD9CM:100.81')",
        },
        {
            "description": "ICD9CM (Procedure) SAMEAS 80_1", 
            "fmql": "DESCRIBE 80_1-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'ICD9CM:10.1')",
        },
        {
            "description": "LOINC SAMEAS 8188", 
            "fmql": "DESCRIBE 8188-1",
            # label: ACYCLOVIR SUSC ISLT
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'LOINC:1-8')",
        },
        {
            "description": "NDC SAMEAS 8252", 
            "fmql": "DESCRIBE 8252-2026002",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'NDC:00002026002')",
        },
        {
            "description": "HICL/INS SAMEAS 8250", 
            "fmql": "DESCRIBE 8250-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'NDDF:ins000001')",
        },
        {
            "description": "HIC/IN SAMEAS 8250_1", 
            "fmql": "DESCRIBE 8250_1-118",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'NDDF:in000118')",
        },
        {
            "description": "Allergy Selection SAMEAS 8254_01", 
            "fmql": "DESCRIBE 8254_01-7174001",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'NDDF:ins025025')",
        },
        {
            "description": "Allergy Selections - no map DAC",
            "fmql": "DESCRIBE 8254_01-1000",
            "test": "testResult = ('sameAs' not in jreply['results'][0]['uri'])",
        },
        {
            "description": "Allergy Selections LOCAL - CDC id'ed is invalid",
            "fmql": "DESCRIBE 8254_01-1160",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs']=='LOCAL')",
        },
        {
            "description": "NDDF/CDC SAMEAS 50", 
            "fmql": "DESCRIBE 50-1",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs'] == 'NDDF:cdc004489')",
        },
        {
            # Has NDCs in a multiple - only one. Appears to be a config bug
            "description": "50-19 - no primary NDC ie/ LOCAL", 
            "fmql": "DESCRIBE 50-19",
            "test": "testResult = (jreply['results'][0]['uri']['sameAs']=='LOCAL')",
        },
    ]
}

CTESTSETS.append(SAMEASTESTS)

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
        
FMQLEP = "http://10.255.167.116:57772/csp/fmquery/FMQL.csp"

def main():
 
    try:
        coi = CacheObjectInterface(FMQLEP)
        logger = DefaultLogger()
        fmqlQP = FMQLQP(coi, logger)
    except Exception as e:
        print "Failed to log in to C***: %s ... exiting" % e
        return
        
    times = []

    try:

        fails = 0
        total = 0
        testNo = 0
        stopOnFail = True
        for i, testSet in enumerate(CTESTSETS, 1):
            for j, testDef in enumerate(testSet["definitions"], 1):
                total += 1
                if not runTest(fmqlQP, testSet["name"], str(i) + ":" + str(j), testDef):
                    fails += 1
                    if stopOnFail:
                        break
        print "=== All Done: %d of %d failed ===" % (fails, total)

    except Exception as e:
        print "Exception %s" % e
        traceback.print_exc()

if __name__ == "__main__":
    main()
