#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2012 caregraf.org
#

"""
 FMQL Query Processor Enhanced

 Query data graph using SPARQL-like query format. Returns JSON.
 query format.
 
"""

import urllib
import re
import json

__author__ =  'Caregraf'
__copyright__ = "Copyright 2011, Caregraf"
__license__ = "AGPL"
__version__=  '0.96'
__status__ = "Development"

class FMQLQPE:

    """ Dress the V0.8 query processor to provide V0.9 functionality"""
    def __init__(self, v08qp, logger):
        self.v08qp = v08qp
        self.logger = logger
        
    def processQuery(self, queryArgs):
        """
        Expect query and then qualifiers for return format

        Note: will exception for internal errors.
        """
        if "fmql" in queryArgs:
            query = queryArgs["fmql"][0] # for html
            nqueryArgs = queryArgs
            queryArgs = self.__toV08QueryArgs(nqueryArgs["fmql"][0]) 
            if "output" in nqueryArgs:
                queryArgs["output"] = nqueryArgs["output"]
        # This call will check valid op etc.
        reply = self.v08qp.processQuery(queryArgs)

        return reply

    # TBD: replace the v08 delegate with direct RPC call
    def __toV08QueryArgs(self, query):
        v08Query = self.__toV08SchemaQueryArgs(query)
        if v08Query:
            return v08Query
        RE_DESCRIBENODE = re.compile('DESCRIBE +([\d_]+\-[\d\.]+)')
        RE_DESCRIBENODES = re.compile('DESCRIBE +([\d_]+)')
        RE_SELECT = re.compile('SELECT +([\d_]+)')
        RE_COUNT = re.compile('COUNT +([\d_]+)')
        RE_COUNTREFS = re.compile('COUNT +REFS +([\d_]+\-[\d\.]+)')
        RE_IN = re.compile(' +IN +([\d_]+\-[\d\.]+)')
        RE_LIMIT = re.compile(' +LIMIT +(\d+)')
        RE_OFFSET = re.compile(' +OFFSET +(\d+)')
        RE_AFTERIEN = re.compile(' +AFTERIEN +(\d+)')
        RE_FILTER = re.compile(' +FILTER *\((.+)\)')
        RE_DESCRIBECSTOP = re.compile(' +CSTOP (\d+)')
        RE_SELECTFROM = re.compile('SELECT +([\.\d]+) +FROM +([\d_]+)')
        RE_NOIDXMAX = re.compile('NOIDXMAX +(\d+)')
        RE_ORDERBY = re.compile('ORDERBY +([\.\d]+)')
        if RE_DESCRIBENODE.match(query):
            nodeId = RE_DESCRIBENODE.match(query).group(1)
            v08Query = {"op": ["Describe"], "url": [nodeId]}
            if RE_DESCRIBECSTOP.search(query):
                v08Query["cstop"] = [RE_DESCRIBECSTOP.search(query).group(1)]
            return v08Query
        if RE_DESCRIBENODES.match(query):
            nodeType = RE_DESCRIBENODES.match(query).group(1)
            v08Query = {"op": ["Describe"], "typeId": [nodeType]}
            if RE_DESCRIBECSTOP.search(query):
                v08Query["cstop"] = [RE_DESCRIBECSTOP.search(query).group(1)]
        if RE_SELECTFROM.match(query):
            pred = RE_SELECTFROM.match(query).group(1)
            nodeType = RE_SELECTFROM.match(query).group(2)
            v08Query = {"op": ["Select"], "typeId": [nodeType], "predicate": [pred]}
        if RE_SELECT.match(query):
            nodeType = RE_SELECT.match(query).group(1)
            v08Query = {"op": ["Select"], "typeId": [nodeType]}
        if RE_COUNT.match(query):
            nodeType = RE_COUNT.match(query).group(1)
            v08Query = {"op": ["Count"], "typeId": [nodeType]}
        if RE_COUNTREFS.match(query):
            nodeType = RE_COUNTREFS.match(query).group(1)
            v08Query = {"op": ["COUNT REFS"], "url": [nodeType]}
        # TODO: too aggressive as .12 will lead to this etc ie/ . form won't work!
        if not v08Query:
            raise Exception("QPERROR", "No Such Query Type")
        if RE_IN.search(query):
            v08Query["in"] = [RE_IN.search(query).group(1)]
        if RE_LIMIT.search(query):
            v08Query["limit"] = [RE_LIMIT.search(query).group(1)]
        if RE_OFFSET.search(query):
            v08Query["offset"] = [RE_OFFSET.search(query).group(1)]
        if RE_OFFSET.search(query):
            v08Query["afterien"] = [RE_AFTERIEN.search(query).group(1)]
        if RE_FILTER.search(query):
            v08Query["filter"] = [RE_FILTER.search(query).group(1)]
        if RE_NOIDXMAX.search(query):
            v08Query["noidxmx"] = [RE_NOIDXMAX.search(query).group(1)]
        if RE_ORDERBY.search(query):
            v08Query["orderby"] = [RE_ORDERBY.search(query).group(1)]
        return v08Query

    def __toV08SchemaQueryArgs(self, query):
        v08Query = None
        RE_DESCRIBETYPE = re.compile('DESCRIBE TYPE +([\d_]+)$')
        RE_SELECTTYPES = re.compile('SELECT TYPES') # allows args
        # TODO: change to SELECT TYPE REFS (vs COUNT REFS)
        RE_SELECTALLREFERRERSTOTYPE = re.compile('SELECTALLREFERRERSTOTYPE +([\d_]+)')
        if RE_DESCRIBETYPE.match(query):
            nodeType = RE_DESCRIBETYPE.match(query).group(1)
            v08Query = {"op": ["DescribeType"], "typeId": [nodeType]}
        elif RE_SELECTTYPES.match(query):
            v08Query = {"op": ["SelectAllTypes"]}
            if re.search(r'TOPONLY', query):
                v08Query["toponly"] = ["1"]
            if re.search(r'BADTOO', query):
                v08Query["badtoo"] = ["1"]
        elif RE_SELECTALLREFERRERSTOTYPE.match(query):
            nodeType = RE_SELECTALLREFERRERSTOTYPE.match(query).group(1)
            v08Query = {"op": ["SelectAllReferrersToType"], "typeId": [nodeType]}
        return v08Query
        
# ################################## TEST ############################
        
import sys
import getopt
import traceback
from brokerRPC import VistARPCConnection
from fmqlQP import FMQLQueryProcessor, DefaultLogger

# Simple test: get all patients, print out
def main():
    opts, args = getopt.getopt(sys.argv[1:], "")
    if len(args) < 4:
        print "Enter <host> <port> <access> <verify> (access/verify for FMQL RPC)"
        return

    try:
        rpcc = VistARPCConnection(args[0], int(args[1]), args[2], args[3], "CG FMQL QP USER", DefaultLogger())
    except Exception as e:
        print "Failed to log in to VistA for FMQL RPC (bad parameters?): %s ... exiting" % e
        return

    logger = DefaultLogger()
    qp = FMQLQueryProcessor(rpcc, logger)
    qpe = FMQLQPE(qp, logger)
    
    queries = ["DESCRIBE 2-1", "DESCRIBE 2", "SELECT 2", "COUNT 2", "DESCRIBE 2_0361 IN 2-3", "SELECT 2 LIMIT 3 OFFSET 1", "SELECT .01 FROM 2 LIMIT 3", "DESCRIBE 79_3 FILTER(.03=2-1)", "COUNT 50_68 FILTER(.05=11-2) NOIDXMAX 1"]
    for query in queries:
        print "=========================================================="
        reply = qpe.processQuery({"fmql": [query]})
        # jreply = json.loads(reply["data"])
        print reply

if __name__ == "__main__":
    main()
