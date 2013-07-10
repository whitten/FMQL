#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

"""
 FMQL Query Processor Enhanced

 Query data graph using SPARQL-like query format. Returns JSON.
 query format.
 
"""

import urllib
import re
import json

class FMQLQPE:

    def __init__(self, rpcc, logger):
        self.rpcc = rpcc
        self.logger = logger
        
    def processQuery(self, queryArgs):
        """
        Expect query and then qualifiers for return format

        Note: will exception for internal errors.
        """
        if "fmql" not in queryArgs:
            raise Exception("Expect fmql=")
        query = queryArgs["fmql"]
        fmqlArgs = self.__schemaQueryToFMQLArgs(query)
        if fmqlArgs:
            return fmqlArgs
        fmqlArgs self.__dataQueryToFMQLArgs(query)
        reply = self.rpcc.invokeRPC("CG FMQL QP", [fmqlArgs])
        return reply        

    def __toFMQLArgs(self, query):
        fmqlArgs = self.__schemaQueryToFMQLArgs(query)
        if fmqlArgs:
            return fmqlArgs
        return self.__dataQueryToFMQLArgs(query)
        
    def __dataQueryToFMQLArgs(query):
    
        # Operations (test in order)
        RE_DESCRIBENODE = re.compile('DESCRIBE +([\d_]+\-[\d\.]+)')
        RE_DESCRIBENODES = re.compile('DESCRIBE +([\d_]+)')
        RE_SELECTFROM = re.compile('SELECT +([\.\d]+) +FROM +([\d_]+)')
        RE_SELECT = re.compile('SELECT +([\d_]+)')
        RE_COUNT = re.compile('COUNT +([\d_]+)')
        RE_COUNTREFS = re.compile('COUNT +REFS +([\d_]+\-[\d\.]+)')

        # Qualifiers
        RE_IN = re.compile(' +IN +([\d_]+\-[\d\.]+)')
        RE_LIMIT = re.compile(' +LIMIT +(\d+)')
        RE_OFFSET = re.compile(' +OFFSET +(\d+)')
        RE_AFTERIEN = re.compile(' +AFTERIEN +(\d+)')
        RE_FILTER = re.compile(' +FILTER *\((.+)\)')
        RE_DESCRIBECSTOP = re.compile(' +CSTOP (\d+)')
        RE_NOIDXMAX = re.compile(' +NOIDXMAX +(\d+)')
        RE_ORDERBY = re.compile(' +ORDERBY +([\.\d]+)')
        
        if RE_DESCRIBENODE.match(query):
            nodeId = RE_DESCRIBENODE.match(query).group(1)
            nodeType, recordId = self.__makeFileRecordId(nodeId)
            cstopArg = "^CNODESTOP:%s" % RE_DESCRIBECSTOP.search(query).group(1) if RE_DESCRIBECSTOP.search(query) else ""
            fmqlArgs = "OP:DESCRIBE^TYPE:%s^ID:%s%s" % (nodeType,recordId,cstopArg)
            return fmqlArgs
            
        if RE_COUNTREFS.match(query):
            nodeId = RE_COUNTREFS.match(query).group(1)
            nodeType, recordId = self.__makeFileRecordId(nodeId)
            fmqlArgs = "OP:COUNTREFS^TYPE:%s^ID:%s^NOIDXMX:0" % (fileId,recordId)
            return fmqlArgs
            
        quals = []
        quals.append("^IN:" + RE_IN.search(query).group(1) if RE_IN.search(query) else "")
        quals.append("^FILTER:" + self.__uncolonFilter(RE_FILTER.search(query).group(1)) if RE_FILTER.search(query) else ""
        quals.append("^LIMIT:" + RE_LIMIT.search(query).group(1) if RE_IN.search(query) else "")
        quals.append("^OFFSET:" + RE_OFFSET.search(query).group(1) if RE_IN.search(query) else "")
        quals.append("^AFTERIEN:" + RE_AFTERIEN.search(query).group(1) if RE_IN.search(query) else "")
        quals.append("^NOIDXMX:" + RE_NOIDXMAX.search(query).group(1) if RE_IN.search(query) else "")
        quals.append("^ORDERBY:" + RE_ORDERBY.search(query).group(1) if RE_IN.search(query) else "")
        
        # ip, flt, limit, offset, afterien, orderBy, cstop
        if RE_DESCRIBENODES.match(query):
            nodeType = RE_DESCRIBENODES.match(query).group(1)
            fmqlArgs = "OP:DESCRIBE^TYPE:" + self.__makeTypeId(nodeType)
            for qual in quals:
                fmqlArgs += qual
            cstopArg = "^CNODESTOP:" + RE_DESCRIBECSTOP.search(query).group(1) if RE_DESCRIBECSTOP.search(query) else ""
            fmqlArgs += cstopArg
            return fmqlArgs
            
        if RE_COUNT.match(query):
            nodeType = RE_COUNT.match(query).group(1)
            fmqlArgs = "OP:COUNT^TYPE:" + self.__makeTypeId(nodeType)
            for qual in quals:
                fmqlArgs += qual
            return fmqlArgs
            
        if RE_SELECTFROM.match(query):
            pred = RE_SELECTFROM.match(query).group(1)
            nodeType = RE_SELECTFROM.match(query).group(2)
            fmqlArgs = "OP:SELECT^TYPE:" + self.__makeTypeId(nodeType) + "^PREDICATE:" + pred
            for qual in quals:
                fmqlArgs += qual
            return fmqlArgs
                
        if RE_SELECT.match(query):
            nodeType = RE_COUNT.match(query).group(1)
            fmqlArgs = "OP:SELECT^TYPE:" + self.__makeTypeId(nodeType)
            for qual in quals:
                fmqlArgs += qual
            return fmqlArgs

        raise Exception("Invalid Query type: " + query)
        
    def __schemaQueryToFMQLArgs(self, query):
        
        RE_DESCRIBETYPE = re.compile('DESCRIBE TYPE +([\d_]+)$')        
        if RE_DESCRIBETYPE.match(query):
            nodeType = RE_DESCRIBETYPE.match(query).group(1)
            fmqlArgs = ["OP:DESCRIBETYPE^TYPE:%s" % nodeType]
            return fmqlArgs

        RE_SELECTTYPES = re.compile('SELECT TYPES') # allows args        
        if RE_SELECTTYPES.match(query):
            fmqlArgs = ["OP:SELECTALLTYPES"]
            if re.search(r'TOPONLY', query):
                fmqlArgs[0] += "^TOPONLY:1"
            if re.search(r'BADTOO', query):
                fmqlArgs[0] += "^BADTOO:1"
            return fmqlArgs
            
        # TODO: change to SELECT TYPE REFS (vs COUNT REFS)
        RE_SELECTALLREFERRERSTOTYPE = re.compile('SELECTALLREFERRERSTOTYPE +([\d_]+)')
        if RE_SELECTALLREFERRERSTOTYPE.match(query):
            nodeType = RE_SELECTALLREFERRERSTOTYPE.match(query).group(1)
            fmqlArgs = ["OP:SELECTALLREFERRERSTOTYPE^TYPE:%s" % nodeType]
            return fmqlArgs
            
        return None
        
    def __makeTypeId(self, nodeId):
        return re.sub(r'\_', ".", urllib.unquote(nodeId))
        
    def __makeFileRecordId(self, url):
        url = urllib.unquote(url)
        pieces = re.search(r'([^\/\-]+)\-([^\-\/]+)$', url)
        if not pieces:
            raise Exception("QPERROR", "Invalid url - must be X-Y")
        nodeId = re.sub(r'\_', '.', pieces.group(1))
        # recordId = re.sub(r'\_', '.', pieces.group(2))
        recordId = pieces.group(2)
        return nodeId, recordId
        
    # current format over RPC separates with : but what
    # if it is used in a filter argument. Usually occurs for
    # dates.
    def __uncolonFilter(self, flt):
        return re.sub(r':', '-', flt)
        
# ################################## TEST ############################
        
import sys
import getopt
import traceback
from brokerRPC import VistARPCConnection

class DefaultLogger:
    def __init__(self):
        pass
    def logInfo(self, tag, msg):
        # self.__log(tag, msg)
        pass
    def logError(self, tag, msg):
        self.__log(tag, msg)
    def __log(self, tag, msg):
        print "FMQLQP -- %s %s" % (tag, msg)

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
    qpe = FMQLQPE(rpcc, logger)
    
    queries = ["DESCRIBE 2-1", "DESCRIBE 2", "SELECT 2", "COUNT 2", "DESCRIBE 2_0361 IN 2-3", "SELECT 2 LIMIT 3 OFFSET 1", "SELECT .01 FROM 2 LIMIT 3", "DESCRIBE 79_3 FILTER(.03=2-1)", "COUNT 50_68 FILTER(.05=11-2) NOIDXMAX 1"]
    for query in queries:
        print "=========================================================="
        reply = qpe.processQuery({"fmql": [query]})
        # jreply = json.loads(reply["data"])
        print reply

if __name__ == "__main__":
    main()
