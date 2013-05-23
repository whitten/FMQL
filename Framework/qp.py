#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

"""
 FMQL Query Processor

 Describing and listing of two graphs - schema and data. Pass through to FMQL RPC
 
"""

__author__ =  'Caregraf'
__copyright__ = "Copyright 2010-2013, Caregraf"
__license__ = "AGPL"
__version__=  'v1.0b'
__status__ = "Production"

import urllib
import re
import json
from brokerRPC import RPCConnection

class FMQLQueryProcessor:

    def __init__(self, rpcc, logger):
        self.rpcc = rpcc
        self.logger = logger

    #
    # TBD
    # - conforms to SPARQL WSDL: http://www.w3.org/TR/rdf-sparql-protocol/#query-bindings-http
    #
    def processQuery(self, queryArgs):
        if not queryArgs.has_key("op"):
            raise Exception("QPERROR", "No operation specified")
        if queryArgs["op"][0] == "SelectAllTypes":
            return self.__SelectAllTypes(queryArgs)
        elif queryArgs["op"][0] == "Select":
            if not queryArgs.has_key("typeId"):
                raise Exception("QPERROR", "No typeId specified for Select")
            ip = ""
            if queryArgs.has_key("in"):
                ip = "^IN:%s" % queryArgs["in"][0]
            flt = None
            if queryArgs.has_key("filter"):
                flt = urllib.unquote(queryArgs["filter"][0])
            limit = ""
            if queryArgs.has_key("limit"):
                limit = "^LIMIT:%s" % queryArgs["limit"][0]
            offset=""
            if queryArgs.has_key("offset"):
                offset = "^OFFSET:%s" % queryArgs["offset"][0]
            # TODO: more testing on afterien - NOT appropriate for indexed filters so for now turning off for all filters
            afterien=""
            if queryArgs.has_key("afterien") and not (queryArgs.has_key("offset") or queryArgs.has_key("filter")): # afterien only for straight selects for now
                afterien = "^AFTERIEN:%s" % queryArgs["afterien"][0]
            orderBy = ""
            if queryArgs.has_key("orderby"):
                orderBy = "^ORDERBY:%s" % queryArgs["orderby"][0]
            pred=""
            # Only take one for now
            if queryArgs.has_key("predicate"):
                pred = "^PREDICATE:%s" % queryArgs["predicate"][0]
            return self.__Select(queryArgs["typeId"][0], ip, flt, limit, offset, pred, orderBy, afterien)
        elif queryArgs["op"][0] == "Count":
            if not queryArgs.has_key("typeId"):
                raise Exception("QPERROR", "No typeId specified for Count")
            ip = ""
            if queryArgs.has_key("in"):
                ip = "^IN:%s" % queryArgs["in"][0]
            flt = None
            noidxmx=""
            if queryArgs.has_key("filter"):
                flt = urllib.unquote(queryArgs["filter"][0])
                if queryArgs.has_key("noidxmx"):
                    noidxmx="^NOIDXMX:%s" % queryArgs["noidxmx"][0]
            limit = ""
            if queryArgs.has_key("limit"):
                limit = "^LIMIT:%s" % queryArgs["limit"][0]
            offset=""
            if queryArgs.has_key("offset"):
                offset = "^OFFSET:%s" % queryArgs["offset"][0]
            # TODO: more testing on afterien - NOT appropriate for indexed filters so for now turning off for all filters
            afterien=""
            if queryArgs.has_key("afterien") and not (queryArgs.has_key("offset") or queryArgs.has_key("filter")): # afterien only for straight selects for now
                afterien = "^AFTERIEN:%s" % queryArgs["afterien"][0]
            return self.__Count(queryArgs["typeId"][0], ip, flt, limit, offset, noidxmx, afterien)
        elif queryArgs["op"][0] == "DescribeType":
            if not queryArgs.has_key("typeId"):
                raise Exception("QPERROR", "No typeId specified for DescribeType")
            # return json.dumps(self.__DescribeType(queryArgs["typeId"][0]))
            return self.__DescribeType(queryArgs["typeId"][0])
        elif queryArgs["op"][0] == "Describe":
            return self.__Describe(queryArgs)
        elif queryArgs["op"][0] == "SelectAllReferrersToType":
            if not queryArgs.has_key("typeId"):
                raise Exception("QPERROR", "no typeId specified for SelectAllReferrers")
            return self.__SelectAllReferrersToType(queryArgs["typeId"][0])
        elif queryArgs["op"][0] == "COUNT REFS":
            if not queryArgs.has_key("url"):
                raise Exception("QPERROR", "no url specified for COUNT REFS")
            return self.__COUNT_REFS(queryArgs["url"][0])
        raise Exception("QPERROR", "Invalid query - no such operation")
        
    def __makeTypeId(self, classId):
        return re.sub(r'\_', ".", urllib.unquote(classId))
        
    def __makeFileRecordId(self, url):
        url = urllib.unquote(url)
        pieces = re.search(r'([^\/\-]+)\-([^\-\/]+)$', url)
        if not pieces:
            raise Exception("QPERROR", "Invalid url - must be X-Y")
        fileId = re.sub(r'\_', '.', pieces.group(1))
        # recordId = re.sub(r'\_', '.', pieces.group(2))
        recordId = pieces.group(2)
        return fileId, recordId

    # Meta Graph: Select ?typ ?name ... WHERE {?typ :name ?name ...} FROM SCHEMA
    # Rambler URL Form: schema
    # In schema graph, everything is of type file.
    def __SelectAllTypes(self, queryArgs):
        fmqlArgs = ["OP:SELECTALLTYPES"]
        if "toponly" in queryArgs:
            fmqlArgs[0] += "^TOPONLY:1"
        if "badtoo" in queryArgs:
            fmqlArgs[0] += "^BADTOO:1"
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    def __Select(self, classId, ip="", flt=None, limit="", offset="", pred="", orderBy="", afterien=""):
        if not flt:
            fmqlArgs = ["OP:SELECT^TYPE:%s%s%s%s%s%s%s" % (self.__makeTypeId(classId), ip, limit, offset, pred, orderBy,afterien)]
        else:
            fmqlArgs = ["OP:SELECT^TYPE:%s%s^FILTER:%s%s%s%s" % (self.__makeTypeId(classId), ip, self.__uncolonFilter(flt), limit, offset, pred)]
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    def __Count(self, classId, ip="", flt=None, limit="", offset="", noidxmx="", afterien=""):
        if not flt:
            fmqlArgs = ["OP:COUNT^TYPE:%s%s%s%s%s" % (self.__makeTypeId(classId), ip, limit, offset, afterien)]
        else:
            fmqlArgs = ["OP:COUNT^TYPE:%s%s^FILTER:%s%s%s%s" % (self.__makeTypeId(classId), ip, self.__uncolonFilter(flt), limit, offset, noidxmx)]
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    # Meta Graph: Describe <classId> FROM SCHEMA
    # Rambler URL Form: schema/<classId>
    def __DescribeType(self, classId):
        fmqlArgs = ["OP:DESCRIBETYPE^TYPE:%s" % classId]
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    # Describe ../<classId>-<instanceId>
    # Rambler URL Form: ../<classId>-<instanceId>
    def __Describe(self, queryArgs):
        if queryArgs.has_key("url"):
            fileId, recordId = self.__makeFileRecordId(queryArgs["url"][0])
            cstop = ""
            if queryArgs.has_key("cstop"):
                cstop = "^CNODESTOP:%s" % queryArgs["cstop"][0]
            fmqlArgs = ["OP:DESCRIBE^TYPE:%s^ID:%s%s" % (fileId,recordId,cstop)]
        elif queryArgs.has_key("typeId"):
            classId = queryArgs["typeId"][0]
            limit = ""
            if queryArgs.has_key("limit"):
                limit = "^LIMIT:%s" % queryArgs["limit"][0]
            offset = ""
            if queryArgs.has_key("offset"):
                offset = "^OFFSET:%s" % queryArgs["offset"][0]
            # TODO: more testing on afterien - NOT appropriate for indexed filters so for now turning off for all filters
            afterien=""
            if queryArgs.has_key("afterien") and not (queryArgs.has_key("offset") or queryArgs.has_key("filter")): # afterien only for straight selects for now
                afterien = "^AFTERIEN:%s" % queryArgs["afterien"][0]
            orderBy = ""
            if queryArgs.has_key("orderby"):
                orderBy = "^ORDERBY:%s" % queryArgs["orderby"][0]
            ip = ""
            if queryArgs.has_key("in"):
                ip = "^IN:%s" % queryArgs["in"][0]
            cstop = ""
            if queryArgs.has_key("cstop"):
                cstop = "^CNODESTOP:%s" % queryArgs["cstop"][0]
            if queryArgs.has_key("filter"):
                flt = urllib.unquote(queryArgs["filter"][0])
                fmqlArgs = ["OP:DESCRIBE^TYPE:%s%s^FILTER:%s%s%s%s" % (self.__makeTypeId(classId), ip, self.__uncolonFilter(flt), limit, offset, cstop)]
            else:
                fmqlArgs=["OP:DESCRIBE^TYPE:%s%s%s%s%s%s%s" % (self.__makeTypeId(classId), ip, limit, offset, afterien, orderBy, cstop)]
        else:
            raise Exception("QPERROR", "No typeId specified for Describe")
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    # Meta Graph: Select ?refby ?predicate WHERE {?s a <:classId> . ?refby ?predicate ?s} FROM SCHEMA
    # Rambler URL Form: schema-<classId> [shared with DescribeType]
    def __SelectAllReferrersToType(self, classId):
        fmqlArgs = ["OP:SELECTALLREFERRERSTOTYPE^TYPE:%s" % classId]
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply

    # COUNT REFS (FileMan doesn't do wildcard Select *=X
    # Rambler URL Form: ../<classId>-<instanceId> [shared with Describe]
    # Note: hard coded NOIDXMX to 0. Too dangerous with 44 etc with lot's
    # of unindexed refs.
    def __COUNT_REFS(self, url):
        fileId, recordId = self.__makeFileRecordId(url)
        fmqlArgs = ["OP:COUNTREFS^TYPE:%s^ID:%s^NOIDXMX:0" % (fileId,recordId)]
        reply = self.rpcc.invokeRPC("CG FMQL QP", fmqlArgs)
        return reply
        
    #
    # TBD: insert. Waits until schema settled.
    #

    # TBD: remove 
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

    qp = FMQLQueryProcessor(rpcc, DefaultLogger())
    
    try:
        reply =  qp.processQuery({"op": ["Describe"], "url": ["2-9"]})
        print json.loads(reply)
        print "=========================================================="
        print "List 10 ..."
        reply = qp.processQuery({"op": ["Select"], "typeId": ["10"], "limit": ["10"]})
        print len(json.loads(reply)["results"])
        print "=========================================================="
        print "DESCRIBE 5 ..."
        reply =  qp.processQuery({"op": ["Describe"], "typeId": ["2"], "limit": ["5"]})
        print len(json.loads(reply)["results"])

    except Exception as e:
        print "Exception trying to list patients: %s" % e
        traceback.print_exc()

if __name__ == "__main__":
    main()
