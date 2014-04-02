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

Query data and schema of FileMan using SPARQL-like query format. Returns JSON.
 
"""

import urllib
import re
import json

class FMQLQP:

    def __init__(self, rpcc, logger):
        self.rpcc = rpcc
        self.logger = logger
        
    def invokeQuery(self, query):
        # direct dispatch i/f
        reply = self.processQuery({"fmql": [query]})
        jreply = json.loads(reply)
        return jreply
        
    def processQuery(self, queryArgs):
        """
        Expect query and then qualifiers for return format

        Note: will exception for internal errors.
        """
        if "fmql" not in queryArgs:
            raise Exception("Expect fmql=")
        query = queryArgs["fmql"][0]
        reply = self.rpcc.invokeRPC("CG FMQL QP", [query])
        return reply
        
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
    qp = FMQLQP(rpcc, logger)
    
    queries = ["DESCRIBE 2-1", "DESCRIBE 2", "SELECT 2", "COUNT 2", "DESCRIBE 2_0361 IN 2-3", "SELECT 2 LIMIT 3 OFFSET 1", "SELECT .01 FROM 2 LIMIT 3", "DESCRIBE 79_3 FILTER(.03=2-1)", "COUNT 50_68 FILTER(.05=11-2) NOIDXMAX 1"]
    for query in queries:
        print "=========================================================="
        reply = qp.processQuery({"fmql": [query]})
        # jreply = json.loads(reply["data"])
        print reply

if __name__ == "__main__":
    main()
