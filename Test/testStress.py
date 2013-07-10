#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2013 caregraf
#

"""
 FMQL Stress Test

 Walk a complete system, file by file, describing all entries and ensuring
 that the reply is proper json.

 Note: large types will take a long time to load if no limit is set in a query
 but this is a stress test and though it may take some time, a query should
 get the correct reply and the Python JSON parser should be able to parse the
 result. On Windows/Cache, the latter appears not to be true and for files that get
 large nodes (Accession: 68) or many entries (Drug Cost: 50_9), set lower
 limits.

 Note: this test runs directly against the FMQL RPC and not the Apache
 resident endpoint.
"""

import json
import urllib, urllib2
import re
from brokerRPC import VistARPCConnection, RPCLogger
from fmqlQP import FMQLQP

###################### Walk all entries #######################

# Allow a manageable reply size for large system files (14.4 == Tasks) in a process-size limited Cache
LOWERLIMITS = {"3_075": "1", "9_4": "1", "9.6": "100", "14.4": "1", "50_8": "1", "50_9": "10", "68": "1", "90057": "10"}
INCREMENTLIMIT = -1 # no limit

# TBD:
# - add check that every reply node has a "uri" ie. check for {} in JSON. Shouldn't happen.

# For first tests: op="CountAllOfType", then do "SelectAllOfType".
def walkAll(qp, op="DESCRIBE", useLowerLimits=True):
	problemFiles = []
	print "Get all the types ..."
    query = "SELECT TYPES"
	reply = qp.processQuery(query)
	jreply = json.loads(reply)
	for result in jreply["results"]:
		if "parent" in result:
			continue
		fileType = re.sub(r'\.', '_', result["number"])
		if useLowerLimits and fileType in LOWERLIMITS:
			ulimit = LOWERLIMITS[fileType]
		else:
			ulimit = INCREMENTLIMIT
		print "=== Now walking %s ===" % fileType
		offset = 0
		totalWalked = 0
		try: 
			while True:
				# walk limit at a time. Do offset
                query = op + " " + fileType + " LIMIT " + ulimit + " OFFSET " + offset
				print "Sending query", query
				reply = qp.processQuery(query)
				jreply = json.loads(reply)
				if "error" in jreply:
					print "Received error %s - breaking" % str(jreply["error"])
					break # Assume if caught then ok
				totalWalked = totalWalked + int(jreply["count"])
				print "Received and parsed %d" % int(jreply["count"])
				if fileType in LOWERLIMITS or INCREMENTLIMIT == -1 or int(jreply["count"]) != INCREMENTLIMIT:
					break
				offset = totalWalked
				print "Big file: Walked next %s of %s. %d so far" % (ulimit, fileType, totalWalked)
		except Exception as e:
			print e
			print "*** Problem with %s ***" % fileType
			problemFiles.append(fileType)
		print "... %d of %s done" % (totalWalked, fileType)
	pfl = open("problemFiles.txt","w")
	pfl.write(str(problemFiles))
	print "Done walking system: problem files in 'problemFiles.txt'"

# ##################### Main Driver #####################

import os

def main():

	try:
		rpcc = VistARPCConnection('localhost', 9201, 'A31234', 'A31234!!', "CG FMQL QP USER", RPCLogger())
	except Exception as e:
		print "Failed to log in to System for FMQL RPC (bad parameters?): %s ... exiting" % e
		return

	fmqlQP = FMQLQP(rpcc, RPCLogger())
		
	walkAll(fmqlQP)
	
if __name__ == "__main__":
	main()

