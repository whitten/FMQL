#!/usr/bin/env python

import os
import re 
import sys
import urllib, urllib2
import json
from StringIO import StringIO
import shutil
from datetime import datetime
from collections import OrderedDict
    
# - fix up to catch timeouts - 101 on 5000 
# - need to have fixed set from config of manual #'s (see id's below)
# - manual pass in of what to get ... first.

"""
Basic FMQL JSON cacher for both schema and data. Used for basic caching AND stress testing
(node) ... should work with any FM system (VISTA or CHCS)

Works off "configs/" and expects a /data top level location where it puts/reads its
data

As endpoint returning old JSON, JSON-LD is another pass. When add JSON-LD natively to FMQL, the extra pass will go away.

To test:
- du -h /data/{SYSTEM}/JSON
- old 2 download has 'File 2, limit 500 : all 1032150 of file 2 in 4:49:22.820342'

Invoke: nohup python -u dsMaker.py VISTACONFIG > nohupVISTA.out & and tail -f nohupVISTA.out; monitor with ps -aux, kill with kill -9 {pid}

------------------------------------------------------------

TODO:
1. {"siteLabel": "NH GREAT LAKES IL",  ... add to about
2. Calc LIMIT to avoid CSP timeout
    - if timeout, /5 and go again (if get 503 explicitly ie/ catch it)
    - run through looking at timing - get around 15 seconds?
    - fits in with CSP failure/retry
    - also try calculate schema complexity and see if corresponds to limit problems ie/
      # of multiples (may not be good indication as don't know singleton vs many)
    NOTE: don't treat 503 the same as "connection timed out" which has happened when outside connections went down.
3. Consider adding to about/config, the known empties to avoid re-checking them with
every pass [PARTIALLY DONE]

Setup: per system directory and under that JSON, JSON-LD and RDF directories. The
latter two are generated in a later phase.
Note: this indirection is due to go away in LPI
"""

# ########################## Data #########################
   
def cacheFMData(config):
    
    print
    print "Caching data of FM System", config["fmqlEP"]
    print
    
    start = datetime.now()
    
    baseLocation = "/data/" + config["name"]
    if not os.path.isdir(baseLocation):
        os.mkdir(baseLocation)
    cacheLocation = baseLocation + "/JSON/"
    if not os.path.isdir(cacheLocation):
        os.mkdir(cacheLocation)

    if not os.path.isdir("tmp"):
        os.mkdir("tmp")
    try:
        zeroFiles = json.load(open("tmp/tmpZeroFiles" + config["name"] + '.json'))
    except: 
        zeroFiles = []
    
    fmqlEP = FMQLIF(config["fmqlEP"], config["fmqlQuery"])
    excludeTypes = set(config["excluded"]) if "excluded" in config else set()
    includeTypes = set(config["included"]) if "included" in config else None

    j = 0
    for i, fli in enumerate(files(fmqlEP, cacheLocation, allFiles=False), 1):
        print i, "Caching file", fli["id"], "expecting count of", fli["count"], "** Note POP says 0 but trying anyhow" if fli["count"] == 0 else ""

        if fli["id"] in excludeTypes:
            print "... but excluded explicitly so skipping"
            continue

        if fli["id"] in zeroFiles:
            print "... but known to be empty ('zero file') so skipping"
            continue

        if includeTypes and fli["id"] not in includeTypes:
            print "... skipping", fli["id"], "as not in explicit included list"
            continue
        
        # Todo: EXPLICT CONFIG SETTINGS IN CONFIG
        # Biggest include SNOMED, Unit Ship Id ...
        # ... trying to stick to 30 seconds or so (40 seems to be max from file 1.2)
        # ... TODO: if first times out or is too low then change limit til hone in
        # on thirty seconds.
        if fli["id"] in ["109_3", "8188_12", "8252", "8111", "4000_1"]:
            limit = 5000
        elif fli["id"] in ["101", "52", "55"]: # TC2 timed out with 55 at 5000
            limit = 1000
        # 503 CSP if more
        elif fli["id"] in ["8253_04", "8253_021", "63", "8253_01"]:
            limit = 100
        else:
            limit = 5000 # speed up as close network

        try: # may get license error - make sure save zero files first
            if not cacheFile(fmqlEP, cacheLocation, fli["id"], limit=limit, cstop="1000", maxNumber=-1):
                zeroFiles.append(fli["id"])
        except:
            if len(zeroFiles):
                json.dump(zeroFiles, open("tmp/tmpZeroFiles" + config["name"] + ".json", "w"))
            raise

    # Report result AND copy meta into 'about.json'
    about = config.copy()
    about["cacheEnd"] = datetime.now().strftime("%m/%d/%Y %I:%M%p")
    about["timeTaken"] = str(datetime.now() - start)
    if len(zeroFiles):
        about["zeroFiles"] = zeroFiles
    print
    print "Caching data done - took", about["timeTaken"]
    json.dump(about, open(cacheLocation + "about.json", "w"), indent=4)
    print "... about flushed to", cacheLocation + "about.json"
    print

def cacheFile(fmqlEP, cacheLocation, fileType, limit=500, cstop=1000, maxNumber=-1):
    """
    File by file, cache data
    
    Mandatory arguments:
    - fmqlEP
    - cacheLocation: where to store the JSON
    - fileType: ex/ 2 or 120_5

    Optional/defaulted arguments:
    - limit for query: defaults to 1000
    - cstop for query: defaults to 10
    - filter for query: default is none
    - maxNumber: maximum number to retrieve. Default is no limit (-1)
    - afterIEN (for restart if necessary and for doing LIMIT at a time)
    - epWord: query used in CSP and node; Apache uses "fmql"
    """
    queryTempl = "DESCRIBE " + fileType + " LIMIT %(limit)s AFTERIEN %(afterien)s CSTOP " + str(cstop)

    # calculate afterien - last one already cached (can't know next) OR 0
    # ... allowing IEN of . (ex/ SYNTH CHCS 8461)
    afteriensSoFar = sorted([float(re.search(r'\-([\d\.]+)\.json$', f).group(1)) for f in os.listdir(cacheLocation) if os.path.isfile(os.path.join(cacheLocation, f)) and re.search('\.json$', f) and re.match(fileType + "\-", f)], reverse=True) 
    if len(afteriensSoFar) == 0:
        afterien = 0
    else:
        afterien = int(afteriensSoFar[0]) if afteriensSoFar[0].is_integer() else afteriensSoFar[0]
        lastReplyFile = cacheLocation + fileType + "-" + str(afterien) + ".json"
        lastReply = json.load(open(lastReplyFile)) 
        if "LIMIT" in lastReply["fmql"]:
            # REM: limit for next go may not be the same so not overriding limit
            lastLimit = int(lastReply["fmql"]["LIMIT"])
            if lastLimit > len(lastReply["results"]):
                print "Got all of", fileType, "already -- moving on" 
                return True  
        afterien = lastReply["results"][-1]["uri"]["value"].split("-")[1]
        print "Still some of filetype", fileType, "left to get. Restarting with AFTERIEN", afterien

    # queryNo and afterIEN are usually 0 but can start again    
    # Loop until there is no more or we reach the maximum
    numberOfTypeCached = 0
    start = datetime.now()
    queryNo = 0
    while True:
        queryNo += 1
        query = queryTempl % {"limit": limit, "afterien": afterien}
        print "Sending query number", queryNo, "after", numberOfTypeCached, "cached -", query
        queryStart = datetime.now()
        jreply = fmqlEP.invokeQuery(query)
        if "error" in jreply:
            raise Exception(jreply["error"])
        # Special case - first call (afterien=0) and no results => don't cache
        # ... REM: doing TOPONLY in case POPONLY is wrong. Means more queries but safer.
        # Ex for CHCS Synth: [u'3_081', u'66', u'52'] not in POP but have "data"
        # ... REM: afterien != 0 and no results then still cache as it means the case of
        # second to last LIMITED query filled up and then the last just returns none.
        # Note: alt is do a COUNT and then DESCRIBE if need be but largely the same cost.
        if afterien == 0 and len(jreply["results"]) == 0:
            print "Finished - got 0 replies from first (afterien=0) query means nothing to cache"
            return False 
        startJSONDump = datetime.now()
        jsonOldFileName = cacheLocation + fileType + "-" + str(afterien) + ".json"
        json.dump(jreply, open(jsonOldFileName, "w"))
        print "Cached JSONOLD", jsonOldFileName, "in", datetime.now() - startJSONDump
        print "... got", len(jreply["results"]), "resources in", datetime.now() - queryStart
        if len(jreply["results"]) == 0:
            break
        numberOfTypeCached += len(jreply["results"])
        if len(jreply["results"]) != limit:
            print "At end - < limit resources returned"
            break
        # TODO: properly reset limit at the start to make sure maximum never exceeded
        if maxNumber != -1 and numberOfTypeCached >= maxNumber:
            print "Breaking as got or exceeded maximum", maxNumber
            break
        afterien = jreply["results"][-1]["uri"]["value"].split("-")[1]
        if (queryNo % 100) == 0:
            print "So far this has taken", datetime.now() - start

    print "Finished - cached", numberOfTypeCached, "took", datetime.now() - start

    return True
    
def files(fmqlEP, cacheLocation, allFiles=False, epWord="query"):
    """
    From SELECT TYPES - if in cache just load and read. Otherwise cache and
    then load and read.
    """
    query = "SELECT TYPES TOPONLY" # doing TOPONLY and not POPONLY in case POP is wrong
    cacheFile = re.sub(r' ', "_", query) + ".json"
    try:
        reply = json.load(open(cacheLocation + cacheFile), object_pairs_hook=OrderedDict)
    except Exception:
        print "First time through - must (re)cache", query, "to", cacheLocation, "..."
        reply = fmqlEP.invokeQuery(query)
        json.dump(reply, open(cacheLocation + cacheFile, "w"))
    oresults = sorted(reply["results"], key=lambda res: int(res["count"]) if "count" in res else 0, reverse=True)
    filesInCountOrder = []
    for i, result in enumerate(oresults, 1):
        fileId = re.sub(r'\.', '_', result["number"])
        filesInCountOrder.append({"id": fileId, "count": 0 if "count" not in result else int(result["count"])})
    print "Returning", len(filesInCountOrder), "top files - not all will have data"

    return filesInCountOrder
    
# ####################### Schemas #####################
    
def cacheSchemas(config):
    """
    Other type of data - meta data
    """
    start = datetime.now()
    
    baseLocation = "/data/" + config["name"]
    if not os.path.isdir(baseLocation):
        os.mkdir(baseLocation)
    cacheLocation = baseLocation + "/JSON/"
    if not os.path.isdir(cacheLocation):
        os.mkdir(cacheLocation)
    
    fmqlEP = FMQLIF(config["fmqlEP"], config["fmqlQuery"])
    
    alreadyCached = [re.match(r'SCHEMA\_([^\.]+)', f).group(1) for f in os.listdir(cacheLocation) if os.path.isfile(os.path.join(cacheLocation, f)) and re.search('\.json$', f) and re.match("SCHEMA_", f)]
    
    query = "SELECT TYPES"
    try:
        jreply = json.load(open(cacheLocation + "SELECT_TYPES.json"))
    except Exception:
        print "First time through - must (re)cache SELECT TYPES to", cacheLocation, "..."
        jreply = fmqlEP.invokeQuery(query)
        json.dump(jreply, open(cacheLocation + "SELECT_TYPES.json", "w"))

    # Not relying on OrderedDict
    fileIds = [re.sub(r'\.', "_", result["number"]) for result in jreply["results"]]
    print "Must cache schema of", len(fileIds), "files ..."
    for fileId in fileIds:
        if fileId in alreadyCached:
            print "Got schema of", fileId, "already so going to next"
            continue
        print "Caching Schema of", fileId
        query = "DESCRIBE TYPE " + fileId
        queryStart = datetime.now()
        jreply = fmqlEP.invokeQuery(query)
        json.dump(jreply, open(cacheLocation + "SCHEMA_" + fileId + ".json", "w"))
        
    print 
    print "Schema caching took", datetime.now() - start, "for", len(fileIds), "files"
    print
        
##################### Utilities ################
                                
class FMQLIF:
    """
    Indirection between endpoint for query so can go to CSPs etc.
        
    TODO: catch 503 etc in here and return clean ERROR codes
    """
    def __init__(self, fmqlEP, epWord="query"):
        self.fmqlEP = fmqlEP
        self.epWord = epWord
        
    def __str__(self):
        return "ENDPOINT: " + self.fmqlEP
        
    def invokeQuery(self, query):
        queryURL = self.fmqlEP + "?" + urllib.urlencode({self.epWord: query}) 
        try:
            sreply = StringIO(urllib2.urlopen(queryURL).read()) # be compatible with cacheObjectInterface
            # Want to preserve order of keys (as FMQL does) 
            jreply = json.load(sreply, object_pairs_hook=OrderedDict) 
            return jreply
        except:
            try:
                sreply.seek(0)
            except:
                raise
            text = sreply.read()
            if re.search(r'FMQL\.CSP', self.fmqlEP): # CSP endpoint
                #
                # Error: <b>Cannot allocate a license</b><br>
                # ErrorNo: <b>5915</b><br>
                # CSP Page: <b>/csp/fmquery/FMQL.CSP</b><br>
                # Namespace: <b>CHCS</b><br>
                #
                if re.search(r'ErrorNo', text):
                    error = re.search(r'Error: \<b\>([^\<]+)', text).group(1)
                    errorNo = re.search(r'ErrorNo: \<b\>([^\<]+)', text).group(1)
                    raise Exception("Received CSP Error - " + errorNo + ' - ' + error)
            print "Couldn't parse reply as JSON", text[0:1000], "..."
            raise
        
def purgeCache(cacheLocation):
    try:
        shutil.rmtree(cacheLocation)
        print "Purged cache", cacheLocation
        os.mkdir(cacheLocation)
    except:
        pass
        
"""
Can get all types and then get replyFiles per type
"""
def typesInCache(cacheLocation):
    # a/c for _ and pure \d file types
    types = sorted(list(set(fl.split("-")[0] for fl in os.listdir(cacheLocation) if os.path.isfile(os.path.join(cacheLocation, fl)) and re.match(r'_?\d', fl) and re.search('\.json$', fl))))
    return types
        
def replyFilesOfType(cacheLocation, fileType):
    replyFiles = sorted([fl for fl in os.listdir(cacheLocation) if os.path.isfile(os.path.join(cacheLocation, fl)) and re.search('\.json$', fl) and re.match(fileType + "-", fl)], key=lambda x: float(re.search(r'\-([\d\.]+)\.json$', x).group(1)))
    return replyFiles
    
# ############################# FMQL Cache Driver ####################################
    
# Relies on configs in configs directory: ./dsMaker.py VISTAAB SCHEMA
def main():

    if len(sys.argv) < 2:
        print "need to specify configuration ex/ VISTAAB - exiting"
        return

    config = sys.argv[1].split(".")[0]

    configsAvailable = [f.split(".")[0] for f in os.listdir("configs")]

    if config not in configsAvailable:
        print "config specified", config, "is not in configs available -", configsAvailable
        return

    configJSON = json.load(open("configs/" + config + ".json"), object_pairs_hook=OrderedDict)

    print
    print "FMQL Caching using configuration", configJSON["name"], "on endpoint", configJSON["fmqlEP"], "..."
    if len(sys.argv) == 3 and sys.argv[2] == "SCHEMA":
        print "\tcaching schema"
        cacheSchemas(configJSON)
        return
    print
    print "Now caching data ..."
    cacheFMData(configJSON)
    print

if __name__ == "__main__":
    main()
