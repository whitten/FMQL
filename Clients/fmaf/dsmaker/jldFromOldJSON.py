#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import re 
import sys
import json
from datetime import datetime
from collections import OrderedDict

from dsMaker import typesInCache, replyFilesOfType

# From fmaf Framework
from fmaf.describeResult import DescribeReply 
from fmaf.formatters.describeReplyToJLD import DescribeReplyToJLD

"""
Making JLD from Plain FMQL JSON - this is phase 2 of the download. Could put it into the
same pipe but separating for now

nohup ./jldFromOldJSON.py VISTATEST > nohupToJLDVISTATEST &

REM: will go away when node returns JLD - only renamer will survive.

TODO:
1. align the @context with LDR's @context including sameAsLabel
2. renaming in a Renamer class that loads meta (or whatever). Too manual in here and in DescribeReply.
Better to have outside renamer change DescribeReply outside both here and it. See comment in FMQL
DescribeReply.
"""

def makeJLD(config, useNameMeta=False, reassemble=True):
    """
    Key: useNameMeta decides if renaming meta in /data/SYSTEM/META should be used to rename
    fields or not. If renaming is used then new JSONLD goes in JSONLDRN and not JSONLD 
    """

    cacheLocation = "/data/" + config["name"] + "/JSON/"
    if not os.path.isdir(cacheLocation):
        print "Cache location for dataset doesn't even exist ... exiting"
        return
        
    jldLocation = re.sub(r'JSON', 'JSONLD', cacheLocation) 
    systemBase = config["systemBase"] # ex/ http://livevista.caregraf.info/    
    nameMeta = {}
    multipleMeta = {}
    metaLocation = "/data/" + config["name"] + "/META/"
    if useNameMeta and os.path.isdir(metaLocation):
        try:
            nameMeta = json.load(open(metaLocation + "fmNameMeta.json"))
        except:
            pass
        try:
            multipleMeta = json.load(open(metaLocation + "fmMultipleMeta.json"))
        except:
            pass
    if len(nameMeta) or len(multipleMeta):
        # use JSONLDRN if renaming happening.
        jldLocation = re.sub(r'JSON', 'JSONLDRN', cacheLocation) 
    if not reassemble: # ie/ JSONLDQRY etc ie/ still in pieces ala FMQL original
        jldLocation = re.sub(r'\/$', 'QRY/', jldLocation) 
    if not os.path.isdir(jldLocation):
        os.mkdir(jldLocation)
        
    start = datetime.now()

    types = typesInCache(cacheLocation)
    print
    print "========== making JSONLD from basic FMQL JSON ==========="
    print "Have", len(types), "in cache", cacheLocation
    print
                
    for i, type in enumerate(types, 1):
        replyFiles = replyFilesOfType(cacheLocation, type)
        print i, "Type", type, "is in", len(replyFiles), "pieces"
        jlds = []
        for j, replyFile in enumerate(replyFiles, 1):
            print "\t", j, "..."
            jldFile = re.sub("\.json", ".jsonld", replyFile)
            if os.path.isfile(os.path.join(jldLocation, jldFile)):
                print "\tFile", jldFile, "already there - skipping"
                continue
            jreply = json.load(open(os.path.join(cacheLocation, replyFile)), object_pairs_hook=OrderedDict)
            dr = DescribeReply(jreply)
            # TODO: pass meta into DescribeReply instead
            drToJLD = DescribeReplyToJLD(fms="chcss", systemBase=systemBase, useMongoResourceId=True, nameMeta=nameMeta, multipleMeta={})
            drToJLD.processReply(dr)
            jld = drToJLD.json()
            if not reassemble:
                jldFile = re.sub("\.json", ".jsonld", replyFile)
                json.dump(jld, open(jldLocation + jldFile, "w"), indent=2)
                print "\tWrote", jldFile
            else:
                jlds.append(jld)
        if len(jlds): # means reassemble
            oneJLD = {}
            oneJLD["@context"] = jlds[0]["@context"]
            oneJLD["generatedAt"] = jlds[-1]["generatedAt"]
            oneJLD["@graph"] = []
            for jld in jlds:
                oneJLD["@graph"].extend(jld["@graph"])
            oneJLDFile = replyFile.split("-")[0] + ".jsonld"
            json.dump(oneJLD, open(jldLocation + oneJLDFile, "w"), indent=2)            
                
    timeTaken = datetime.now() - start
    # Note: about.json should be top level and not under JSON anyhow
    try:
        # load about from JSON, add more meta and save
        jsonAbout = json.load(open(cacheLocation + "about.json"), object_pairs_hook=OrderedDict)
        # jsonAbout["toJLDTimeTaken"] = str(timeTaken)
        jsonAbout["JLDMade"] = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
        json.dump(jsonAbout, open('/data/' + config["name"] + "/about.json", "w"), indent=4)
    except:
        print
        print "*** WARNING: no about.json in cacheLocation to copy up a level (peer of JLD)"
        print
        
    print
    print "JLDing took", timeTaken
    print
            
# ############################# Driver ####################################

# invoke with ./jldFromOldJSON.py VISTATEST
CONFIGS_DIR = "configs/"

def main():

    if len(sys.argv) < 2:
        print "need to specify configuration ex/ VISTAAB - exiting"
        return
        
    config = sys.argv[1].split(".")[0]

    configsAvailable = [f.split(".")[0] for f in os.listdir(CONFIGS_DIR)]

    if config not in configsAvailable:
        print "config specified", config, "is not in configs available -", configsAvailable
        return

    configJSON = json.load(open(CONFIGS_DIR + config + ".json"))

    print
    print "Making JSON-LD from FMQL JSON for", configJSON["name"], "..."
    
    makeJLD(configJSON)
        
if __name__ == "__main__":
    main()
