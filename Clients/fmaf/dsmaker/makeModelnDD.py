#!/usr/bin/env python

import os
import sys
import json
from fmaf.fileManInfo import FileManInfo, loadFileManInfoFromCache 
from fmaf.dataModel import DataModel

DEFAULT_CACHE_BASE = "/data/" # /data/VISTAAB etc ie/ where data cached - work off cache below

"""
python makeModelnDD.py VISTAAB VISTA 
... expects FM Info in /data/VISTAAB/JSON
"""
def main():

    if len(sys.argv) < 3:
        print "need to specify both the system name (ex/ OSEHRA) and type (VISTA or CHCS)  - exiting"
        return
        
    sysName = sys.argv[1]
    sysType = sys.argv[2]
        
    cacheBase = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_CACHE_BASE

    print
    print "Generating dd and model for", sysName, "of type", sysType
    print
    
    baseLocation = cacheBase + sysName
    cacheLocation = baseLocation + "/JSON/"
    if not os.path.isdir(cacheLocation):
        raise Exception("Can't find cache location")
    # Third argument, description, not passed in
    fileManInfo = loadFileManInfoFromCache(cacheLocation, sysName, sysType)
                
    if not os.path.isdir(baseLocation + "/META"):
        os.mkdir(baseLocation + "/META")
        
    ddFile = baseLocation + "/META/dd.json"
    print "DD written out to", ddFile
    open(ddFile, "w").write(fileManInfo.toJSONLD())

    dataModel = DataModel(fileManInfo)
    modelFile = baseLocation + "/META/model.json"
    print "Model written out to", modelFile
    open(modelFile, "w").write(dataModel.toJSONLD())

if __name__ == "__main__":
    main()
