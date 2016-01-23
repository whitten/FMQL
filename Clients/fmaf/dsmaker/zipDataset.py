#!/usr/bin/env python

import os
import sys
import re 
import zipfile

DATASETLOCNBASE = "/data/"

def zipDataset(systemName):
    """
    From /data/SYSTEM, 
         /data/SYSTEM/JSONLD (data)
               SYSTEM/about.json
               SYSTEM/META (includes model.json)
    in SYSTEM.zip under /data
    
    If existing one there then it is deleted.
    """
    print "Making dataset zip for", systemName
    systemDatasetBase = DATASETLOCNBASE + systemName + "/"
    zipF = zipfile.ZipFile(DATASETLOCNBASE + systemName + ".zip", "w") # w ensures overwrite
    if not (os.path.isdir(systemDatasetBase + "JSONLD") and os.path.isfile(systemDatasetBase + "about.json") + os.path.isfile(systemDatasetBase + "model.json")):
        raise Exception("Can't zip - missing either JSONLD, about.json or META/model.json")

    # JSON-LD
    added = 0
    for jldFileToAdd in os.listdir(systemDatasetBase + "JSONLD"):
            if not re.search(r'\.jsonld', jldFileToAdd):
                continue
            added += 1
            zipF.write(systemDatasetBase + "JSONLD/" + jldFileToAdd, arcname=systemDatasetBase + "JSONLD/" + jldFileToAdd, compress_type=zipfile.ZIP_DEFLATED)
    print "Added", added, "JLD files ..."
    
    # about
    zipF.write(systemDatasetBase + "about.json", arcname=systemDatasetBase + "about.json", compress_type=zipfile.ZIP_DEFLATED)
    print "Added 'about.json' ..."
    
    # META/model.json
    zipF.write(systemDatasetBase + "META/model.json", arcname=systemDatasetBase + "META/model.json", compress_type=zipfile.ZIP_DEFLATED)    
    print "Added 'model.json' ..."
    
    zipF.close()
    print "Zip in", DATASETLOCNBASE + systemName + ".zip"
    print

def main():

    if len(sys.argv) < 2:
        print "need to specify configuration ex/ VISTAAB - exiting"
        return

    systemName = sys.argv[1].split(".")[0]

    zipDataset(systemName)
        
if __name__ == "__main__":
    main()
