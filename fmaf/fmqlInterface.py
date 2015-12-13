#!/usr/bin/env python

#
# LICENSE:
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Affero General Public License version 3 (AGPL) 
# as published by the Free Software Foundation.
# (c) 2010-2014 caregraf
#

import urllib, urllib2

class FMQLInterface(object):
    """
    Hides Rest EP
    """
    def __init__(self, ep, queryArg="fmql"):
        self.ep = ep
        self.queryArg = queryArg # may be "query"
        
    def invokeRPC(self, name, params):
        # to match broker if for now
        return self.invokeQuery(params[0]).read()
        
    def invokeQuery(self, query):
        queryURL = self.ep + "?" + urllib.urlencode({self.queryArg: query}) 
        return urllib2.urlopen(queryURL)
