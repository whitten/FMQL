class CacheObjectInterface:

    def __init__(self, ep):
        # ex/ http://...../FMQL.csp
        self.ep = 
        self.cookie = ""
        
    def invokeRPC(self, name, params):
        # to match broker if for now
        return self.invokeQuery(params)
        
    def invokeQuery(self, query):
        queryDict = {"query": query}
        queryurl = self.ep + "?" + urllib.urlencode(queryDict)
        request = urllib2.Request(queryurl)
        if self.cookie:
            request.add_header('cookie', self.cookie)
        response = urllib2.urlopen(request)
        if not self.cookie:
            # SET-COOKIE: CSPSESSIONID-SP-57772-UP-csp-fmquery-=0010000100002g3gWldo9l0000fAj6SQgextDm2AmskX7GxQ--; path=/csp/fmquery/;  httpOnly;
            self.cookie = response.info().getheader('Set-Cookie')  
        return response      