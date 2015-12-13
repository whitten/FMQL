"""
RDF Document Builder

Implements the RDF Builder interface used by various FMQL utilities. Avoids
the need to use an external library like rdflib. This is for straight forward serialization of RDF graphs to XML. XSLT friendly.

Note on inline (vs collection) lists: collections/formal RDF lists are difficult
to SPARQL (and XSLT). For now, we make multiple explicit assertions, inline, for a predicate with more than one value ie/
        <x:pred rdf:resource="http://1"/>
        <x:pred rdf:resource="http://2"/>
vs
        <x:pred rdf:parseType="Collection">
            <rdf:Description rdf:about="http://1"/>
            <rdf:Description rdf:about="http://2"/>            
        </x:pred>
inline listing is very straightforward in turtle: x:pred <http://1>, <http:/2> .

# LICENSE:
# This program is free software; you can redistribute it and/or modify it under the terms of
# the GNU Affero General Public License version 3 (AGPL) as published by the Free Software
# Foundation.
# (c) 2010-2014 caregraf
"""
        
import codecs
import re
import cgi
from datetime import datetime
import StringIO

class RDFBuilder:
    """
    uriBase:
    - default base for URIs of Resources. Ex/ http://www.examplehospital.com/
    
    How to serialize result:
        resultS = rdfb.done()
        with codecs.open(result.rdf", "w", encoding=encoding) as resultFile:
            resultFile.write(resultS.getvalue())
            resultFile.close()
        
    Namespaces:
    - baseNSInfo: the default namespace, ex/ (vs, http://datasets.caregraf.org/vs/)
    - extraNSInfos: ex/ [(cg, http://datasets.caregraf.org/cg/)]
    NOTE: rdf, rdfs, owl are built in namespaces.
    """
    def __init__(self, baseNSInfo, defaultURIBase, output=StringIO.StringIO(), extraNSInfos=[]):
        self.rdf = StringIO.StringIO()
        self.rdf.write('<?xml version="1.0" encoding="utf-8"?>\n<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"\n xmlns:owl="http://www.w3.org/2002/07/owl#"\n')
        self.namespaces = {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "owl": "http://www.w3.org/2002/07/owl#"}
        self.rdf.write(' xmlns:%s="%s"' % (baseNSInfo[0], baseNSInfo[1]))
        self.namespaces[baseNSInfo[0]] = baseNSInfo[1]
        self.baseNSInfo = baseNSInfo
        if len(extraNSInfos):
            self.rdf.write("\n") # space before last ns
        for extraNSInfo in extraNSInfos:
            self.namespaces[extraNSInfo[0]] = extraNSInfo[1]
            self.rdf.write(' xmlns:%s="%s"\n' % (extraNSInfo[0], extraNSInfo[1]))
        self.rdf.write('>\n\n')
        self.defaultBase = defaultURIBase
        self.nodeLevel = 0
        self.bpred = ""
        self.indent = ""
        self.nodesSoFar = {}
        
    def nodesBuilt(self):
        return self.nodesSoFar.keys()
        
    def declareNode(self, node, nodeType):
        """
        Using inline owl:Class ... not rdf:Description + type.
        
        Consider: rdf:ID vs rdf:about ... defaultBase ie/ http://testvista.caregraf.org/
        """
        if not re.search(r':', nodeType):
            raise Exception("Need name spaced type - %s" % nodeType)
        self.rdf.write('\t<%s rdf:about="%s"/>\n\n' % (nodeType, self.__fullURI(node)))
                            
    def startNode(self, node=None, nodeType="", bpred="", force=False):
        """
        Consider:
        owl:Class, not rdf:Description + type
        """
        if node: # "" for Ontology
            node = self.__fullURI(node)
            if node in self.nodesSoFar and not force:
                return False
            self.nodesSoFar[node] = ""
            
        if node == None: # ex/ for custom 63.04 fields
            self.rdf.write('\t<rdf:Description>\n')
            return True
        
        self.nodeLevel += 1
        if self.nodeLevel > 1:
            self.indent = "\t"
            if bpred:
                self.bpred = bpred
            else:
                self.bpred = nodeType.lower() # default name for bnode is its type
            #     <vs:report_text-8925 rdf:datatype="http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"> is rdflib way. May change.
            self.rdf.write('\t\t<%s:%s rdf:parseType="Resource">\n' % (self.baseNSInfo[0], self.bpred))
        else:
            self.rdf.write('\t<rdf:Description rdf:about="%s">\n' % node)
            
        # Simple case of a label => use default name space
        # If fully qualified then check it.
        if nodeType:
            nodeTypeURI = self.__fullURI(nodeType)
            self.rdf.write('%s\t\t<rdf:type rdf:resource="%s"/>\n' % (self.indent, nodeTypeURI))  
            
        return True
                   
    """
    pred - if no namespace, then takes default
    node - if there then stand alone definition for assertion
    """
    def addAssertion(self, pred, value, node=None):
        if node:
            self.rdf.write('\t<rdf:Description rdf:about="%s">\n' % self.__fullURI(node))            
        if value["type"] == "uri":
            self.addURIAssertion(pred, value)
        else:
            self.addLiteralAssertion(pred, value)
        if node:
            self.rdf.write('\t</rdf:Description>\n')
        
    def addURIAssertion(self, pred, value):
        pred = self.__pred(pred)
        uriValue = self.__fullURI(value["value"])
        self.rdf.write('%s\t\t<%s rdf:resource="%s"/>\n' % (self.indent, pred, uriValue))  
        return uriValue
        
    # TBD: embed HTML - JSON and \/. CDATA in tags? Or <div xmlns:...> or ...
    # canon XML ref in RDF Primer
    # http://www.w3.org/TR/2002/REC-xml-exc-c14n-20020718/, http://www.w3.org/TR/2004/REC-rdf-primer-20040210/#xmlliterals
    def addLiteralAssertion(self, pred, value):
        pred = self.__pred(pred)
        # TBD: see NextVistA ... try \/ in JSON but not in ...
        # See if sub \r for <br/> and use pre to display?
        if "datatype" in value and value["datatype"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral":
            # brKeepCopy = re.sub(r'<br\/>', '[BR]', value["value"])
            brNixCopy = re.sub(r'<br\/>', '\n', value["value"])
            escCopy = cgi.escape(brNixCopy)
            # added due to return from FMQL 
            escCopy = escCopy.encode('ascii', 'ignore')
            # cleanCopy = re.sub(r'\[BR\]', '<xhtml:br/>', escCopy) 
            self.rdf.write('%s\t\t<%s rdf:parseType="Literal">%s</%s>\n' % (self.indent, pred, escCopy, pred))
            return
        # ex/ http://www.w3.org/1999/02/22-rdf-syntax-ns#dateTime or http://www.w3.org/2001/XMLSchema#int or #boolean. Can be xsd: too.
        if "datatype" in value:
            litVal = value["value"] if not re.search(r'boolean$', value["datatype"]) else str(value["value"]).lower() # True -> true, False -> false
            self.rdf.write('%s\t\t<%s rdf:datatype="%s">%s</%s>\n' % (self.indent, pred, value["datatype"], litVal, pred))
            return
        self.rdf.write('%s\t\t<%s>%s</%s>\n' % (self.indent, pred, cgi.escape(value["value"]), pred))
        
    def addInlineList(self, pred, values):
        """
        This does not lead to a Collection which is an RDF List (hard to query in SPARQL). Instead it inlines 1 or more values by assertion pred N times. This is much neater in turtle than in XML.
        
        XML:
            <x:pred>VAL1</x:pred>
            <x:pred>VAL2</x:pred>
            <x:pred>VAL3</x:pred>
            
        Turtle:
            x:pred VAL1, VAL2, VAL3 .
            
        Form of URI: <ns1:pred rdf:resource="http://1"/> vs <ns1:pred>1</ns1:pred> or <ns1:pred rdf:datatype="xsd:int">1</ns1:pred>
        """
        pass
        
    # http://www.w3.org/TR/2004/REC-owl-ref-20040210/#Enumeration
    # - can be collection as references Things and not Datatypes
    # see: http://www.w3.org/TR/rdf-primer/#collections
    def startOWLOneOf(self):
        self.rdf.write('%s\t\t<owl:oneOf rdf:parseType="Collection">\n' % self.indent)
    
    def endOWLOneOf(self):
        self.rdf.write('%s\t\t</owl:oneOf>\n' % self.indent)
    
    def addOWLOneOfMember(self, node):
        node = self.__fullURI(node)
        self.rdf.write('%s\t\t\t<owl:Thing rdf:about="%s"/>\n' % (self.indent, node))
        
    # http://www.w3.org/TR/2004/REC-owl-ref-20040210/#unionOf-def
    def startOWLUnionOf(self):
        self.rdf.write('%s\t\t<owl:unionOf rdf:parseType="Collection">\n' % self.indent)
    
    def endOWLUnionOf(self):
        self.rdf.write('%s\t\t</owl:unionOf>\n' % self.indent)
    
    def addOWLUnionOfMember(self, node):
        node = self.__fullURI(node)
        self.rdf.write('%s\t\t\t<owl:Class rdf:about="%s"/>\n' % (self.indent, node))
        
    def addOWLPropertyRestriction(self, onProperty, restrict, value, datatype=""):
        onProperty = self.__fullURI(onProperty)
        self.rdf.write('%s\t\t<rdfs:subClassOf>\n' % self.indent)
        self.rdf.write('%s\t\t\t<owl:Restriction>\n' % self.indent)
        self.rdf.write('%s\t\t\t\t<owl:onProperty rdf:resource="%s"/>\n' % (self.indent, onProperty))
        if not datatype:
            value = self.__fullURI(value)
            self.rdf.write('%s\t\t\t\t<owl:%s rdf:resource="%s"/>\n' % (self.indent, restrict, value))
        else:
            fullDatatype = self.__fullURI(datatype)
            self.rdf.write('%s\t\t\t\t<owl:%s rdf:datatype="%s">%s</owl:%s>\n' % (self.indent, restrict, fullDatatype, value, restrict))        
        self.rdf.write('%s\t\t\t</owl:Restriction>\n' % self.indent)
        self.rdf.write('%s\t\t</rdfs:subClassOf>\n' % self.indent)
        
    def __pred(self, pred):
        if not re.search(r':', pred):
            pred = self.baseNSInfo[0] + ":" + pred
        else:
            ns = re.match(r'([^:]+)', pred).group(1)
            if ns not in self.namespaces:
                raise Exception("Invalid namespace %s passed in predicate %s" % (ns, pred))
        return pred
        
    def __fullURI(self, uriValue):
        """
        Three options:
        - http:...
        - ns:... -> expand
        - ... -> expand with default
        """
        if not re.match(r'http:', uriValue):
            if re.match(r'[^:]+:', uriValue):
                bits = re.match(r'([^:]+):(.+)$', uriValue)
                ns = bits.group(1)
                if ns not in self.namespaces:
                    raise Exception("Invalid namespace %s passed in uri value %s" % (ns, uriValue))
                uriValue = self.namespaces[ns] + bits.group(2)
            else:
                uriValue = self.defaultBase + uriValue
        return uriValue
        
    def endNode(self):
        if self.nodeLevel > 1:
            self.rdf.write('\t\t</%s:%s>\n' % (self.baseNSInfo[0], self.bpred))
            self.nodeLevel = 1
            self.bpred = ""
            self.indent = ""
        else:    
            self.nodeLevel = 0
            self.rdf.write('\t</rdf:Description>\n\n')
        
    def done(self):
        self.rdf.write('</rdf:RDF>')
        self.rdf.seek(0,0)
        return self.rdf 
