'''
    Copyright (c) 2009 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net 
    
    
    = sjson redux =

    * objects are resources (except for ref objects)
          * if it has an 'id' property it is treated as resource, where the value of 'id' is the resource URI
          * otherwise, assign a bnode
          * statement for empty object add this statement bnode rdf:type rdfs:Resource. 
    * each object property maps to a RDF property
          * by default, for updates, assume an RDF model where owl:maxCardinality = 1 for every property 
    * strings that match this regex: URI|jname are assumed to be a reference to resource, unless the jname matches "string value namespace"
    * prop : [] are represented in RDF as
      prop : value, 
      prop : value
      prop-seq:
      {
      type: propseqclass //subclass of RDF$seq,
      prop-bag : prop, //domain propseqclass, range: Propery
      _1: value,  _2: value //note: can contain duplicate values
      }

Note: RDF collection and containers will appears as objects look like:
{
rdfs:member : [...]
rdf:first : [...]
}

    
    * for comparison: shindig/canonicaldb.json's pattern:
{ 
    type : [ resources ],
    prop : {
     'idref' : [ values ]
    }
}
    to conform to pattern, could rewrite as:
{  
   { id : type, type-members : [ resources] },
   { subject : idref,
     property : prop
     object : [ values ]
   }
}
  more naturally:
{
 { resource }, //include type property
 { id : idref,
   prop : [ values] 
 }
}
to avoid prop naming redundancy without special-case semantics:
{
  id : prop
  distribute-prop : [
    ['id', [ values ]],
    ['id', [ values ]] 
  ]
}
slightly special case (add reserved word 'propertymap') (top-level only):
{
  'propertymap' : prop,
  'id1' : value,
  'id2' : value
}
{ id : idref, prop: values} for each idref
thus the shindig schema could look like:
{  
   { id : type, type-members : [ resources] },
   { 'propertymap' : prop,
     'idref' : values
    }
}
 how about: (top-level only)
{ 'idmap' : { /*defaults e.g. type : foo */ }
   'id' : {}
}
constructs items 
{ id : idref
  prop : values 
}
//$ is delimiter 
{ 
  nsmap : {  } 
}

    * depending on the schema, lists can represent either duplicate properties of the same name, rdf containers or rdf collections.
          o default, treat as blank node of type rdf:Seq (to perserve order, more efficient then rdf:List 
    * string, numbers and null are treated as literals (define json datatypes for number and null? or use xsd?)
    * references to resources are spelled like this: { $ref : id }
    * if "root" of the json maps to a literal:
          o construct a statement like: <somerootURI> hasJson "literal". 
    * when to use $ref and when to inline the object? If the resource occurs once, inline.
    * when to include id : _bnode and when to omit? By default, include them.
          o we can omit as long as those objects don't need to updated or if the implementation has a way to figure out what bnode is.    
'''

try:
    import json
except ImportError:
    import simplejson as json

from rx import RxPath    
from rx.RxPath import Statement, OBJECT_TYPE_RESOURCE, RDF_MS_BASE, RDF_SCHEMA_BASE, OBJECT_TYPE_LITERAL
import re

RESOURCE_REGEX = re.compile(r'^[\w:/\.\?&\$\-_\+#\@]+$') #XXX

JSON_BASE = 'sjson:schema/' #XXX
PROPSEQ  = JSON_BASE+'propseq'
PROPSEQTYPE = JSON_BASE+'propseqtype'
PROPBAG = JSON_BASE+'propseqprop'

def _expandqname(qname, nsmap):
    #assume reverse sort of prefix
    for ns, prefix in nsmap:
        if qname.startswith(prefix+'$'):
            suffix = qname[len(prefix)+1:]
            return ns+suffix
    return qname

class sjson(object):
    
    nsmap=[(JSON_BASE,'')]
    
    bnodecounter = 0
    bnodeprefix = '_:'

    ID = property(lambda self: self.QName(JSON_BASE+'id'))
    PROPERTYMAP = property(lambda self: self.QName(JSON_BASE+'propertymap'))

    def lookslikeUriOrQname(self, s):
        #XXX think about customization, e.g. if number were ids
        if not isinstance(s, (str,unicode)):        
            return False
        m = RESOURCE_REGEX.match(s)
        return m is not None
        
    def _value(self, node):
        #XXX: handle datatype
        return node.data

    def _blank(self):
        self.bnodecounter+=1
        return self.bnodeprefix + str(self.bnodecounter)
    
    def QName(self, prop):
        #reverse sorted so longest comes first
        for ns, prefix in self.nsmap:
            if prop.startswith(ns):
                suffix = prop[len(ns):]
                if prefix:
                    return prefix+'$'+suffix 
                else:
                    return suffix
        return prop

    def _to_sjson(self, root, depth=-1, exclude_blankids=False):
        """
        If resource is a references more than once, just the string is output
        RDF lists and containers that are not SJSON sequences
        
        >>> r = Res("http://example.org/book#1"); r['v1'] = 'string'; r['v2'] = 1;
        >>> "http://example.org/book#2"
        >>> r['l'] = [1, 2, 3, 4, 5]; r['r'] = Res('o')
        
        >>> sjson().to_sjson(doc())
        """
        #XXX depth
        #XXX exclude_blankids (but if false, will need to add back if shared)
        
        #use RxPathDom, expensive but arranges as sorted tree, normalizes RDF collections et al. 
        #and is schema aware
        from rx import RxPathDom
        if not isinstance(root, RxPathDom.Node):
            #assume doc is iterator of statements or quad tuples
            #note: order is not preserved
            root = RxPath.createDOM(RxPath.MemModel(root), schemaClass=RxPath.BaseSchema) 
        
        results = []
        seen = {}   
        shared = {}         
        if isinstance(root, (RxPathDom.Document, RxPathDom.DocumentFragment)): #XXX RxPathDom.DocumentFragment
            if isinstance(root, RxPathDom.Document):
                #filter out propseq resources and resources with no properties            
                nodes = [n for n in root.childNodes if n.childNodes and
                            not n.matchName(JSON_BASE,'propseqtype')]
            else:
                nodes = [n for n in root.childNodes]
            #from pprint import pprint
            #pprint(nodes)
            results = [{} for i in xrange(0, len(nodes))]
            todo = [(results, i, n) for i,n in enumerate(nodes)]            
        elif isinstance(root, RxPathDom.Resource):
            results = [ {} ]
            todo = [(results, 0, root)]
        elif isinstance(root, RxPathDom.BasePredicate): 
            obj = p.childNodes[0]
            key = self.QName(root.parentNode.uri)
            propmap = { self.PROPERTYMAP : self.QName(root.stmt.predicate) }
            if isinstance(obj, RxPathDom.Text):
                v = self._value(obj)
                todo = []
            else:
                v = {}                
                todo = [ (propmap, key, obj) ]
            propmap[key] = v
            results = [propmap]
        elif isinstance(root, RxPathDom.Text):
            #return string value
            return self._value(root);
        else:                    
            raise TypeError('Unexpected root node')
 
        def setobj(obj, res, parent, key):
            if isinstance(obj, RxPathDom.Text):
                v = self._value(obj)
                #otherwise its a resource
            elif obj.uri == res.uri: #object is same as subject
                v = self.QName(obj.uri)                
            elif obj.uri == RDF_MS_BASE + 'nil':
                v = [] #empty list
            else: 
                uri = obj.uri                     
                #if an object appears in the tree more than once,
                #replace prior reference with uri 
                #and add to shared     
                prior = seen.get(uri)
                #print 'seen', uri, prior
                if prior:
                    v = self.QName(uri)
                    shared[v] = prior[0][ prior[1] ]
                    prior[0][ prior[1] ] = v            
                else:
                    v = {}
                    #print 'add to seen', uri, type(uri)
                    seen[uri] = (parent, key, v)
                    todo.append( (parent, key, obj) )
            parent[ key ] = v
        
        def setPropSeq(propseq, res):
            #XXX what about empty lists?
            childlist = []
            for p in propseq.childNodes:
                prop = p.stmt.predicate
                obj = p.childNodes[0] 
                if prop == PROPBAG:
                    propbag = obj.uri
                elif prop == RxPath.RDF_SCHEMA_BASE+u'member':
                    childlist.append(0)
                    setobj(obj, res, childlist, len(childlist)-1)         
            return propbag, childlist
    
        while todo:
            #res (the resource) has already been attached to its parent
            #now we need to assign its properties
            parent, key, res = todo.pop()        
            if res.uri not in seen:
                #print 'add subject to seen', res.uri, type(res.uri)
                seen[res.uri] = (parent, key, parent[key])
            else:
                #print 'subject seen', res.uri
                parent[key] = res.uri #replace object with uri reference
                prior = seen[res.uri]
                key = self.QName(res.uri)                
                if key in shared:
                    continue
                else:
                    #add to shared
                    shared[key] = prior[2]
                    parent = shared                
            
            if res.childNodes:
                s = parent[key]
                s[self.ID] = res.uri
            else:
                #no properties, just write out the id, not a dict
                #and don't bother including it in shared
                #XXX but then we can't tell if its an id or just a string?
                if parent is shared:
                    del parent[key]
                else:                    
                    parent[key] = res.uri
                continue
                
            currentlist = []
            propseqs = {}
            
            for p in res.childNodes:  
                prop = p.stmt.predicate
                if prop == PROPSEQ:
                    #this will replace sequences                    
                    seqprop, childlist = setPropSeq(p.childNodes[0], res)
                    s[ self.QName(seqprop) ] = childlist

            for p in res.childNodes:  
                prop = p.stmt.predicate
                if prop == PROPSEQ:
                    continue
                if self.QName(prop) in s:
                    continue #must have be already handled by getPropSeq

                nextMatches = p.nextSibling and p.nextSibling.stmt.predicate == prop                
                #XXX Test empty and singleton rdf lists and containers                                
                if nextMatches or currentlist:
                    parent = currentlist
                    key = len(currentlist)
                    currentlist.append(0)                     
                else:
                    parent = s
                    key = self.QName(prop)
                
                obj = p.childNodes[0]
                setobj(obj, res, parent, key)
                
                if currentlist and not nextMatches:
                    s[ self.QName(prop) ] = currentlist
                    currentlist = [] #list done

        if shared:
            results = { 'results':results, 'shared':shared}

        #if self.nsmap:        
        #    results['prefix'] = self.nsmap
        
        return results

    def to_sjson(self, root, depth=-1):
        results = self._to_sjson(root, depth)
        return json.dumps(results) #a list

    def to_rdf(self, json):
        scope = ''
        m = RxPath.MemModel()
        
        if isinstance(json, (str,unicode)):
            todo = json = json.loads(json)            
        if isinstance(json, dict):
            todo = [r for r in json.get('results',[])
                              if isinstance(r, dict)]
            if 'shared' in json:
                todo.extend( json['shared'].values() )            
        else:
            todo = json 
        if not isinstance(todo, list):
            raise TypeError('whats this?')

        #nsmapstack = [ self.nsmap.copy() ]
        nsmap = self.nsmap
        
        def getorsetid(obj):
            #nsmap = nsmapstack.pop()
            nsmapprop = _expandqname('nsmap', nsmap) 
            nsmapval = obj.get(nsmapprop)
            if nsmapval is not None:
                pass #XXX update stack            
            idprop = _expandqname('id', nsmap) 
            id = obj.get(idprop)
            if id is None:
                id = self._blank() #XXX
                obj[idprop] = id
            return id, idprop
        
        while todo:
            obj = todo.pop()
            #XXX if obj.nsmap: push nsmap
            #XXX propmap
            #XXX idmap
            id, idprop = getorsetid(obj) 
                            
            for prop, val in obj.items():
                if prop == idprop:                    
                    continue
                prop = _expandqname(prop, nsmap)
                
                if isinstance(val, dict):
                    objid, idprop = getorsetid(val) 
                    m.addStatement( Statement(id, prop, objid, OBJECT_TYPE_RESOURCE, scope) )    
                    todo.push(val)
                elif isinstance(val, list):
                    #dont build a PROPSEQTYPE if prop in rdf:_ rdf:first rdfs:member                
                    specialprop = prop.startswith(RDF_MS_BASE+'_') or prop in [
                                  RDF_MS_BASE+'first', RDF_SCHEMA_BASE+'member']
                    #XXX special handling for prop == PROPSEQ ?
                    if not specialprop:
                        if not val:
                            m.addStatement( Statement(id, prop, RDF_MS_BASE+'nil', 
                                OBJECT_TYPE_RESOURCE, scope) )
                        else:  
                            seq = self._blank() 
                            m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
                            m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                                PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) )
                            m.addStatement( Statement(seq, PROPBAG, prop, 
                                OBJECT_TYPE_RESOURCE, scope) )
                            m.addStatement( Statement(id, PROPSEQ, seq, OBJECT_TYPE_RESOURCE, scope) )
                    for i, item in enumerate(val):
                        if isinstance(item, dict):
                            itemid, idprop = getorsetid(val) #XXX
                            m.addStatement( Statement(id, prop, itemid, OBJECT_TYPE_RESOURCE, scope) )  
                            if not specialprop:
                                m.addStatement( Statement(seq, 
                                    RDF_MS_BASE+'_'+str(i+1), id, OBJECT_TYPE_RESOURCE, scope) )
                            todo.push(val)
                        elif isinstance(item, list):                        
                            pass #XXX nested lists: add JSONSEQ
                        else:
                            #simple type
                            if self.lookslikeUriOrQname(item):
                                objecttype = OBJECT_TYPE_RESOURCE
                            else:
                                objecttype = OBJECT_TYPE_LITERAL
                            m.addStatement( Statement(id, prop, item, objecttype, scope) )
                            if not specialprop:
                                m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, scope) )
                else: #simple type
                    if self.lookslikeUriOrQname(val):
                        objecttype = OBJECT_TYPE_RESOURCE
                    else:
                        objecttype = OBJECT_TYPE_LITERAL
                    m.addStatement( Statement(id, prop, val, objecttype, scope) )                    

        return m.getStatements()
