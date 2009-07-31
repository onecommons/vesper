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
from rx.RxPathUtils import _encodeStmtObject
import re

RESOURCE_REGEX = re.compile(r'^[\w:/\.\?&\$\-_\+#\@]+$') #XXX

JSON_BASE = 'sjson:schema#' #XXX
PROPSEQ  = JSON_BASE+'propseq'
PROPSEQTYPE = JSON_BASE+'propseqtype'
PROPBAG = JSON_BASE+'propseqprop'

XSD = 'http://www.w3.org/2001/XMLSchema#'

_xsdmap = { XSD+'integer': int,
#XSD+'int': int, #including this prevent RDF round-tripping
XSD+'double' : float,
#XSD+'float' : float,
JSON_BASE+'null' : lambda v: None,
 XSD+'boolean' : lambda v: v=='true' and True or False,
}

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

    def __init__(self, addOrderInfo=True, generateBnode=None, scope = ''):
        self._genBNode = generateBnode
        self.addOrderInfo = addOrderInfo
        self.scope = scope
        
    def lookslikeUriOrQname(self, s):
        #XXX think about customization, e.g. if number were ids
        if not isinstance(s, (str,unicode)):        
            return False
        m = RESOURCE_REGEX.match(s)
        if m is not None:
            return s
        return False
 
    def deduceObjectType(self, item):    
        if isinstance(item, list):
            return item, None
        if isinstance(item, dict):
            if 'value' not in item or len(item) != 3:
                return item, None
            value = item['value']
            objectType = item.get('datatype')
            if not objectType:
                objectType = item.get('xml:lang')
            if not objectType:                
                return item, None
            else:
                return value, objectType 

        res = self.lookslikeUriOrQname(item)
        if res:
            return res, OBJECT_TYPE_RESOURCE
        elif item is None:
            return 'null', JSON_BASE+'null'
        elif isinstance(item, bool):
            return (item and 'true' or 'false'), XSD+'boolean'
        elif isinstance(item, int):
            return unicode(item), XSD+'integer'
        elif isinstance(item, float):
            return unicode(item), XSD+'double'
        else:
            return item, OBJECT_TYPE_LITERAL

    def _value(self, node):
        from rx import RxPathDom
        if isinstance(node.parentNode, RxPathDom.BasePredicate):
            stmt = node.parentNode.stmt
            if len(stmt.objectType) > 1:
                valueparse = _xsdmap.get(stmt.objectType)
                if valueparse:
                    return valueparse(node.data)
                else:
                    return _encodeStmtObject(stmt)
        return node.data

    def _blank(self):
        if self._genBNode:
            return self._genBNode()
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

    def _setPropSeq(self, propseq, idrefs):
        #XXX what about empty lists?
        from rx import RxPathDom
        childlist = []
        propbag = None
        for p in propseq.childNodes:
            prop = p.stmt.predicate
            obj = p.childNodes[0]
            #print '!!propseq member', p.stmt
            if prop == PROPBAG:
                propbag = obj.uri
            elif prop == RxPath.RDF_SCHEMA_BASE+u'member':
                if isinstance(obj, RxPathDom.Text):
                    childlist.append( self._value(obj) )
                elif obj.uri == RDF_MS_BASE + 'nil':
                    childlist.append( [] )
                else: #otherwise it's a resource
                    childlist.append( self.QName(obj.uri) )
                    #XXX test this: if obj.uri != res.uri: #object isn't same as subject
                    key = len(childlist)-1
                    idrefs.setdefault(obj.uri, []).append((childlist, key))
        return propbag, childlist

    def _to_sjson(self, root, depth=-1, exclude_blankids=False):
        #1. build a list of subjectnodes
        #2. map them to object or lists, building id => [ objrefs ] dict
        #3. iterate through id map, if number of refs == 1 set in object, otherwise add to shared

        #use RxPathDom, expensive but arranges as sorted tree, normalizes RDF collections et al.
        #and is schema aware
        from rx import RxPathDom
        if not isinstance(root, RxPathDom.Node):
            #assume doc is iterator of statements or quad tuples
            #note: order is not preserved
            root = RxPath.createDOM(RxPath.MemModel(root), schemaClass=RxPath.BaseSchema)

        #step 1: build a list of subjectnodes
        if isinstance(root, (RxPathDom.Document, RxPathDom.DocumentFragment)):
            if isinstance(root, RxPathDom.Document):
                listnodes = []
                nodes = []
                for n in root.childNodes:
                    if not n.childNodes:
                        #filter out resources with no properties
                        continue
                    if n.matchName(JSON_BASE,'propseqtype'):
                        listnodes.append(n)
                    else:
                        nodes.append(n)
            else:
                nodes = [n for n in root.childNodes]
            #from pprint import pprint
            #pprint(nodes)
            todo = nodes
        elif isinstance(root, RxPathDom.Resource):
            todo = [root]
        elif isinstance(root, RxPathDom.BasePredicate):
            #XXX
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

        #step 2: map them to object or lists, building id => [ objrefs ] dict
        #along the way
        results = {}
        lists = {}
        idrefs = {}

        for listnode in listnodes:
            seqprop, childlist = self._setPropSeq(listnode, idrefs)
            lists[listnode.uri] = (seqprop, childlist)

        for res in todo:
            if not res.childNodes:
                #no properties
                continue
            currentobj = { self.ID : res.uri }
            currentlist = []
            propseqs = {}
            #print 'adding to results', res.uri
            results[res.uri] = currentobj
            #print 'res', res, res.childNodes
            for p in res.childNodes:
                prop = p.stmt.predicate                
                if prop == PROPSEQ:
                    #this will replace sequences
                    #seqprop, childlist = self._setPropSeq(p.childNodes[0], res, idrefs)
                    seqprop, childlist = lists[p.stmt.object]
                    key = self.QName(seqprop)
                    currentobj[ key ] = childlist
                    #print 'adding propseq', p.stmt.object
                    lists[ p.stmt.object ] = childlist
                    #idrefs.setdefault(p.stmt.object, []).append( (currentobj, key) )

            for p in res.childNodes:
                prop = p.stmt.predicate                
                if prop == PROPSEQ:
                    continue
                key = self.QName(prop)
                if key in currentobj:
                    assert key != self.ID
                    continue #must have be already handled by getPropSeq

                nextMatches = p.nextSibling and p.nextSibling.stmt.predicate == prop
                #XXX Test empty and singleton rdf lists and containers
                if nextMatches or currentlist:
                    parent = currentlist
                    key = len(currentlist)
                    currentlist.append(0)
                else:
                    parent = currentobj

                obj = p.childNodes[0]
                if isinstance(obj, RxPathDom.Text):
                    parent[ key ] = self._value(obj)
                elif obj.uri == RDF_MS_BASE + 'nil':
                    parent[ key ] = []
                else: #otherwise it's a resource
                    #print 'prop key', key, prop, type(parent)
                    parent[ key ] = self.QName(obj.uri)
                    if obj.uri != res.uri: #object isn't same as subject
                        idrefs.setdefault(obj.uri, []).append( (parent, key) )

                if currentlist and not nextMatches:
                    #done with this list
                    currentobj[ prop ] = currentlist
                    currentlist = []

        #3. iterate through id map, if number of refs == 1 set in object, otherwise add to shared
        shared = []
        unresolvedRefs = []
        for id, refs in idrefs.items():
            if len(refs) == 1:
                obj, key = refs[0]
                if id in results:
                    obj[key] = results[id]
                    del results[id]
                elif id in lists:
                    obj[key] = lists[id][1]
                    del lists[id]
                else:
                    #print id, 'not found in', results, 'or', lists
                    unresolvedRefs.append(id)
            else:
                #print 'refs', refs
                shared.append(id)

        results = results.values()
        #if shared:
        #    results = { 'results':results, 'shared':shared}
        #if self.nsmap:
        #    results['prefix'] = self.nsmap
        return results

    def to_sjson(self, root, depth=-1):
        results = self._to_sjson(root, depth)
        return json.dumps(results) #a list

    def to_rdf(self, json, scope = None):        
        m = RxPath.MemModel()
        if scope is None:
            scope = self.scope

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

        def _createNestedList(val):
            assert isinstance(val, list)
            if not val:
                return RDF_MS_BASE+'nil'

            seq = self._blank()
            m.addStatement( Statement(seq, RDF_MS_BASE+'type',
                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(seq, RDF_MS_BASE+'type',
                PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) )

            for i, item in enumerate(val):
                item, objecttype = self.deduceObjectType(item)
                if isinstance(item, dict):
                    itemid, idprop = getorsetid(item)
                    m.addStatement( Statement(seq,
                        RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, scope) )
                    todo.append(item)
                elif isinstance(item, list):
                    nestedlistid = _createNestedList(item)
                    m.addStatement( Statement(seq,
                            RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, scope) )
                else: #simple type
                    m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, scope) )
            return seq
        
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
                val, objecttype = self.deduceObjectType(val)
                if isinstance(val, dict):
                    objid, idprop = getorsetid(val) 
                    m.addStatement( Statement(id, prop, objid, OBJECT_TYPE_RESOURCE, scope) )    
                    todo.append(val)
                elif isinstance(val, list):
                    #dont build a PROPSEQTYPE if prop in rdf:_ rdf:first rdfs:member                
                    specialprop = prop.startswith(RDF_MS_BASE+'_') or prop in [
                                  RDF_MS_BASE+'first', RDF_SCHEMA_BASE+'member']
                    addOrderInfo = not specialprop and self.addOrderInfo
                    #XXX special handling for prop == PROPSEQ ?
                    if not val:
                        m.addStatement( Statement(id, prop, RDF_MS_BASE+'nil', 
                                                OBJECT_TYPE_RESOURCE, scope) )
                    elif addOrderInfo:
                        seq = self._blank() 
                        m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                            RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
                        m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                            PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) )
                        m.addStatement( Statement(seq, PROPBAG, prop, 
                            OBJECT_TYPE_RESOURCE, scope) )
                        m.addStatement( Statement(id, PROPSEQ, seq, OBJECT_TYPE_RESOURCE, scope) )
                    
                    for i, item in enumerate(val):
                        item, objecttype = self.deduceObjectType(item)
                        if isinstance(item, dict):
                            itemid, idprop = getorsetid(item)
                            m.addStatement( Statement(id, prop, itemid, OBJECT_TYPE_RESOURCE, scope) )  
                            if addOrderInfo:
                                m.addStatement( Statement(seq, 
                                    RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, scope) )
                            todo.append(item)
                        elif isinstance(item, list):                        
                            nestedlistid = _createNestedList(item)
                            m.addStatement( Statement(id, prop, nestedlistid, OBJECT_TYPE_RESOURCE, scope) )
                            if addOrderInfo:
                                m.addStatement( Statement(seq,
                                    RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, scope) )
                        else:
                            #simple type                            
                            m.addStatement( Statement(id, prop, item, objecttype, scope) )
                            if addOrderInfo:
                                m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, scope) )
                else: #simple type
                    m.addStatement( Statement(id, prop, val, objecttype, scope) )                    

        return m.getStatements()

def tojson(statements, options=None):
    options = options or {}
    return sjson(**options)._to_sjson(statements)

def tostatements(contents, options=None):
    options = options or {}
    return sjson(**options).to_rdf( {
        'results' : contents
        })
