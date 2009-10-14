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
      prop : prop_seq-bnode      
      //prop-seq:
      {
      id : prop_seq-bnode,      
      type: propseqclass //subclass of RDF$seq,
      #prop-bag : prop, //domain propseqclass, range: Propery
      _1: value,  _2: value //note: can contain duplicate values      
      }
   * nested list:
    {
    prop : [ ['value1' ] ]
    }
    prop : outer-prop_seq-bnode
    prop : inner-seq-bnode //cuz its a value
      {
      id : outer-prop_seq-bnode,      
      type: propseqclass, //subclass of RDF$seq,
      _1: inner-seq-bnode      
      }
      
      {
      id : inner-seq-bnode,      
      type: standalone-seqclass, //subclass of RDF$seq,
      _1: value1      
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

from rx.python_shim import *
from rx import RxPath    
from rx.RxPath import Statement, StatementWithOrder, OBJECT_TYPE_RESOURCE, RDF_MS_BASE, RDF_SCHEMA_BASE, OBJECT_TYPE_LITERAL
from rx.RxPathUtils import encodeStmtObject
import re

JSON_BASE = 'sjson:schema#' #XXX
PROPSEQ  = JSON_BASE+'propseq'
PROPSEQTYPE = JSON_BASE+'propseqtype'
STANDALONESEQTYPE = JSON_BASE+'standalongseqtype'
PROPBAG = JSON_BASE+'propseqprop'
PROPSUBJECT = JSON_BASE+'propseqsubject'

XSD = 'http://www.w3.org/2001/XMLSchema#'

_xsdmap = { XSD+'integer': int,
XSD+'double' : float,
JSON_BASE+'null' : lambda v: None,
 XSD+'boolean' : lambda v: v=='true' and True or False,
}

#this map prevents RDF round-tripping, only use if preserveRdfTypeInfo == False
_xsdExtendedMap = { 
XSD+'decimal': float, #note: decimal.Decimal is not json serializable
XSD+'int': int, 
XSD+'float' : float,
}

def _expandqname(qname, nsmap):
    #assume reverse sort of prefix
    return qname
    #XXX    
    for ns, prefix in nsmap:
        if qname.startswith(prefix+'$'):
            suffix = qname[len(prefix)+1:]
            return ns+suffix
    return qname

_defaultBNodeGenerator = 'uuid'

def toJsonValue(data, objectType, preserveRdfTypeInfo=False):
    if len(objectType) > 1:
        valueparse = _xsdmap.get(objectType)
        if valueparse:
            return valueparse(data)
        elif preserveRdfTypeInfo:
            return encodeStmtObject(data, objectType)
        else:
            valueparse = _xsdExtendedMap.get(objectType)
            if valueparse:
                return valueparse(data)
    else:
        return data

class sjson(object):    
    #XXX need separate output nsmap for serializing
    #this nsmap shouldn't be the default for that
    nsmap=[(JSON_BASE,'')] 
    
    bnodecounter = 0
    bnodeprefix = '_:'
    
    ID = property(lambda self: self.QName(JSON_BASE+'id'))
    PROPERTYMAP = property(lambda self: self.QName(JSON_BASE+'propertymap'))
        
    def __init__(self, addOrderInfo=True, generateBnode=_defaultBNodeGenerator, 
                            scope = '', model=None, preserveRdfTypeInfo=True, 
                                                          setBNodeOnObj=False,
                                                          refPrefix=''):
        self._genBNode = generateBnode
        if generateBnode == 'uuid': #XXX hackish
            self.bnodeprefix = RxPath.BNODE_BASE
        self.addOrderInfo = addOrderInfo
        self.preserveRdfTypeInfo = preserveRdfTypeInfo
        self.refPrefix = refPrefix
        self.RESOURCE_REGEX = re.compile(r'^%s([\w:/\.\?&\$\-_\+#\@]+)$' % self.refPrefix)
        self.scope = scope
        self.model = model
        self.setBNodeOnObj = setBNodeOnObj

    def _value(self, node):
        from rx import RxPathDom
        if isinstance(node.parentNode, RxPathDom.BasePredicate):
            stmt = node.parentNode.stmt
            return toJsonValue(node.data, stmt.objectType, 
                                        self.preserveRdfTypeInfo)
        return node.data
    
    def QName(self, prop):
        '''
        convert prop to QName
        '''
        #reverse sorted so longest comes first
        for ns, prefix in self.nsmap:
            if prop.startswith(ns):
                suffix = prop[len(ns):]
                if prefix:
                    return prefix+'$'+suffix 
                else:
                    return suffix
        return prop

    def serializeRef(self, uri):
        return self.refPrefix+self.QName(uri) 
    
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
                    childlist.append( self.serializeRef(obj.uri) )
                    key = len(childlist)-1
                    idrefs.setdefault(obj.uri, []).append((childlist, key))
        return propbag, childlist

    def createObjectRef(self, id, obj, isshared):
        '''

        obj: The object referenced by the id, if None, the object was not encountered
        '''
        if obj is not None and not isshared:
            return obj
        else:
            if not isshared and self.model:
                #look up and serialize the resource
                #XXX pass on options like includesharedrefs
                #XXX pass in idrefs
                #XXX add depth option
                results = self._to_sjson(self.model.getStatements(id)) 
                objs = results.get('results')
                if objs:
                    return objs[0]                    
            return id

    def _to_sjson(self, root, includesharedrefs=False, exclude_blankids=False):
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
        elif isinstance(root, RxPathDom.Resource):
            nodes = [root]
        elif isinstance(root, RxPathDom.BasePredicate):
            #XXX
            obj = p.childNodes[0]
            key = self.QName(root.parentNode.uri)
            propmap = { self.PROPERTYMAP : self.QName(root.stmt.predicate) }
            if isinstance(obj, RxPathDom.Text):
                v = self._value(obj)
                nodes = []
            else:
                v = {}
                nodes = [obj]
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

        for res in nodes:
            if not res.childNodes:
                #no properties
                continue
            currentobj = { self.ID : res.uri }
            currentlist = []
            #print 'adding to results', res.uri
            results[res.uri] = currentobj
            #print 'res', res, res.childNodes
            
            #deal with sequences first
            for p in res.childNodes:
                if p.stmt.predicate == PROPSEQ:
                    #this will replace sequences                    
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
                    parent[ key ] = self.serializeRef(obj.uri)
                    if includesharedrefs or obj.uri != res.uri:
                        #add ref if object isn't same as subject
                        idrefs.setdefault(obj.uri, []).append( (parent, key) )

                if currentlist and not nextMatches:
                    #done with this list
                    currentobj[ prop ] = currentlist
                    currentlist = []

        #3. iterate through id map, if number of refs == 1 set in object, otherwise add to shared
        roots = results.copy()
        for id, refs in idrefs.items():
            isshared = includesharedrefs or len(refs) > 1
            obj = None
            if id in results:
                obj = results[id]
            elif id in lists:
                obj = lists[id][1]
            #else:
            #   print id, 'not found in', results, 'or', lists
            ref = self.createObjectRef(id, obj, isshared)
            if ref != id:
                if obj is None:
                    #createObjectRef created an obj from an unreferenced obj,
                    #so add it to the result
                    results[id] = ref
                else:
                    #remove since the obj is referenced
                    roots.pop(id, None)
                for obj, key in refs:
                    obj[key] = ref

        retval = { 'results': roots.values() }
        if not includesharedrefs:
            retval['objects'] = results
        #if self.nsmap:
        #    retval['prefix'] = self.nsmap
        return retval

    def to_sjson(self, root):
        results = self._to_sjson(root)
        results = results['results']
        return json.dumps(r.values()) #a list

    def to_rdf(self, json, scope = None):        
        m = RxPath.MemModel() #XXX        

        if scope is None:
            scope = self.scope

        parentid = '' 
        #nsmapstack = [ self.nsmap.copy() ]
        nsmap = self.nsmap

        def getorsetid(obj):
            #nsmap = nsmapstack.pop()
            nsmapprop = _expandqname('nsmap', nsmap) 
            nsmapval = obj.get(nsmapprop)
            if nsmapval is not None:
                pass #XXX update stack            
            idprop = _expandqname('id', nsmap) 
            objId = obj.get(idprop)
            if objId is None:  
                #mark bnodes for nested objects differently                
                prefix = parentid and 'j:e:' or 'j:t:'
                suffix = parentid and (parentid + ':') or ''
                objId = self._blank(prefix+'object:'+suffix)
                if self.setBNodeOnObj:
                    obj[idprop] = objId
            return objId, idprop

        if isinstance(json, (str,unicode)):
            todo = json = json.loads(json)            
        if isinstance(json, dict):
            todo = [json]
        else:
            todo = list(json)
        if not isinstance(todo, list):
            raise TypeError('whats this?')
        todo = [ (x, getorsetid(x), '') for x in todo]
                
        def _createNestedList(val):
            assert isinstance(val, list)
            if not val:
                return RDF_MS_BASE+'nil'
            assert parentid
            prefix = parentid and 'j:e:' or 'j:t:'
            suffix = parentid and (parentid + ':') or ''            
            seq = self._blank(prefix+'list:'+ suffix)
            m.addStatement( Statement(seq, RDF_MS_BASE+'type',
                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(seq, RDF_MS_BASE+'type',
                PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) ) #XXX STANDALONESEQTYPE

            for i, item in enumerate(val):
                item, objecttype = self.deduceObjectType(item)
                if isinstance(item, dict):
                    itemid, idprop = getorsetid(item)
                    m.addStatement( Statement(seq,
                        RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, scope) )
                    todo.append( (item, (itemid, idprop), parentid))
                elif isinstance(item, list):
                    nestedlistid = _createNestedList(item)
                    m.addStatement( Statement(seq,
                            RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, scope) )
                else: #simple type
                    m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, scope) )
            return seq
        
        while todo:
            obj, (id, idprop), parentid = todo.pop(0)
                        
            #XXX support top level lists: 'list:' 
            assert isinstance(obj, dict), "only top-level dicts support right now"            
            #XXX if obj.nsmap: push nsmap
            #XXX propmap
            #XXX idmap
            if not self.isEmbeddedBnode(id): 
                #this object isn't embedded so set it as the new parent
                parentid = id
            
            for prop, val in obj.items():
                if prop == idprop:                    
                    continue
                prop = _expandqname(prop, nsmap)
                val, objecttype = self.deduceObjectType(val)
                if isinstance(val, dict):
                    objid, idprop = getorsetid(val) 
                    m.addStatement( Statement(id, prop, objid, OBJECT_TYPE_RESOURCE, scope) )    
                    todo.append( (val, (objid, idprop), parentid) )
                elif isinstance(val, list):
                    #dont build a PROPSEQTYPE if prop in rdf:_ rdf:first rdfs:member                
                    specialprop = prop.startswith(RDF_MS_BASE+'_') or prop in [
                                  RDF_MS_BASE+'first', RDF_SCHEMA_BASE+'member']
                    addOrderInfo = not specialprop and self.addOrderInfo
                    #XXX special handling for prop == PROPSEQ ?
                    if not val:
                        m.addStatement( Statement(id, prop, RDF_MS_BASE+'nil', 
                                                OBJECT_TYPE_RESOURCE, scope) )
                        
                    #to handle dups, build itemdict
                    itemdict = {}
                    for i, item in enumerate(val):               
                        item, objecttype = self.deduceObjectType(item)
                        if isinstance(item, dict):
                            itemid, idprop = getorsetid(item)
                            pos = itemdict.get((itemid, OBJECT_TYPE_RESOURCE))                            
                            if pos:
                                pos.append(i)
                            else:
                                itemdict[(itemid, OBJECT_TYPE_RESOURCE)] = [i]                                                                
                                todo.append( (item, (itemid, idprop), parentid) )
                        elif isinstance(item, list):                        
                            nestedlistid = _createNestedList(item)
                            itemdict[(nestedlistid, OBJECT_TYPE_RESOURCE)] = [i]                                                                                            
                        else:
                            #simple type
                            pos = itemdict.get( (item, objecttype) )                            
                            if pos:
                                pos.append(i)
                            else:
                                itemdict[(item, objecttype)] = [i]                                                                                            
                    
                    listStmts = []
                    for (item, objecttype), pos in itemdict.items():
                        if addOrderInfo:
                            s = StatementWithOrder(id, prop, item, objecttype, scope, pos)
                        else:
                            s = Statement(id, prop, item, objecttype, scope)
                        listStmts.append(s)
                    
                    m.addStatements(listStmts)
                    
                    if addOrderInfo and not m.canHandleStatementWithOrder:    
                        lists = {}
                        for s in listStmts:                            
                            value = (s[2], s[3])            
                            ordered  = lists.setdefault( (s[4], s[0], s[1]), [])                
                            for p in s.listpos:
                                ordered.append( (p, value) )
                        if lists:
                            self.generateListResources(m, lists)
                                
                else: #simple type
                    m.addStatement( Statement(id, prop, val, objecttype, scope) )
        return m.getStatements()

    def _blank(self, prefix=''):
        #prefixes include:
        #j:proplist:sub:pred
        #j:t:list:n
        #j:e:list:owner:n
        #j:t:object:n
        #j:e:object:owner:n
        if self._genBNode=='uuid':
            return RxPath.generateBnode(prefix=prefix)
        if self._genBNode=='counter':
            self.bnodecounter+=1
            return self.bnodeprefix + prefix + str(self.bnodecounter)
        else:
            return self._genBNode(prefix)
    
    def isEmbeddedBnode(self, id):
        prefixlen = len(self.bnodeprefix + 'j:')
        if id.startswith(self.bnodeprefix + 'j:e') or id.startswith(self.bnodeprefix + 'j:proplist:'):
            return True
        return False
        
    def lookslikeUriOrQname(self, s):
        #XXX think about customization, e.g. if number were ids
        if not isinstance(s, (str,unicode)):        
            return False
        m = self.RESOURCE_REGEX.match(s)
        if m is not None:
            return m.group(1)
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

    def generateListResources(self, m, lists):
        '''
        Generate property list resources
        `lists` is a dictionary: (scope, subject, prop) => [(pos, (object, objectvalue))+]
        '''
        #print 'generateListResources', lists
        for (scope, subject, prop), ordered in lists.items(): 
            #use special bnode pattern so we can find these quickly
            seq = self.bnodeprefix + 'j:proplist:' + subject+';'+prop
            m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(seq, RDF_MS_BASE+'type', 
                PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(seq, PROPBAG, prop, 
                OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(seq, PROPSUBJECT, subject, 
                OBJECT_TYPE_RESOURCE, scope) )
            m.addStatement( Statement(subject, PROPSEQ, seq, OBJECT_TYPE_RESOURCE, scope) )

            ordered.sort()
            for pos, (item, objecttype) in ordered:
                m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(pos+1), item, objecttype, scope) )

def tojson(statements, options=None):
    options = options or {}
    results = sjson(**options)._to_sjson(statements)
    return results['results']

def tostatements(contents, options=None):
    options = options or {}
    return sjson(**options).to_rdf(contents)
