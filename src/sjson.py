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

== references ==
* two components of the language: namemaps and explicit values
* when reading, if idrefpattern is present, then it will be used to deduce whether or not value is a reference, even if unambiguousRefs is specified
** idrefpattern : '' removes the patten in the current scope, all values will be treated as literals
* when writing, may specify a match pattern and an output pattern, add to output and any reference that does not match the pattern will be serialized explicitly, as will any literal that does match the pattern

namemap : {
namemap : 'nsmap',
id : 'itemid',
context : 'scope',
ids : [ '/foo:(/w+)/http://www#$1' ],
props : ['/foo:(/w+)/http://foo$1' ],
refs : [/foo/],
'date' : '/$3/foo'
}

* reading from store: needs to write out a namemap so that it can be read back in
  * internal-refpattern: distingish ref from values: round-trip OK
  * output ref differently: could prevent internal-refpattern from matching refs
  * limit to augmenting: /foo/@$1 => @(foo)/$1
  * datatypes:  
  * property names, ids, references

user can specify a namemap:
ref : ref || { '@(ref)' : '$1'} 
ref : {'ref' : '(regex)'}

'datatype' : 'pattern'+'replace'

datatypemap, datatype map is only converting internal datatype to json datatypes. 
The other direction is needs to be specified in the datatype.
'''

from rx.python_shim import *
from rx import RxPath    
from rx.RxPath import Statement, StatementWithOrder, OBJECT_TYPE_RESOURCE, RDF_MS_BASE, RDF_SCHEMA_BASE, OBJECT_TYPE_LITERAL
from rx.RxPathUtils import encodeStmtObject
import re

try:
    import yaml
    use_yaml = True
except ImportError:
    use_yaml = False

VERSION = '0.9'
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

from rx import Uri
ABSURI, URIREF = Uri.getURIRegex(allowbnode=True)

_refpatternregex = re.compile(r'''(.*?)
    (?<!\\)\(
    (.+)
    (?<!\\)\)
    (.*)
''', re.X)

defaultRefPattern = '(URIREF)'

#XXX add _idrefpatternCache = {}

#find referenced match and swap them? would need to parse the regex ourself
def _parseIdRefPattern(pattern, serializing=False):
    '''
    pattern can be one of:

    literal?'('regex')'literal?
    or
    {'<(URIREF)>' : 'http://foo/@@'}
        
>>> _parseIdRefPattern({'<(\w+)>' : 'http://foo/@@'})
('\\<(\\w+)\\>', 'http://foo/\\1')
>>> _parseIdRefPattern({'<(\w+)>' : 'http://foo/@@'}, True)
('http\\:\\/\\/foo\\/(\\w+)', '<\\1>')
>>> _parseIdRefPattern(r'<(ABSURI)>') ==  ("\\<((?:"+ ABSURI + "))\\>", '\\1')
True
    '''
    if isinstance(pattern, dict):
        if len(pattern) != 1:
            raise RuntimeError('parse error: bad idrefpattern: %s' % pattern)
        pattern, replace = pattern.items()[0]
    else:
        replace = '@@' #r'\g<0>'

    #convert backreferences from $1 to \1 (javascript to perl style)
    #match = re.sub(r'(?<!\$)\$(?=\d{1,2}|\&)', r'\\', p[end+1:])
    #match = re.sub(r'\&', r'\\g<0>', match)
   
    m = re.match(_refpatternregex, pattern)
    if not m:
        raise RuntimeError('parse error: bad idrefpattern: %s' % pattern)
    before, regex, after = m.groups()
    regex = re.sub(r'(?<!\\)ABSURI', '(?:%s)' % ABSURI, regex)
    regex = re.sub(r'(?<!\\)URIREF', URIREF, regex) #URIREF already wrapped in (?:)

    pattern = re.escape(before) + '('+regex+')' + re.escape(after)
    pattern = re.compile(r'\A%s\Z' % pattern)
    
    if serializing:
        #turn `replace` into the inputpattern regex by replacing the @@ 
        #with the regex in the original pattern
        inputpattern = ''.join([x=='@@' and '('+regex+')' or re.escape(x) 
                    for x in re.split(r'((?<!\\)@@)',replace)])
        #replace is set to the orginal pattern with \1 replacing the regex part
        replace = before + r'\1' + after
        return pattern, re.compile(r'\A%s\Z' % inputpattern), replace
    else:        
        replace = re.sub(r'((?<!\\)@@)', r'\\1', replace)    
        return pattern, replace

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

def loads(data):
    '''
    Load a json-like string with either the json or yaml library, 
    depending which is installed.
    (Yaml's' "flow"-syntax provides a more forgiving super-set of json, 
    allowing single-quoted strings and trailing commas.)
    '''
    if use_yaml:
        return yaml.safe_load(data)
    else:
        return json.loads(data)

class Serializer(object):    
    #XXX need separate output nsmap for serializing
    #this nsmap shouldn't be the default for that
    nsmap=[(JSON_BASE,'')] 
        
    ID = property(lambda self: self.QName(JSON_BASE+'id'))
    PROPERTYMAP = property(lambda self: self.QName(JSON_BASE+'propertymap'))
    
    def __init__(self, nameMap = None, preserveTypeInfo=False, 
            includesharedrefs=False, explicitRefObjects=False):
        self.includesharedrefs = includesharedrefs
        self.preserveRdfTypeInfo = preserveTypeInfo
        self.explicitRefObjects = explicitRefObjects
        
        self.nameMap = nameMap and nameMap.copy() or {}
        if explicitRefObjects:
            if 'refs' in self.nameMap:
                del self.nameMap['refs']
        else:
            if 'refs' not in self.nameMap:            
                self.nameMap['refs'] = defaultRefPattern
                        
        idrefpattern = self.nameMap.get('refs')
        if idrefpattern:
            regexes = _parseIdRefPattern(idrefpattern, True)
        else:
            regexes = (None, None, None)
        self.outputRefPattern, self.inputRefPattern, self.refTemplate = regexes

    def _value(self, node):
        from rx import RxPathDom
        if isinstance(node.parentNode, RxPathDom.BasePredicate):
            stmt = node.parentNode.stmt
            objectType = stmt.objectType
            v = toJsonValue(node.data, objectType, self.preserveRdfTypeInfo)
        else:
            v = node.data
            objectType = OBJECT_TYPE_LITERAL

        if isinstance(v, (str, unicode)) and self.outputRefPattern:
            if self.outputRefPattern.match(v):
                #explicitly encode literal so we don't think its a reference
                return encodeStmtObject(v, objectType)
        return v
                
    
    def QName(self, prop):
        '''
        convert prop to QName
        '''
        #XXX currently just removes JSON_BASE 
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
        if not self.explicitRefObjects:
            assert self.inputRefPattern
            #check if the ref looks like our inputRefPattern
            #if it doesn't, create a explicit ref object
            if isinstance(uri, (str,unicode)):
                match = self.inputRefPattern.match(uri)
            else:
                match = False
        
        if self.explicitRefObjects or not match:            
            return encodeStmtObject(uri, OBJECT_TYPE_RESOURCE)
        else:
            assert self.refTemplate
            return match.expand(self.refTemplate)
            #return self.QName(uri) 
    
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

    def createObjectRef(self, id, obj, isshared, model):
        '''
        obj: The object referenced by the id, if None, the object was not encountered
        '''
        if obj is not None and not isshared:
            return obj
        else:
            if not isshared and model:
                #look up and serialize the resource
                #XXX pass on options like includesharedrefs
                #XXX pass in idrefs
                #XXX add depth option
                results = self.to_sjson(model.getStatements(id)) 
                objs = results.get('data')
                if objs:
                    return objs[0]                    
            return id

    def to_sjson(self, root=None, model=None):
        #1. build a list of subjectnodes
        #2. map them to object or lists, building id => [ objrefs ] dict
        #3. iterate through id map, if number of refs == 1 set in object, otherwise add to shared

        #XXX add exclude_blankids option?
        
        #use RxPathDom, expensive but arranges as sorted tree, normalizes RDF collections et al.
        #and is schema aware
        from rx import RxPathDom
        if not isinstance(root, RxPathDom.Node):
            #assume doc is iterator of statements or quad tuples
            #note: order is not preserved
            if root is not None:
                rootModel = RxPath.MemModel(root)
            else:
                rootModel = model
            root = RxPath.createDOM(rootModel, schemaClass=RxPath.BaseSchema)

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
        includesharedrefs = self.includesharedrefs
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
                    assert key != self.ID, (key, self.ID)
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
            ref = self.createObjectRef(id, obj, isshared, model)
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

        retval = { 'data': roots.values() }
        #if not includesharedrefs:
        #    retval['objects'] = results            
        if self.nameMap:
            retval['namemap'] = self.nameMap
        retval['sjson'] = VERSION
        return retval

class ParseContext(object):
    parentid = ''
    
    @staticmethod
    def initParseContext(nameMap, parent):
        if nameMap is not None or parent.nameMapChanges():
            return ParseContext(nameMap, parent)
        else:
            return parent

    def __init__(self, nameMap, parent=None):
        if nameMap is None:
            assert parent
            self.nameMap = parent.nameMap
        else:
            self.nameMap = nameMap
        self.parent = parent
        if parent:
            self.idName = self.nameMap.get('id', parent.idName)
            self.contextName = self.nameMap.get('context', parent.contextName)
            self.namemapName = self.nameMap.get('namemap', parent.namemapName)
            self.refsValue = self.nameMap.get('refs', parent.refsValue)
        else:
            self.idName = self.nameMap.get('id', 'id')
            self.contextName = self.nameMap.get('context', 'context')
            self.namemapName = self.nameMap.get('namemap', 'namemap')            
            self.refsValue = self.nameMap.get('refs')
        
        self.reservedNames = [self.idName, self.contextName, 
            #it's the parent context's namemap propery that is in effect:
            parent and parent.namemapName or 'namemap']
                            
        self.idrefpattern, self.refTemplate = None, None
        self._setIdRefPattern(self.refsValue)
    
    def nameMapChanges(self):
        if self.parent and self.nameMap != self.parent.nameMap:
            return True
        return False
        
    def getProp(self, obj, name):
        nameprop = getattr(self, name+'Name')        
        return obj.get(nameprop)

    def getName(self, name):
        return getattr(self, name+'Name')
    
    def isReservedPropertyName(self, prop):
        return prop in self.reservedNames
            
    def _expandqname(self, qname):
        #assume reverse sort of prefix
        return qname
        #XXX    
        for ns, prefix in self.nsmap:
            if qname.startswith(prefix+'$'):
                suffix = qname[len(prefix)+1:]
                return ns+suffix
        return qname
        
    def _setIdRefPattern(self, idrefpattern):
        if idrefpattern:
            self.idrefpattern, self.refTemplate = _parseIdRefPattern(idrefpattern)
        elif idrefpattern is not None: #property present but empty value
            self.idrefpattern, self.refTemplate = None, None

class Parser(object):
    
    bnodecounter = 0
    bnodeprefix = '_:'
    
    def __init__(self,addOrderInfo=True, 
            generateBnode=_defaultBNodeGenerator, 
            scope = '', 
            setBNodeOnObj=False,
            nameMap=None,
            useDefaultRefPattern=True):

        self._genBNode = generateBnode
        if generateBnode == 'uuid': #XXX hackish
            self.bnodeprefix = RxPath.BNODE_BASE
        self.scope = scope
        self.addOrderInfo = addOrderInfo
        self.setBNodeOnObj = setBNodeOnObj
    
        nameMap = nameMap or {}
        if useDefaultRefPattern and 'refs' not in nameMap:
            nameMap['refs'] = defaultRefPattern
        self.defaultParseContext = ParseContext(nameMap)
    
    def to_rdf(self, json, scope = None):        
        m = RxPath.MemModel() #XXX        

        if scope is None:
            scope = self.scope

        parentid = ''
        parseContext = self.defaultParseContext

        def getorsetid(obj):
            namemap = parseContext.getProp(obj, 'namemap')
            newParseContext = ParseContext.initParseContext(namemap, parseContext)
            objId = newParseContext.getProp(obj, 'id')
            if objId is None:  
                #mark bnodes for nested objects differently                
                prefix = parentid and 'j:e:' or 'j:t:'
                suffix = parentid and (str(parentid) + ':') or ''
                objId = self._blank(prefix+'object:'+suffix)
                if self.setBNodeOnObj:
                    obj[ newParseContext.getName('id') ] = objId
            return objId, newParseContext

        if isinstance(json, (str,unicode)):
            todo = json = loads(json) 
        
        if isinstance(json, dict):
            if 'sjson' in json:
                todo = json.get('data', [])
                namemap = parseContext.getProp(json, 'namemap')
                parseContext = ParseContext.initParseContext(namemap, parseContext)
            else:
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
                item, objecttype = self.deduceObjectType(item, parseContext)
                if isinstance(item, dict):
                    itemid, itemParseContext = getorsetid(item)
                    m.addStatement( Statement(seq,
                        RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, scope) )
                    todo.append( (item, (itemid, itemParseContext), parentid))
                elif isinstance(item, list):
                    nestedlistid = _createNestedList(item)
                    m.addStatement( Statement(seq,
                            RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, scope) )
                else: #simple type
                    m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, scope) )
            return seq
        
        while todo:
            obj, (id, parseContext), parentid = todo.pop(0)
                        
            #XXX support top level lists: 'list:' 
            assert isinstance(obj, dict), "only top-level dicts support right now"            
            #XXX propmap
            #XXX idmap
            if not self.isEmbeddedBnode(id): 
                #this object isn't embedded so set it as the new parent
                parentid = id
                        
            for prop, val in obj.items():
                if parseContext.isReservedPropertyName(prop):
                    continue
                prop = parseContext._expandqname(prop)
                val, objecttype = self.deduceObjectType(val, parseContext)
                if isinstance(val, dict):
                    objid, valParseContext = getorsetid(val) 
                    m.addStatement( Statement(id, prop, objid, OBJECT_TYPE_RESOURCE, scope) )    
                    todo.append( (val, (objid, valParseContext), parentid) )
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
                        item, objecttype = self.deduceObjectType(item, parseContext)
                        if isinstance(item, dict):
                            itemid, itemParseContext = getorsetid(item)
                            pos = itemdict.get((itemid, OBJECT_TYPE_RESOURCE))                            
                            if pos:
                                pos.append(i)
                            else:
                                itemdict[(itemid, OBJECT_TYPE_RESOURCE)] = [i]                                                                
                                todo.append( (item, (itemid, itemParseContext), parentid) )
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
        if not isinstance(id, (str,unicode)):
            return False
        prefixlen = len(self.bnodeprefix + 'j:')
        if id.startswith(self.bnodeprefix + 'j:e') or id.startswith(self.bnodeprefix + 'j:proplist:'):
            return True
        return False

    @staticmethod
    def lookslikeUriOrQname(s, parseContext):
        #XXX think about customization, e.g. if number were ids
        if not isinstance(s, (str,unicode)):
            return False
        if not parseContext.idrefpattern:
            return False
        
        m = parseContext.idrefpattern.match(s)
        if m is not None:            
            res = m.expand(parseContext.refTemplate)
            return res
        return False

    def deduceObjectType(self, item, parseContext):    
        if isinstance(item, list):
            return item, None
        if isinstance(item, dict):
            size = len(item) 
            if 'value' not in item or size<2 or size>3:
                return item, None
            value = item['value']
            objectType = item.get('datatype')
            if not objectType:
                objectType = item.get('xml:lang')
            type = item.get('type')
            if type == 'uri':                
                objectType = OBJECT_TYPE_RESOURCE
            elif type == 'bnode':
                return self.bnodeprefix+value, OBJECT_TYPE_RESOURCE
            if not objectType:
                if type == 'literal':
                    return value, OBJECT_TYPE_LITERAL
                else:
                    return item, None
            else:
                return value, objectType 

        res = self.lookslikeUriOrQname(item, parseContext)
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
    results = Serializer(**options).to_sjson(statements)
    return results#['results']

def tostatements(contents, options=None):
    options = options or {}
    return Parser(**options).to_rdf(contents)
