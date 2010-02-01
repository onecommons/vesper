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
import re

from vesper.backports import *
from vesper.data import RxPath
from vesper.data.store.basic import MemStore
from vesper.data.RxPath import Statement, StatementWithOrder, OBJECT_TYPE_RESOURCE, RDF_MS_BASE, RDF_SCHEMA_BASE, OBJECT_TYPE_LITERAL
from vesper.data.RxPathUtils import encodeStmtObject, OrderedModel, peekpair
from vesper import multipartjson

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

from vesper.data import Uri
ABSURI, URIREF = Uri.getURIRegex(allowbnode=True)

_refpatternregex = re.compile(r'''(.*?)
    (?<!\\)\(
    (.+)
    (?<!\\)\)
    (.*)
''', re.X)

#by default we only find refs that match this pattern
#we picked an unusual pattern because to require ref pattern
#to be specified then to have false positives
defaultSerializeRefPattern = '@(URIREF)'
defaultParseRefPattern = '@(URIREF)'

#XXX add _idrefpatternCache = {}

#XXX add prop refs support: proprefs" : [
# "foo|bar" , {"(\w{1,5})" : "http://foo/@@"},
# '!prop1|prop2' , '<(URIREF)>',
#  '.*' , { 'foo:(\w+)' : "http://foo/@@", 'bar:(\w+)' : "http://bar/@@"}
#]
#XXX support multiple patterns as dict or array

#find referenced match and swap them? would need to parse the regex ourself
def _parseIdRefPattern(pattern, serializing=False):
    r'''
    pattern can be one of:

    literal?'('regex')'literal?
    or
    { pattern : replacement}
    e.g.
    {'<(URIREF)>' : 'http://foo/@@'}
        
>>> p, r = _parseIdRefPattern({'<(\w+)>' : 'http://foo/@@'})
>>> p.pattern, r
('\\A\\<(\\w+)\\>\\Z', 'http://foo/\\1')
>>> output, input, r = _parseIdRefPattern({'<(\w+)>' : 'http://foo/@@'}, True)
>>> output.pattern, input.pattern, r
('\\A\\<(\\w+)\\>\\Z', '\\Ahttp\\:\\/\\/foo\\/(\\w+)\\Z', '<\\1>')
>>> _parseIdRefPattern(r'<(ABSURI)>')[0].pattern ==  "\\A\\<((?:"+ ABSURI + "))\\>\\Z"
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

def toJsonValue(data, objectType, preserveRdfTypeInfo=False, scope=None, valueName='value'):
    if len(objectType) > 1:
        encode = scope is not None
        if not encode:
            valueparse = _xsdmap.get(objectType)
            if valueparse:
                return valueparse(data)
            if not preserveRdfTypeInfo:
                valueparse = _xsdExtendedMap.get(objectType)
                if valueparse:
                    return valueparse(data)
        return encodeStmtObject(data, objectType, scope=scope, valueName=valueName)
    else:
        return data

def findPropList(model, subject, predicate, objectValue=None, objectType=None, scope=None):
    #by default search for special proplist bnode pattern
    #other models/mappings may need to implement a different way to figure this out
    listid = model.bnodePrefix+'j:proplist:'+subject+';'+predicate
    rows = model.filter({
        0 : listid,
        2 : objectValue,
        3 : objectType,
        4 : scope
    })
    #print 'findprop', listid, list(rows)
    return rows

def loads(data):
    '''
    Load a json-like string with either the json or yaml library, 
    depending which is installed.
    (Yaml's' "flow"-syntax provides a more forgiving super-set of json, 
    allowing single-quoted strings and trailing commas.)
    '''
    if multipartjson.looks_like_multipartjson(data):
        return multipartjson.loads(data, False)
    elif use_yaml:
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
            includeObjectMap=False, explicitRefObjects=False, asList=False):
        '''
        '''
        #XXX add replaceRefWithObject option: always, ifShared, never (default: ifShared (current behahavior))
        self.includeObjectMap = includeObjectMap
        self.preserveRdfTypeInfo = preserveTypeInfo
        self.explicitRefObjects = explicitRefObjects
        self.asList = asList
        
        self.nameMap = nameMap and nameMap.copy() or {}
        if explicitRefObjects:
            if 'refs' in self.nameMap:
                del self.nameMap['refs']
        else:
            if 'refs' not in self.nameMap:            
                self.nameMap['refs'] = defaultSerializeRefPattern
                        
        idrefpattern = self.nameMap.get('refs')
        if idrefpattern:
            regexes = _parseIdRefPattern(idrefpattern, True)
        else:
            regexes = (None, None, None)
        self.outputRefPattern, self.inputRefPattern, self.refTemplate = regexes

    def _value(self, stmt, context=None):
        #XXX use valueName
        objectType = stmt.objectType            
        v = toJsonValue(stmt.object, objectType, self.preserveRdfTypeInfo, context)

        if isinstance(v, (str, unicode)):                
            if context is not None or (self.outputRefPattern 
                            and self.outputRefPattern.match(v)):
                #explicitly encode literal so we don't think its a reference
                return encodeStmtObject(v, objectType, scope=context)
        else:
            assert context is None or isinstance(v, dict), 'context should have been handled by toJsonValue'

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

    def serializeRef(self, prop, uri, context):
        if not self.explicitRefObjects:
            assert self.inputRefPattern
            #check if the ref looks like our inputRefPattern
            #if it doesn't, create a explicit ref object
            if isinstance(uri, (str,unicode)):
                match = self.inputRefPattern.match(uri)
            else:
                match = False
        
        if self.explicitRefObjects or not match or context is not None:
            #XXX use valueName
            assert isinstance(uri, (str, unicode))
            return encodeStmtObject(uri, OBJECT_TYPE_RESOURCE, scope=context)
        else:
            assert self.refTemplate
            return match.expand(self.refTemplate)
            #return self.QName(uri) 
    
    def _setPropSeq(self, orderedmodel, propseq):
        childlist = []
        propbag = None
        for stmt in orderedmodel.getProperties(propseq):            
            prop = stmt.predicate
            obj = stmt.object
            #print '!!propseq member', p.stmt
            if prop == PROPBAG:
                propbag = obj
            elif prop == RxPath.RDF_SCHEMA_BASE+u'member':
                childlist.append( stmt )
        return propbag, childlist

    def _finishPropList(self, prop, childlist, idrefs, resScope, nestedLists):
        for i, stmt in enumerate(childlist):
            if not isinstance(stmt, Statement):
                #this list was already processed
                assert all(not isinstance(stmt, Statement) for stmt in childlist)
                return
            scope = stmt.scope
            if scope == resScope:
                scope = None #only include scope if its different
            if stmt.objectType != OBJECT_TYPE_RESOURCE:
                childlist[i]  = self._value(stmt, scope)
            elif stmt.object == RDF_MS_BASE + 'nil' and scope is None:
                childlist[i] = []
            else: #otherwise it's a resource
                childlist[i] = self.serializeRef(prop, stmt.object, scope)
                if scope is None:
                    #only add ref if we don't have to serialize the pScope
                    idrefs.setdefault(stmt.object, []).append(
                                    (childlist, resScope, i))
                if stmt.object in nestedLists:
                    #if nested list handle it now
                    (seqprop, nestedlist) = nestedLists[stmt.object]
                    assert not seqprop
                    self._finishPropList(prop, nestedlist, idrefs, resScope, nestedLists)
        assert all(not isinstance(node, Statement) for node in childlist)

    def createObjectRef(self, id, obj, includeObject, model):
        '''
        obj: The object referenced by the id, if None, the object was not encountered
        '''
        if includeObject:
            if obj is not None:
                assert isinstance(obj, (str,unicode,dict,list,int,long,bool)
                                             ), '%r is not a json type' % obj
                return obj
            if model:
                #look up and serialize the resource
                #XXX pass on options like includesharedrefs
                #XXX pass in idrefs
                #XXX add depth option
                results = self.to_sjson(model.getStatements(id)) 
                objs = results.get('data')
                if objs:
                    return objs[0]                    
        return id

    def to_sjson(self, stmts=None, model=None, scope=''):
        #1. build a list of subjectnodes
        #2. map them to object or lists, building id => [ objrefs ] dict
        #3. iterate through id map, if number of refs == 1 set in object, otherwise add to shared

        #XXX add exclude_blankids option?
        defaultScope = scope
        if stmts is None:
            stmts = model.getStatments()
        #step 1: build a list of subjectnodes
        listresources = set()
        nodes = []
        root = OrderedModel(stmts)
        for resourceUri in root.resources:
            islist = False
            for resourceStmt in root.subjectDict[resourceUri]:
                if (resourceStmt.predicate == RDF_MS_BASE+'type' and 
                    resourceStmt.object in (PROPSEQTYPE, STANDALONESEQTYPE)):
                    #resource has rdf:type propseqtype
                    listresources.add(resourceUri)
                    islist = True
                    break
            if not islist:
                nodes.append(resourceUri)

        #step 2: map them to object or lists, building id => [ objrefs ] dict
        #along the way        
        results = {}
        lists = {}
        idrefs = {}

        for listnode in listresources:            
            seqprop, childlist = self._setPropSeq(root, listnode)
            lists[listnode] = (seqprop, childlist)

        for res in nodes: 
            childNodes = root.getProperties(res)           
            if not childNodes:
                #no properties
                continue            
            currentobj = { self.ID : res }
            currentlist = []
            #print 'adding to results', res.uri
            results[res] = currentobj
            #print 'res', res, res.childNodes
            
            #deal with sequences first
            resScopes = {}
            for stmt in childNodes:
                if stmt.predicate == PROPSEQ:
                    #this will replace sequences
                    seqprop, childlist = lists[stmt.object]
                    key = self.QName(seqprop)
                    currentobj[ key ] = childlist
                    #print 'adding propseq', p.stmt.object
                else:
                    pscope = stmt.scope
                    resScopes[pscope] = resScopes.setdefault(pscope, 0) + 1
            
            mostcommon = 0
            resScope = defaultScope
            for k,v in resScopes.items():
                if v > mostcommon:
                    mostcommon = v
                    resScope = k
            
            if resScope != defaultScope:
                #add context prop if different from default context
                currentobj['context'] = resScope
                                        
            for stmt, next in peekpair(childNodes):
                prop = stmt.predicate
                if prop == PROPSEQ:
                    continue
                
                key = self.QName(prop)
                if key in currentobj:
                    #XXX create namemap that remaps ID if conflict
                    assert key not in ('context', self.ID), (
                                    'property with reserved name %s' % key)
                    #must have be already handled by _setPropSeq
                    self._finishPropList(key, currentobj[key], idrefs, resScope, lists)
                    continue 

                nextMatches = next and next.predicate == prop
                #XXX Test empty and singleton rdf lists and containers
                if nextMatches or currentlist:
                    parent = currentlist
                    key = len(currentlist)
                    currentlist.append(0)
                else:
                    parent = currentobj
                
                if stmt.scope != resScope:
                    pScope = stmt.scope
                else:
                    pScope = None
                
                if stmt.objectType != OBJECT_TYPE_RESOURCE:
                    parent[ key ] = self._value(stmt, pScope)
                elif stmt.object == RDF_MS_BASE + 'nil' and pScope is None:
                    parent[ key ] = []
                else: #otherwise it's a resource
                    #print 'prop key', key, prop, type(parent)
                    parent[ key ] = self.serializeRef(key, stmt.object, pScope)
                    if stmt.object != res and pScope is None:
                        #add ref if object isn't same as subject
                        #and we don't have to serialize the pScope
                        idrefs.setdefault(stmt.object, []).append( (parent, resScope, key) )

                if currentlist and not nextMatches:
                    #done with this list
                    currentobj[ prop ] = currentlist
                    currentlist = []

        #3. iterate through id map, if number of refs == 1, replace the reference with the object
        roots = results.copy()
        for id, refs in idrefs.items():
            includeObject = len(refs) <= 1
            obj = None
            if id in results:
                obj = results[id]
            elif id in lists:
                obj = lists[id][1]
                #print 'nestedlist', id, lists[id] 
            #else:
            #    print id, 'not found in', results, 'or', lists
            ref = self.createObjectRef(id, obj, includeObject, model)
            #print 'includeObject', includeObject, id, ref
            if ref != id:
                if obj is None:
                    #createObjectRef created an obj from an unreferenced obj,
                    #so add it to the result
                    results[id] = ref
                else:
                    #remove since the obj is referenced
                    roots.pop(id, None) 
                for parent, parentScope, key in refs:                    
                    #if ref.context is not set and parent.context is set, set
                    #ref.context = defaultScope so ref doesn't inherit parent.context
                    if (not isinstance(ref, dict) or 'context' not in ref
                                            ) and parentScope != defaultScope:
                        refcopy = ref.copy()
                        refcopy['context'] = defaultScope
                        parent[key] = refcopy
                    else:
                        parent[key] = ref

        if self.asList:
            header = dict(sjson=VERSION)
            if self.nameMap:
                header['namemap'] = self.nameMap            
            return [header] + roots.values()
        
        retval = { 'data': roots.values() }
        if self.includeObjectMap:
            retval['objects'] = results
        if self.nameMap:
            retval['namemap'] = self.nameMap
        retval['sjson'] = VERSION
        return retval

class ParseContext(object):
    parentid = ''
    
    @staticmethod
    def initParseContext(obj, parent):
        if parent:
            nameMap = parent.getProp(obj, 'namemap')
        else:
            nameMap = obj.get('namemap')
            
        if nameMap is not None or parent.nameMapChanges():
            #new namemap comes into effect, need to create a parsecontext
            pc = ParseContext(nameMap, parent)
            if parent:
                default = parent.context
            else:
                default = None
            pc.context = pc.getProp(obj, 'context', default)
            return pc
        else:
            if parent:
                context = parent.getProp(obj, 'context')
            else: #no parent or namemap
                context = obj.get('context')
            if context is not None:
                #context specified, need to create a parsecontext
                pc = ParseContext(nameMap, parent)
                pc.context = context
                return pc

        #otherwise can just use parent context
        return parent

    def __init__(self, nameMap, parent=None):
        if nameMap is None:
            self.nameMap = parent and parent.nameMap or {}
        else:
            self.nameMap = nameMap
        self.parent = parent
        if parent:
            self.idName = self.nameMap.get('id', parent.idName)
            self.contextName = self.nameMap.get('context', parent.contextName)
            self.namemapName = self.nameMap.get('namemap', parent.namemapName)
            self.valueName = self.nameMap.get('value', parent.valueName)
            self.refsValue = self.nameMap.get('refs', parent.refsValue)
        else:
            self.idName = self.nameMap.get('id', 'id')
            self.contextName = self.nameMap.get('context', 'context')
            self.namemapName = self.nameMap.get('namemap', 'namemap')
            self.valueName = self.nameMap.get('value', 'value')            
            self.refsValue = self.nameMap.get('refs')
        
        self.reservedNames = [self.idName, self.contextName, 
            #it's the parent context's namemap propery that is in effect:
            parent and parent.namemapName or 'namemap']
                            
        self.idrefpattern, self.refTemplate = None, None
        self._setIdRefPattern(self.refsValue)
        self.currentProp = None
    
    def nameMapChanges(self):
        if self.parent and self.nameMap != self.parent.nameMap:
            return True
        return False
        
    def getProp(self, obj, name, default=None):
        nameprop = getattr(self, name+'Name')        
        value = obj.get(nameprop, default)
        if isinstance(value, multipartjson.BlobRef):
            value = value.resolve()
        return value

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
            generateBnode=None, 
            scope = '', 
            setBNodeOnObj=False,
            nameMap=None,
            useDefaultRefPattern=True):
        generateBnode = generateBnode or _defaultBNodeGenerator
        self._genBNode = generateBnode
        if generateBnode == 'uuid': #XXX hackish
            self.bnodeprefix = RxPath.BNODE_BASE
        self.addOrderInfo = addOrderInfo
        self.setBNodeOnObj = setBNodeOnObj
    
        nameMap = nameMap or {}
        if useDefaultRefPattern and 'refs' not in nameMap:
            nameMap['refs'] = defaultParseRefPattern
        self.defaultParseContext = ParseContext(nameMap)
        self.defaultParseContext.context = scope
    
    def to_rdf(self, json, scope = None):
        m = MemStore() #XXX

        parentid = ''
        parseContext = self.defaultParseContext
        if scope is None:
            scope = parseContext.context

        def getorsetid(obj):
            newParseContext = ParseContext.initParseContext(obj, parseContext)
            objId = newParseContext.getProp(obj, 'id')
            if objId is None:  
                #mark bnodes for nested objects differently                
                prefix = parentid and 'j:e:' or 'j:t:'
                suffix = parentid and (str(parentid) + ':') or ''
                objId = self._blank(prefix+'object:'+suffix)
                if self.setBNodeOnObj:
                    obj[ newParseContext.getName('id') ] = objId
            elif not isinstance(objId, (unicode, str)):
                objId = str(objId) #OBJECT_TYPE_RESOURCEs need to be strings
            return objId, newParseContext
            
        
        if isinstance(json, (str,unicode)):
            json = loads(json) 
        
        if isinstance(json, dict):
            if 'sjson' in json:
                start = json.get('data', [])
                parseContext = ParseContext.initParseContext(json, parseContext)
            else:
                start = [json]
        else:
            start = list(json)
        
        todo = []
        for x in start:
            if 'sjson' in x:
                #not an object, just reset parseContext (for next call to getorsetid())
                parseContext = ParseContext.initParseContext(x, parseContext)
            else:
                todo.append( (x, getorsetid(x), '') )
        #print 'parse json', todo
                       
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
                STANDALONESEQTYPE, OBJECT_TYPE_RESOURCE, scope) )

            for i, item in enumerate(val):
                item, objecttype, itemscope = self.deduceObjectType(item, parseContext)
                if isinstance(item, dict):
                    itemid, itemParseContext = getorsetid(item)
                    m.addStatement( Statement(seq,
                        RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, itemscope) )
                    todo.append( (item, (itemid, itemParseContext), parentid))
                elif isinstance(item, list):
                    nestedlistid = _createNestedList(item)
                    m.addStatement( Statement(seq,
                            RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, itemscope) )
                else: #simple type
                    m.addStatement( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, itemscope) )
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

            scope = parseContext.context
            #print scope
            
            for prop, val in obj.items():
                if parseContext.isReservedPropertyName(prop):
                    continue
                prop = parseContext._expandqname(prop)
                parseContext.currentProp = prop
                val, objecttype, scope = self.deduceObjectType(val, parseContext)
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
                    itemScopes = {}
                    for i, item in enumerate(val):
                        item, objecttype, itemscope = self.deduceObjectType(item, parseContext)
                        itemdict = itemScopes.setdefault(itemscope, {})
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
                    for itemscope, itemdict in itemScopes.items():
                        for (item, objecttype), pos in itemdict.items():
                            #only preserve position if more than one item per scope or only one scope
                            if addOrderInfo and (len(itemScopes)==1 or len(itemdict)>1):
                                s = StatementWithOrder(id, prop, item, objecttype, itemscope, pos)
                            else:
                                s = Statement(id, prop, item, objecttype, itemscope)
                            listStmts.append(s)
                    
                    m.addStatements(listStmts)
                    
                    if addOrderInfo and not m.canHandleStatementWithOrder:    
                        lists = {}
                        for s in listStmts:
                            if not isinstance(s, StatementWithOrder):
                                continue
                            value = (s[2], s[3])            
                            ordered  = lists.setdefault( (s[4], s[0], s[1]), [])
                            for p in s.listpos:
                                ordered.append( (p, value) )
                        if lists:
                            self.generateListResources(m, lists)
                                
                else: #simple type
                    m.addStatement( Statement(id, prop, val, objecttype, scope) )
            parseContext.currentProp = None
            
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
        #XXX think about case where if number were ids
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
            return item, None, parseContext.context
        if isinstance(item, multipartjson.BlobRef):
            item = item.resolve()
        elif isinstance(item, dict):
            size = len(item)
            valueName = parseContext.getName('value')            
            maxsize = 3 + int('context' in item)
            if valueName not in item or size<2 or size>maxsize:
                #not an explicit value object, just return it
                return item, None, parseContext.context
            value = item[valueName]
            if isinstance(value, multipartjson.BlobRef):
                value = value.resolve()
            context = item.get('context', parseContext.context)
            if isinstance(context, multipartjson.BlobRef):
                context = context.resolve()
            objectType = item.get('datatype')
            if not objectType:
                objectType = item.get('xml:lang')
            if isinstance(objectType, multipartjson.BlobRef):
                objectType = objectType.resolve()
            itemtype = item.get('type')
            if itemtype == 'uri':                
                objectType = OBJECT_TYPE_RESOURCE
            elif itemtype == 'bnode':
                return self.bnodeprefix+value, OBJECT_TYPE_RESOURCE, context
            if not objectType:
                if itemtype == 'literal':
                    return value, OBJECT_TYPE_LITERAL, context
                else:
                    #looks like it wasn't an explicit value
                    return item, None, parseContext.context
            else:
                return value, objectType, context

        context = parseContext.context
        res = self.lookslikeUriOrQname(item, parseContext)
        if res:
            return res, OBJECT_TYPE_RESOURCE, context
        elif item is None:
            return 'null', JSON_BASE+'null', context
        elif isinstance(item, bool):
            return (item and 'true' or 'false'), XSD+'boolean', context
        elif isinstance(item, (int, long)):
            return unicode(item), XSD+'integer', context
        elif isinstance(item, float):
            return unicode(item), XSD+'double', context
        elif isinstance(item, (unicode, str)):
            return item, OBJECT_TYPE_LITERAL, context
        else:            
            raise RuntimeError('parse error: unexpected object type: %s (%r)' % (type(item), item)) 

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

def tojson(statements, **options):
    results = Serializer(**options).to_sjson(statements)
    return results#['results']

def tostatements(contents, **options):
    return Parser(**options).to_rdf(contents)
