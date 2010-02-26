#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
pjson (`persistent json`)
~~~~~~~~~~~~~~~~~~~~~~~~~

pjson is a set of property names and value patterns designed to make it easy 
to persist JSON. Its basic elements can be summarized as:

  `id` property 
     Indicates the id (or key) of the JSON object
   A JSON object like `{"$ref" : "ref"}` or a value that matches `@ref` pattern.
      Parses as an object reference
   A JSON object like `{"datatype": "datatype_name", value : "value" }` 
      Parses the value is a non-JSON datatype
  
`pjson` also defines a header object that can be used to specify alternative 
names or patterns for those predefined -- the aim is allow the use of 
existing JSON without having to modify it other than supply a pjson header.

The header object must contain a property is name is *"pjson"* and value is *"0.9"*.
It may also contain any of the reserved `pjson` property names 
(i.e. `id`, `$ref`, `namemap`, `id`, `datatype` and `context`) 
If present the value of the property is used as the reserved name.
For example, ``{ "psjon": "0.9", "id" : "itemid" }`` will instruct the parser 
to treat "itemid" as the `id`; properties named "id" will be treated as standard properties.

The header object can also contain a property named `refs`, which can be either a string or a JSON object. 
If it is a string, it must either be empty or match the following pattern:
  
*literal?*'('*regex*')'*literal?*

Where *regex* is a string that will be treated as a regular expression and 
*literal* are optional strings [1]_. When parsing JSON will treat any property
value that matches the *regex* as an object reference.
The *literal*s at the begin or end of the pattern also have to match if specified
but they are ignored as part of the object reference. Note that the parentheses
around the *regex* are required to delimitate the regex (even if no *literal* 
is specified) but ignored when pattern matching the value.
The regex follows the Javascript regular expressions (but without the leading 
and trailing "/") except two special values can be included in the regex:
*ABSURI* and *URIREF*. The former will expand into regular expression matching
an absolute URL, the latter expands to regular expression that matches 
relative URLs, which includes most strings that don't contain spaces or most
punctuation characters.  

As an example, the default `refs` pattern is *@(URIREF)*.

If `refs` is an empty string, pattern matching will be disabled.

If `refs` is an JSON object it must contain only one property. 
The property name will be treated as a ref pattern as described above,
and the property value will be used to generate the object reference.
The sequence "@@" will be replaced with the value of the regex match.
For example:

``{"<([-_a-zA-Z0-9])>" : "http://example.com/@@"}``

will treat values with like "<id1>" as an object reference with the value
"http://example.com/id1".

When serializing to JSON, any object reference that doesn't match the `refs` 
pattern will be serialized as an explicit ref object.
Likewise, any value that is not an object reference but *does* match the `refs` 
pattern will be serialized as an explicit data value.

Advance properties:

 `namemap`  
    The value of the `namemap` property must be a `pjson` header object as 
    described above. That header will be applied to all properties and descendent objects 
    contained within the JSON object that contains the `namemap` property.
    
 `context`
    The presence of a `context` property will assign that context to all 
    properties and descendent objects contained within the JSON object.
    The `context` property can also appear inside a `datatype` or `$ref` object.
    In that case, the context will be applied to only that value.
    
Additional semantics:

 * A JSON object without an `id` will be associated with the closest ancestor 
   (containing) JSON object that has an id. 
   If the object is at the top level it will be assigned an anonymous id.
 * `datatype` property can be either 'json', 'lang:' + *language code*, or a URI reference.
    If "json", the value is treated as is. If it begins with "lang:", it labels the value
    (which should be a string) with the given language code. 
    Any other value will be treated as non-JSON datatype whose interpretation 
    is dependent on the data store.

.. [1] Design note: This pattern was chosen because it always reversible 
 -- so that the same `namemap` can be used when serializing `pjson` to generate 
 references from the object ids.


Parse and serialize psjon to and from Vesper tuplesets.
'''
import re

from vesper.backports import *
from vesper.data import base
from vesper.data.store.basic import MemStore
from vesper.data.base import Statement, StatementWithOrder, OBJECT_TYPE_RESOURCE, RDF_MS_BASE, RDF_SCHEMA_BASE, OBJECT_TYPE_LITERAL
from vesper.data.base.utils import isBnode, OrderedModel, peekpair
from vesper import multipartjson

try:
    import yaml
    use_yaml = True
except ImportError:
    use_yaml = False

VERSION = '0.9'
JSON_BASE = 'pjson:schema#' #XXX
PROPSEQ  = JSON_BASE+'propseq'
PROPSEQTYPE = JSON_BASE+'propseqtype'
STANDALONESEQTYPE = JSON_BASE+'standalongseqtype'
PROPBAG = JSON_BASE+'propseqprop'
PROPSUBJECT = JSON_BASE+'propseqsubject'

XSD = 'http://www.w3.org/2001/XMLSchema#'

_xsdmap = { XSD+'integer': int,
#XXX sparql maps to float to XSD:decimal unless an exponent is used
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

from vesper.utils import Uri
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

def toJsonValue(data, objectType, preserveRdfTypeInfo=False, scope=None, 
                    datatypePropName = 'datatype', contextPropName='context'):
    assert objectType != OBJECT_TYPE_RESOURCE
    if len(objectType) > 1:
        valueparse = _xsdmap.get(objectType)
        if not preserveRdfTypeInfo and not valueparse:
            valueparse = _xsdExtendedMap.get(objectType)
        
        if scope is None and valueparse:
            return valueparse(data)
        elif valueparse:            
            literalObj = {datatypePropName:'json', 'value':valueparse(data)}
        else:
            if ':' not in objectType:
                #must be a language tag
                dataType = 'lang:' + objectType 
            else: #otherwise its a datatype URI
                if objectType.startswith('pjson:'): 
                    #this was tacked on while parsing to make type look like 
                    #an URI, so strip it out now
                    dataType = objectType[len('pjson:'):]
                else:
                    dataType = objectType
            literalObj = {datatypePropName : dataType, 'value': data }
    
        if scope is not None:
            literalObj[contextPropName] = scope
            
        return literalObj
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
            
    #PROPERTYMAP = property(lambda self: self.QName(JSON_BASE+'propertymap'))
    
    def __init__(self, nameMap = None, preserveTypeInfo=False, 
            includeObjectMap=False, explicitRefObjects=False, asList=False):
        '''
        { "pjson" : "0.9", "namemap" : {"refs":"@(URIREF)"}, "data": [...] }
        or if `asList == True`: 
        [{"pjson" : "0.9"}, ...]
        
        :param preserveTypeInfo: If True, avoid loss of precision by serializing
        a `datatype` object instead of the JSON type if the data store maps 
        multiple data types to a JSON type (e.g. different types of numbers).
        :param explicitRefObjects: Always serialize object references as a `$ref` object
        '''
        #XXX add replaceRefWithObject option: always, ifShared, never (default: ifShared (current behahavior))
        #also, includeObjectMap == True default replaceRefWithObject to never
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
        self.parseContext = ParseContext(self.nameMap)
        idrefpattern = self.nameMap.get('refs')
        if idrefpattern:
            regexes = _parseIdRefPattern(idrefpattern, True)
        else:
            regexes = (None, None, None)
        self.outputRefPattern, self.inputRefPattern, self.refTemplate = regexes

    def serializeObjectValue(self, objectValue, objectType, scope):
        if objectType != OBJECT_TYPE_RESOURCE:
            return self._value(objectValue, objectType, scope), False
        elif objectValue == RDF_MS_BASE + 'nil' and scope is None:
            return [], False
        else: #otherwise it's a resource
            return self.serializeRef(objectValue, scope), True
        
    def _value(self, objectValue, objectType, context=None):
        datatypeName = self.parseContext.datatypeName
        contextName = self.parseContext.contextName
        v = toJsonValue(objectValue, objectType, self.preserveRdfTypeInfo, 
                                        context, datatypeName, contextName)

        if isinstance(v, (str, unicode)):                
            if context is not None or (self.outputRefPattern 
                            and self.outputRefPattern.match(v)):
                #explicitly encode literal so we don't think its a reference
                literalObj = { datatypeName : 'json', 'value' : v }
                if context is not None:
                    literalObj[ contextName ] = context
                return literalObj
        else:
            assert context is None or isinstance(v, dict), (
                    'context should have been handled by toJsonValue')

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

    def serializeRef(self, uri, context):
        if not self.explicitRefObjects:
            assert self.inputRefPattern
            #check if the ref looks like our inputRefPattern
            #if it doesn't, create a explicit ref object
            if isinstance(uri, (str,unicode)):
                match = self.inputRefPattern.match(uri)
            else:
                match = False
        
        if self.explicitRefObjects or not match or context is not None:
            assert isinstance(uri, (str, unicode))
            ref =  { self.parseContext.refName : uri }
            if context is not None:
                ref[self.parseContext.contextName] = context
            return ref
        else:
            assert self.refTemplate
            return match.expand(self.refTemplate)
    
    def _setPropSeq(self, orderedmodel, propseq):
        childlist = []
        propbag = None
        for stmt in orderedmodel.getProperties(propseq):            
            prop = stmt.predicate
            obj = stmt.object
            #print '!!propseq member', p.stmt
            if prop == PROPBAG:
                propbag = obj
            elif prop == base.RDF_SCHEMA_BASE+u'member':
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
            childlist[i], isRes = self.serializeObjectValue(stmt.object, 
                                                stmt.objectType, scope)
            if isRes:                
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

    def createObjectRef(self, id, obj, includeObject, model, scope):
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
                #XXX add unittests
                #XXX pass in idrefs and results
                #XXX add depth option
                results = self.to_pjson(model.getStatements(id), model, scope) 
                objs = results.get('data')
                if objs:
                    return objs[0]                    
        return id

    def to_pjson(self, stmts=None, model=None, scope=''):
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

        idName = self.parseContext.idName 
        contextName = self.parseContext.contextName 
        reservedNames = self.parseContext.reservedNames
        for res in nodes: 
            childNodes = root.getProperties(res)           
            if not childNodes:
                #no properties
                continue            
            currentobj = { idName : res }
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
                currentobj[ contextName ] = resScope
                                        
            for stmt, next in peekpair(childNodes):
                prop = stmt.predicate
                if prop == PROPSEQ:
                    continue
                
                key = self.QName(prop)
                if key in currentobj:
                    #XXX create namemap that remaps ID if conflict
                    if key in reservedNames:
                        raise RuntimeError('property with reserved name %s' % key)
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

                parent[ key ], isRes = self.serializeObjectValue(stmt.object, 
                                                stmt.objectType, pScope)
                if isRes:                
                    #print 'prop key', key, prop, type(parent)
                    if stmt.object != res and pScope is None:
                        #add ref if object isn't same as subject
                        #and we don't have to serialize the pScope
                        idrefs.setdefault(stmt.object, []).append( 
                                            (parent, resScope, key) )

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
            ref = self.createObjectRef(id, obj, includeObject, model, defaultScope)            
            if ref != id:
                if obj is None:
                    #createObjectRef loaded a new obj from the model
                    #so add it to the result
                    results[id] = ref
                else:
                    #remove from roots since the obj is being included as a child
                    roots.pop(id, None) 
                for parent, parentScope, key in refs:
                    #replace the object reference with the object
                    if (not isinstance(ref, dict) or contextName not in ref
                                            ) and parentScope != defaultScope:
                        #if ref.context is not set and parent.context is set, 
                        #set ref.context = defaultScope so ref doesn't inherit
                        #parent.context
                        refcopy = ref.copy()
                        refcopy[ contextName ] = defaultScope
                        parent[key] = refcopy
                    else:                        
                        parent[key] = ref

        if self.asList:
            header = dict(pjson=VERSION)
            if self.nameMap:
                header['namemap'] = self.nameMap            
            return [header] + roots.values()
        
        retval = { 'data': roots.values() }
        if self.includeObjectMap:
            #XXX results' keys are datastore's ids, not the serialized refs
            #object map should have serialized refs so that this can be used
            #as a look up table
            retval['objects'] = results
        if self.nameMap:
            retval['namemap'] = self.nameMap
        retval['pjson'] = VERSION
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
            self.datatypeName = self.nameMap.get('datatype', parent.datatypeName)
            self.refName = self.nameMap.get('ref', parent.refName)
            self.refsValue = self.nameMap.get('refs', parent.refsValue)
        else:
            self.idName = self.nameMap.get('id', 'id')
            self.contextName = self.nameMap.get('context', 'context')
            self.namemapName = self.nameMap.get('namemap', 'namemap')
            self.datatypeName = self.nameMap.get('datatype', 'datatype')
            self.refName = self.nameMap.get('ref', '$ref')
            self.refsValue = self.nameMap.get('refs')
        
        self.reservedNames = [self.idName, self.contextName, self.refName, 
            self.datatypeName, 
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
        if name == '$ref':
            attrName = 'refName'
        else:
            attrName = name + 'Name'
        return getattr(self, attrName, name)
    
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
            self.bnodeprefix = base.BNODE_BASE
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
            if 'pjson' in json:
                start = json.get('data', [])
                if isinstance(start, dict):
                    start = [start]
                parseContext = ParseContext.initParseContext(json, parseContext)
            else:
                start = [json]
        else:
            start = list(json)
        
        todo = []
        for x in start:
            if 'pjson' in x:
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
            return base.generateBnode(prefix=prefix)
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
    def lookslikeIdRef(s, parseContext):
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
            context = parseContext.context
            res = self.lookslikeIdRef(item, parseContext)
        elif isinstance(item, dict):
            size = len(item)
            hasContext = int('context' in item)
            refName = parseContext.getName('$ref')
            context = item.get('context', parseContext.context)
            if refName in item:
                ref = item[refName]                
                #if isBnode(ref): #XXX ensure bnode is serialized consistently
                #   value = ref[bnode_len]
                #   return self.bnodeprefix+value, OBJECT_TYPE_RESOURCE, context
                return ref, OBJECT_TYPE_RESOURCE, context
            dataTypeName = parseContext.getName('datatype')
            if dataTypeName not in item or size < 2 or size > 2+hasContext:
                #not an explicit value object, just return it
                return item, None, parseContext.context
                        
            value = item['value']
            if isinstance(value, multipartjson.BlobRef):
                value = value.resolve()
            objectType = item.get(dataTypeName)
            if isinstance(objectType, multipartjson.BlobRef):
                objectType = objectType.resolve()

            if objectType == 'json':
                item = value
                res = None
            else:
                if objectType.startswith('lang:'):
                    objectType = objectType[len('lang:'):]
                elif ':' not in objectType:
                    #make the type look like an URL
                    objectType = 'pjson:' + objectType
                return value, objectType, context
        else:
            res = self.lookslikeIdRef(item, parseContext)                        
            context = parseContext.context
        
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
    results = Serializer(**options).to_pjson(statements)
    return results#['results']

def tostatements(contents, **options):
    return Parser(**options).to_rdf(contents)
