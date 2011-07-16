#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
pjson (`persistent json`)
~~~~~~~~~~~~~~~~~~~~~~~~~

Parse and serialize pjson to and from Vesper tuplesets.
'''
#XXX implement 
#idmap: id : { prop : value }
#and
#propmap: propname : { id : value }
#XXX: replace RuntimeError with custom Exception class
import re, uuid

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
ABSURI = Uri.getURIRegex(allowbnode=True)[0]
URIREF = r'(?:[\w$_-]+[^\s{}\\\\]*)'

_refpatternregex = re.compile(r'''(.*?)
    (?<!\\)\(
    (.*)
    (?<!\\)\)
    (.*)
''', re.X)

#by default we only find refs that match this pattern
#we picked an unusual pattern because to require ref pattern
#to be specified then to have false positives
defaultRefPattern = '@((::)?URIREF)'

#XXX add _patternCache = {}
#match pattern and replacement template when parsing
#match pattern and replacement template when serializing 
_ReplacePatterns = namedtuple('_ReplacePatterns', 
  'parsePattern parseTemplate serializePattern serializeTemplate weight')

def _parseIdRefPattern(pattern, key=None, disallowPrefix=False):
    r'''
    pattern can be one of:

    literal?'('regex')'literal?
    or
    { pattern : replacement}
    e.g.
    {'<(URIREF)>' : 'http://foo/@@'}
        
>>> patterns = _parseIdRefPattern({'<(\w+)>' : 'http://foo/@@'})
>>> patterns.parsePattern.pattern, patterns.parseTemplate
('\\A\\<(\\w+)\\>\\Z', 'http://foo/\\1')
>>> patterns.serializePattern.pattern, patterns.serializeTemplate
('\\Ahttp\\:\\/\\/foo\\/(\\w+)\\Z', '<\\1>')
>>> _parseIdRefPattern(r'<(ABSURI)>')[0].pattern ==  "\\A\\<((?:"+ ABSURI + "))\\>\\Z"
True
    '''
    if isinstance(pattern, dict):
        if key is not None:
            pattern, replace = key, pattern[key]
        else:
            if len(pattern) != 1:
                raise RuntimeError('parse error: bad pattern: %s' % pattern)
            pattern, replace = pattern.items()[0]
    else:
        replace = '@@' #r'\g<0>'
   
    m = re.match(_refpatternregex, pattern)
    if not m:
        #assume pattern is a prefix and the regex matches everything after that
        before, regex, after = pattern, '.*', ''        
    else:
        before, regex, after = m.groups()
    #assign weight so that patterns with literal prefixes are evaluated first, 
    #and patterns that match anything go last
    if before:
        weight = 0
    elif regex == '.*' and not after:
        weight = 2
    else:
        weight = 1
    
    #convert backreferences from $1 to \1 (javascript style to perl/python style)
    regex = re.sub(r'(?<!\$)\$(?=\d{1,2}|\&)', r'\\', regex)
    regex = re.sub(r'\&', r'\\g<0>', regex)
    #handle ABSURI and URIREF
    regex = re.sub(r'(?<!\\)ABSURI', '(?:%s)' % ABSURI, regex)
    #URIREF is already wrapped in (?:)
    regex = re.sub(r'(?<!\\)URIREF', URIREF, regex)
    pattern = re.escape(before) + '('+regex+')' + re.escape(after)
    if disallowPrefix:
        #disallow replacements that match '::...') by prepending (?!::)
        pattern = '(?!::)' + pattern
    pattern = re.compile(r'\A%s\Z' % pattern)

    if not re.search(r'((?<!\\)@@)', replace):
        replace += '@@' #append '@@' if it doesn't appear
    
    parseTemplate = re.sub(r'((?<!\\)@@)', r'\\1', replace)    

    #turn `replace` into the serializepattern regex by replacing the @@ 
    #with the regex in the original pattern
    serializePattern = ''.join([x=='@@' and '('+regex+')' or re.escape(x) 
                for x in re.split(r'((?<!\\)@@)',replace)])
    serializePattern = re.compile(r'\A%s\Z' % serializePattern)
    #serializeTemplate is set to the orginal pattern with \1 replacing the regex part
    serializeTemplate = before + r'\1' + after    
    return _ReplacePatterns(pattern, parseTemplate, serializePattern, 
                                            serializeTemplate, weight)
    
_defaultBNodeGenerator = 'uuid'

def toJsonValue(data, objectType, preserveRdfTypeInfo=False, scope=None, 
    datatypePropName = 'datatype', contextPropName='context'):
    assert objectType != OBJECT_TYPE_RESOURCE
    if objectType != OBJECT_TYPE_LITERAL:
        valueparse = _xsdmap.get(objectType)
        if not preserveRdfTypeInfo and not valueparse:
            valueparse = _xsdExtendedMap.get(objectType)
        
        if scope is None and valueparse:
            return valueparse(data)
        elif valueparse:            
            literalObj = {datatypePropName:'json', 'value':valueparse(data)}
        else:
            dataType = rdfDataTypeToPjsonDataType(objectType)
            literalObj = {datatypePropName : dataType, 'value': data }
    
        if scope is not None:
            literalObj[contextPropName] = scope
            
        return literalObj
    else:  #its literal, just return it
        return data

def rdfDataTypeToPjsonDataType(objectType):
    if objectType == OBJECT_TYPE_LITERAL:
        return 'json'    
    elif ':' not in objectType:
        #must be a language tag
        return 'lang:' + objectType 
    else: #otherwise its a datatype URI
        if objectType.startswith('pjson:'): 
            #this was tacked on while parsing to make type look like 
            #an URI, so strip it out now
            return objectType[len('pjson:'):]
        else:
            return objectType

def pjsonDataTypeToRdfDataType(objectType):
    if objectType.startswith('lang:'):
        objectType = objectType[len('lang:'):]
    elif ':' not in objectType:
        #make the type look like an URL
        objectType = 'pjson:' + objectType
    return objectType

def getDataType(item, parseContext):
    if item is None:
        return 'null', JSON_BASE+'null'
    elif isinstance(item, bool):
        return (item and u'true' or u'false'), XSD+'boolean'
    elif isinstance(item, (int, long)):
        return unicode(item), XSD+'integer'
    elif isinstance(item, float):
        return unicode(item), XSD+'double'
    elif isinstance(item, (unicode, str)):
        if parseContext:
            return parseContext.deduceDataType(item)
        else:
            return item, OBJECT_TYPE_LITERAL
    elif isinstance(item, base.ResourceUri):
        return item.uri, OBJECT_TYPE_RESOURCE
    else:
        raise RuntimeError('parse error: unexpected object type: %s (%r)' % (type(item), item))

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

def _isEmbeddedBnode(uri):
    return re.match(r'(bnode|_)\:j\:e\:.+', uri)

def loads(data):
    '''
    :param data: A string. 
       
    If `data` looks like multipartjson it is loaded with that,
    otherwise the tries to load it using the yaml library, 
    if it is installed, otherwise the standard json library is used.
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
    def __init__(self, nameMap = None, preserveTypeInfo=False, 
            includeObjectMap=False, explicitRefObjects=False, asList=False,
            onlyEmbedBnodes=False, parseContext=None, saveOrder=None,
            serializeIdAsRefs=True, omitEmbeddedIds=False):
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
        self.preserveTypeInfo = preserveTypeInfo
        self.explicitRefObjects = explicitRefObjects
        self.asList = asList
        self.onlyEmbedBnodes = onlyEmbedBnodes #objects without embedded ids are always top level        
        self.serializeIdAsRefs = serializeIdAsRefs
        self.omitEmbeddedIds = omitEmbeddedIds        
        nameMap = nameMap and nameMap.copy() or {}
        if explicitRefObjects:
            if 'refpattern' in nameMap:
                del nameMap['refpattern']
        else:
            if not parseContext and 'refpattern' not in nameMap:
                nameMap['refpattern'] = defaultRefPattern
        self.parseContext = ParseContext(nameMap, parseContext)
        self.config = self.__dict__.copy()
        self.nameMap = nameMap
        self.saveOrder = saveOrder        
        #self.configHash = (nameMap.items() self.config
        self.outputRefPattern, self.inputRefPattern, self.refTemplate = None, None, None
        idrefpattern = self.parseContext.refpatternValue
        if idrefpattern:
            patterns = _parseIdRefPattern(idrefpattern)
            self.outputRefPattern = patterns.parsePattern
            self.inputRefPattern = patterns.serializePattern
            self.refTemplate = patterns.serializeTemplate

    def __eq__(self, other):
        if not isinstance(other, Serializer):
            return False
        return self.config == other.config
    
    def __hash__(self):
        return hash(tuple(self.config.iteritems()))
    
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
        datatypeReplacements = self.parseContext.datatypeReplacements
        v = None
        falsePositive = False
        if context is None and datatypeReplacements:
            for dataType, replacements in datatypeReplacements.items():
                for r in replacements:
                    match = r.serializePattern.match(objectValue)
                    if match:
                        if objectType == dataType:
                            v = match.expand(r.serializeTemplate)
                        else:
                            falsePositive = True
        if v is None:
            v = toJsonValue(objectValue, objectType, self.preserveTypeInfo,
                                        context, datatypeName, contextName)

        if isinstance(v, (str, unicode)):
            if context is not None or falsePositive or (self.outputRefPattern 
                                          and self.outputRefPattern.match(v)):
                #explicitly encode literal so we don't think its a reference
                #or the wrong datatype                
                datatype = rdfDataTypeToPjsonDataType(objectType)
                literalObj = { datatypeName : datatype, 'value' : v }
                if context is not None:
                    literalObj[ contextName ] = context
                return literalObj
        else:
            assert context is None or isinstance(v, dict), (
                    'context should have been handled by toJsonValue')

        return v
                
    def serializeProp(self, prop):
        '''        
        If the property matches a replacement, apply the replacement.
        If not, but if the property looks like a replacement, use the default
        "::" replacement.
        Otherwise, return the property name. 
        '''
        prop = _serializeAbbreviations(prop, self.parseContext.propReplacements)
        if self.parseContext.isReservedPropertyName(prop):
            prop = "::" + prop
        return prop        

    def _serializeId(self, id):
        return _serializeAbbreviations(id, self.parseContext.idReplacements)

    def serializeId(self, id):
        id = self._serializeId(id)
        if self.serializeIdAsRefs and self.inputRefPattern:
            match = self.inputRefPattern.match(id)
            if match:
                return match.expand(self.refTemplate)
        elif self.outputRefPattern and self.outputRefPattern.match(id):
            #distinguish id that match ref patterns, e.g. @id
            return '::' + id
        return id
        
    def serializeRef(self, uri, context):
        match = False
        if not self.explicitRefObjects:
            #check if the ref looks like our inputRefPattern
            #if it doesn't, create a explicit ref object
            if isinstance(uri, (str,unicode)) and self.inputRefPattern:
                #apply the match to the serialized version of the id 
                match = self.inputRefPattern.match(self._serializeId(uri))

        if self.explicitRefObjects or not match or context is not None:
            assert isinstance(uri, (str, unicode))        
            ref =  { self.parseContext.refName : self.serializeId(uri) }
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
            if prop == PROPBAG:
                propbag = obj
            elif prop == base.RDF_SCHEMA_BASE+u'member':
                childlist.append( stmt )
        return propbag, childlist

    def _finishPropList(self, childlist, idrefs, resScope, nestedLists):
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
                    self._finishPropList(nestedlist, idrefs, resScope, nestedLists)
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
        for res in nodes:
            #get an ordered list of statements with this resource as their subject
            childNodes = root.getProperties(res)
            if not childNodes:
                #no properties, don't include
                continue
                                   
            if self.omitEmbeddedIds and _isEmbeddedBnode(res):
                currentobj = {}
            else:
                idValue = self.serializeId(res)
                currentobj = { idName : idValue }
            
            currentlist = []
            #print 'adding to results', res.uri
            results[res] = currentobj
            #print 'res', res, res.childNodes
            
            #deal with sequences first
            resScopes = {}
            #build a map of properties with list values: prop => [rdf:member statements]
            currentobjListprops = {}
            for stmt in childNodes:
                if stmt.predicate == PROPSEQ:
                    #this will replace sequences
                    seqprop, childlist = lists.get(stmt.object, (None,None))
                    if seqprop is None:
                        continue #dangling SEQPROP
                    key = self.serializeProp(seqprop)
                    currentobjListprops[key] = childlist, [(s[2],s[3]) for s in childlist]
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
                
                key = self.serializeProp(prop)
                if key in currentobjListprops:
                    #prop already handled by _setPropSeq
                    assert key not in self.parseContext.reservedNames
                    childlist, listvalues = currentobjListprops[key]
                    if key not in currentobj:
                        self._finishPropList(childlist, idrefs, resScope, lists)
                        currentobj[key] = childlist
                    if (stmt[2], stmt[3]) in listvalues:
                        currentlist = []
                        continue
                    #statement's value not in sequence, have the value added to the list
                    currentlist = childlist

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
            includeObject = len(refs) <= 1 and (not self.onlyEmbedBnodes or isBnode(id))
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

        if self.saveOrder:
            topObjects = []
            for _id in self.saveOrder:
                obj = roots.pop(_id, None)
                if obj is not None:
                    topObjects.append(obj)
            topObjects.extend(roots.values())
        else:
            topObjects = roots.values()
            
        if self.asList:
            header = dict(pjson=VERSION)
            if self.nameMap:
                header['namemap'] = self.nameMap            
            return [header] + topObjects
        
        retval = { 'data': topObjects }
        if self.includeObjectMap:
            #XXX results' keys are datastore's ids, not the serialized refs
            #object map should have serialized refs so that this can be used
            #as a look up table
            retval['objects'] = results
        if self.nameMap:
            retval['namemap'] = self.nameMap
        retval['pjson'] = VERSION
        return retval

def _serializeAbbreviations(name, replacements):
    for r in replacements:
        match = r.serializePattern.match(name)
        if match:
            return match.expand(r.serializeTemplate)

    # if we have replacement for 'rdf:' 
    # we don't want to serialize a property name like 'rdf:foo' as is
    # to prevent this we use a default replacement {'' : '::'} so the 
    # property will be serialized as '::rdf:'
    #(and we disallow replacements that match '::...') by prepending (?!::)
    #to all patterns)
    for r in replacements:
        if r.parsePattern.match(name):
            #this prop matches an pattern
            return '::' + name
    #::name => ::::name
    if name.startswith('::'):
        return '::' + name
    return name

def _parseAbbreviations(qname, replacements):    
    for r in replacements:
        match = r.parsePattern.match(qname)
        if match:
            return match.expand(r.parseTemplate)
    if qname.startswith('::'):
        return qname[2:]
    return qname        

class ParseContext(object):    
    context = None
    
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
            self.refName = self.nameMap.get('$ref', parent.refName)
            self.refpatternValue = self.nameMap.get('refpattern', parent.refpatternValue)
            self.idpatternsValue = self.nameMap.get('idpatterns', parent.idpatternsValue)
            self.propertypatternsValue = self.nameMap.get('propertypatterns', parent.propertypatternsValue)
            self.sharedpatternsValue = self.nameMap.get('sharedpatterns',parent.sharedpatternsValue)
            self.exclude = self.nameMap.get('exclude',parent.exclude)
            self.datatypepatternsValue = self.nameMap.get('datatypepatterns', parent.datatypepatternsValue)
        else:
            self.idName = self.nameMap.get('id', 'id')
            self.contextName = self.nameMap.get('context', 'context')
            self.namemapName = self.nameMap.get('namemap', 'namemap')
            self.datatypeName = self.nameMap.get('datatype', 'datatype')
            self.refName = self.nameMap.get('$ref', '$ref')
            self.refpatternValue = self.nameMap.get('refpattern')
            self.idpatternsValue = self.nameMap.get('idpatterns')
            self.propertypatternsValue = self.nameMap.get('propertypatterns')
            self.sharedpatternsValue = self.nameMap.get('sharedpatterns')
            self.exclude = self.nameMap.get('exclude', [])
            self.datatypepatternsValue = self.nameMap.get('datatypepatterns', {})
            
        self.validateProps()
        self.reservedNames = (self.idName, self.contextName, self.refName, 
            self.datatypeName, 'pjson', 
            #it's the parent context's namemap propery that is in effect:
            parent and parent.namemapName or 'namemap')
                            
        self.idrefpattern, self.refTemplate = None, None
        self._setIdRefPattern(self.refpatternValue)
        self.propReplacements = self._setReplacements(self.propertypatternsValue)
        self.idReplacements = self._setReplacements(self.idpatternsValue)
        self.datatypeReplacements = self._setDataTypePatterns(self.datatypepatternsValue)
        self.currentProp = None

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, ParseContext):
            return False 
        for name in ('reservedNames', 'idrefpattern', 'refTemplate', 
                    'propReplacements', 'idReplacements', 'datatypeReplacements'):
            if getattr(self, name) != getattr(other, name):
                return False

    def __hash__(self):
        return hash((tuple(getattr(self, name) for name in ('reservedNames', 
            'idrefpattern', 'refTemplate','propReplacements', 'idReplacements')),
             tuple(self.datatypeReplacements.iteritems())))
        
    def validateProps(self):
        for name in 'id', 'context', 'namemap', 'datatype', 'ref':
            if not isinstance(getattr(self, name+'Name'), (str, unicode)):
                raise RuntimeError('value of "%s" property must be a string' % name)

        if not isinstance(self.exclude, list):
            raise RuntimeError('value of "exclude" property must be a list')
        if not isinstance(self.datatypepatternsValue, dict):
            raise RuntimeError('value of "datatypes" property must be an object')

        #XXX: more validation
        
    def nameMapChanges(self):
        if self.parent and self.nameMap != self.parent.nameMap:
            return True
        return False

    def _setReplacements(self, more):
        replacements = []        
        for d in self.sharedpatternsValue, more:            
            if d:
                if isinstance(d, dict):                                    
                    for key in d:
                        replacements.append(_parseIdRefPattern(d, key, True))
                else:
                    replacements.append(_parseIdRefPattern(d))
        replacements.sort(key=lambda v: v.weight)
        return tuple(replacements)
            
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
            
    def parseProp(self, name):
        if not isinstance(name, (str,unicode)):
            raise RuntimeError("propery %r must a be a string" % name)
        return _parseAbbreviations(name, self.propReplacements)

    def _parseId(self, name):        
        return _parseAbbreviations(name, self.idReplacements)

    def parseId(self, name):
        ident = self.lookslikeIdRef(name)
        if ident:
            return ident
        else:
            return self._parseId(name)
        
    def _setIdRefPattern(self, idrefpattern):
        if idrefpattern:
            patterns = _parseIdRefPattern(idrefpattern)
            self.idrefpattern = patterns.parsePattern
            self.refTemplate = patterns.parseTemplate
        elif idrefpattern is not None: 
            #if property present but has empty value, remove current pattern
            self.idrefpattern, self.refTemplate = None, None

    def _setDataTypePatterns(self, datatypes):
        '''
        { datatype : pattern }
        or
        { datatype : [patterns] } 
        where pattern is either 'pattern' or { 'pattern' : 'replacement' }
        '''
        patterns = {}
        for datatype, replacements in datatypes.items():
            if not isinstance(replacements, list):
                replacements = [_parseIdRefPattern(replacements)]
            else:
                replacements = [_parseIdRefPattern(r) for r in replacements]
                replacements.sort(key=lambda v: v.weight)
            
            patterns[pjsonDataTypeToRdfDataType(datatype)] = tuple(replacements)
        return patterns
        
    def lookslikeIdRef(self, s):
        #XXX think about case where if number were ids
        if not isinstance(s, (str,unicode)):
            return False
        if not self.idrefpattern:
            return False

        m = self.idrefpattern.match(s)
        if m is not None:
            #this string looks the ref pattern
            #so use the ref template to generate the resource id
            res = m.expand(self.refTemplate)
            #the id might be an abbreviation to try to parse that
            res = self._parseId(res)
            return res
        return False

    def deduceDataType(self, s):
        datatypes = self.datatypeReplacements
        if datatypes:
            for datatype, replacements in datatypes.items():
                for r in replacements:
                    match = r.parsePattern.match(s)
                    if match:
                        return match.expand(r.parseTemplate), datatype
        return s, OBJECT_TYPE_LITERAL

def generateUUIDSequence(start=0, prefix='test:'):
    '''
    Generates a deterministic sequence of UUIDs. Useful only for testing.
    
    usage:
    vesper.pjson.Parser(generateUUID=vesper.pjson.generateUUIDSequence())
    '''    
    def generateUUID():
        seq = start
        while True:
            seq+=1
            yield uuid.uuid3(uuid.NAMESPACE_URL, '%s%04d'%(prefix, seq))
    generator = generateUUID()
    return lambda obj: 'uuid:' + str(generator.next())

class Parser(object):
    '''
    Parses pJSON compliant JSON and converts it to RDF statements.
    '''    
    bnodeprefix = '_:'    
    
    def __init__(self,addOrderInfo=True, 
            generateBnode=None, 
            scope = '', 
            setBNodeOnObj=False,
            nameMap=None,
            useDefaultRefPattern=True,
            toplevelBnodes=True,
            generateUUID=None,
            saveOrder=None,
            checkForDuplicateIds=True):
        generateBnode = generateBnode or _defaultBNodeGenerator
        self._genBNode = generateBnode
        if generateBnode == 'uuid': #XXX hackish
            self.bnodeprefix = base.BNODE_BASE
        self.addOrderInfo = addOrderInfo
        self.setBNodeOnObj = setBNodeOnObj
        self.toplevelBnodes = toplevelBnodes
        self._bnodeCounter = 0
        self.generateUUID = generateUUID or (lambda obj: 'uuid:' + str(uuid.uuid4()))
        self.saveOrder = saveOrder
        self.checkForDuplicateIds = checkForDuplicateIds
    
        nameMap = nameMap or {}
        if useDefaultRefPattern and 'refpattern' not in nameMap:
            nameMap['refpattern'] = defaultRefPattern
        self.defaultParseContext = ParseContext(nameMap)
        self.defaultParseContext.context = scope
    
    def to_rdf(self, json, scope = None):
        stmts = []
        emptyObjects = [] #XXX have an option to add directly to a store
        canHandleStatementWithOrder = False #XXX make this an option for use with store than can do this
        parentid = ''
        parseContext = self.defaultParseContext
        if scope is None:
            scope = parseContext.context

        def getorsetid(obj):
            newParseContext = ParseContext.initParseContext(obj, parseContext)
            objId = newParseContext.getProp(obj, 'id')
            if objId is None:
                if not parentid and not self.toplevelBnodes:
                    objId = self.generateUUID(obj)
                else:
                    #mark bnodes for nested objects differently                
                    prefix = parentid and 'j:e:' or 'j:t:'
                    suffix = parentid and (str(parentid) + ':') or ''
                    objId = self._blank(prefix+'object:'+suffix)
                if self.setBNodeOnObj:
                    obj[ newParseContext.getName('id') ] = objId
                return objId, newParseContext
            elif not isinstance(objId, (unicode, str)):
                objId = str(objId) #OBJECT_TYPE_RESOURCEs need to be strings
            objId = newParseContext.parseId(objId)
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
        elif isinstance(json, list):
            start = list(json)
        else:
            raise RuntimeError("JSON must be an object or array")
        
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
            stmts.append( Statement(seq, RDF_MS_BASE+'type',
                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
            stmts.append( Statement(seq, RDF_MS_BASE+'type',
                STANDALONESEQTYPE, OBJECT_TYPE_RESOURCE, scope) )

            for i, item in enumerate(val):
                item, objecttype, itemscope = self.deduceObjectType(item, parseContext)
                if isinstance(item, dict):
                    itemid, itemParseContext = getorsetid(item)
                    stmts.append( Statement(seq,
                        RDF_MS_BASE+'_'+str(i+1), itemid, OBJECT_TYPE_RESOURCE, itemscope) )
                    todo.append( (item, (itemid, itemParseContext), parentid))
                elif isinstance(item, list):
                    nestedlistid = _createNestedList(item)
                    stmts.append( Statement(seq,
                            RDF_MS_BASE+'_'+str(i+1), nestedlistid, OBJECT_TYPE_RESOURCE, itemscope) )
                else: #simple type
                    stmts.append( Statement(seq, RDF_MS_BASE+'_'+str(i+1), item, objecttype, itemscope) )
            return seq
        
        alreadySeen = {}
        saveOrder = self.saveOrder
        while todo:
            obj, (id, parseContext), parentid = todo.pop(0)
            if self.checkForDuplicateIds and len(obj) > 1:
                if id in alreadySeen and alreadySeen[id] != obj:                                                                 
                    raise RuntimeError("duplicate id encountered: %s" % id)
                else:
                    alreadySeen[id] = obj
                  
            if saveOrder is not None: saveOrder.append(id)
            istoplevel = not parentid
            
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
                if parseContext.isReservedPropertyName(prop) or prop in parseContext.exclude:
                    continue                
                prop = parseContext.parseProp(prop)
                parseContext.currentProp = prop
                val, objecttype, scope = self.deduceObjectType(val, parseContext)
                if isinstance(val, dict):
                    objid, valParseContext = getorsetid(val)
                    stmts.append( Statement(id, prop, objid, OBJECT_TYPE_RESOURCE, scope) )    
                    todo.append( (val, (objid, valParseContext), parentid) )
                elif isinstance(val, list):
                    #dont build a PROPSEQTYPE if prop in rdf:_ rdf:first rdfs:member                
                    specialprop = prop.startswith(RDF_MS_BASE+'_') or prop in [
                                  RDF_MS_BASE+'first', RDF_SCHEMA_BASE+'member']
                    addOrderInfo = not specialprop and self.addOrderInfo
                    #XXX special handling for prop == PROPSEQ ?
                    if not val:
                        stmts.append( Statement(id, prop, RDF_MS_BASE+'nil', 
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
                    
                    stmts.extend(listStmts)
                    
                    if addOrderInfo and not canHandleStatementWithOrder:
                        lists = {}
                        for s in listStmts:
                            if not isinstance(s, StatementWithOrder):
                                continue
                            value = (s[2], s[3])            
                            ordered  = lists.setdefault( (s[4], s[0], s[1]), [])
                            for p in s.listpos:
                                ordered.append( (p, value) )
                        if lists:
                            self.generateListResources(stmts, lists)
                                
                else: #simple type
                    stmts.append( Statement(id, prop, val, objecttype, scope) )
            
            if istoplevel and not parseContext.currentProp:
                emptyObjects.append(id)
            parseContext.currentProp = None
            
        return stmts, emptyObjects

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
            self._bnodeCounter+=1
            return self.bnodeprefix + prefix + str(self._bnodeCounter)
        else:
            return self._genBNode(prefix)
    
    def isEmbeddedBnode(self, id):
        if not isinstance(id, (str,unicode)):
            return False
        if id.startswith(self.bnodeprefix + 'j:e:') or id.startswith(self.bnodeprefix + 'j:proplist:'):
            return True
        return False

    def deduceObjectType(self, item, parseContext):
        if isinstance(item, list):
            return item, None, parseContext.context
        if isinstance(item, multipartjson.BlobRef):
            item = item.resolve()
            context = parseContext.context
            res = parseContext.lookslikeIdRef(item)
        elif isinstance(item, dict):
            size = len(item)
            hasContext = int('context' in item)
            refName = parseContext.getName('$ref')
            context = item.get('context', parseContext.context)
            if refName in item:
                #if refname is the same as the id name only 
                #treat as reference if item has no other properties
                if refName != parseContext.getName('id') or size == 1 + hasContext:
                    ref = parseContext.parseId( item[refName] )
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
                return value, pjsonDataTypeToRdfDataType(objectType), context
        else:
            res = parseContext.lookslikeIdRef(item)
            context = parseContext.context
        
        if res:
            return res, OBJECT_TYPE_RESOURCE, context
        else:
            value, valueType = getDataType(item, parseContext)
            return value, valueType, context

    def generateListResources(self, stmts, lists):
        '''
        Generate property list resources
        `lists` is a dictionary: (scope, subject, prop) => [(pos, (object, objectvalue))+]
        '''
        #print 'generateListResources', lists
        for (scope, subject, prop), ordered in lists.items(): 
            #use special bnode pattern so we can find these quickly
            seq = self.bnodeprefix + 'j:proplist:' + subject+';'+prop
            stmts.append( Statement(seq, RDF_MS_BASE+'type', 
                RDF_MS_BASE+'Seq', OBJECT_TYPE_RESOURCE, scope) )
            stmts.append( Statement(seq, RDF_MS_BASE+'type', 
                PROPSEQTYPE, OBJECT_TYPE_RESOURCE, scope) )
            stmts.append( Statement(seq, PROPBAG, prop, 
                OBJECT_TYPE_RESOURCE, scope) )
            stmts.append( Statement(seq, PROPSUBJECT, subject, 
                OBJECT_TYPE_RESOURCE, scope) )
            stmts.append( Statement(subject, PROPSEQ, seq, OBJECT_TYPE_RESOURCE, scope) )

            ordered.sort()
            for pos, (item, objecttype) in ordered:
                stmts.append( Statement(seq, RDF_MS_BASE+'_'+str(pos+1), item, objecttype, scope) )

def tojson(statements, **options):
    results = Serializer(**options).to_pjson(statements)
    return results#['results']

def tostatements(contents, **options):
    return Parser(**options).to_rdf(contents)[0]

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        try:
            f = open(sys.argv[1])
            contents = f.read()
        except:
            contents = sys.argv[1]
        print contents
        for s in tostatements(contents):
            print s

