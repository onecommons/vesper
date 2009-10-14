'''
    Various utility functions and classes for RxPath

    Copyright (c) 2004-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''

import re, types
try:
    import cStringIO
    StringIO = cStringIO
except ImportError:
    import StringIO

from rx import utils
from rx.python_shim import *

#try:
#    from Ft.Rdf import OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL    
#    #note: because we change these values here other modules need to import this module
#    #before importing any Ft.Rdf modules
#    import Ft.Rdf
#    Ft.Rdf.BNODE_BASE = 'bnode:'
#    Ft.Rdf.BNODE_BASE_LEN = len('bnode:')
#    from Ft.Rdf import BNODE_BASE, BNODE_BASE_LEN
#except ImportError:
#4Suite RDF not installed    
OBJECT_TYPE_RESOURCE = "R"
OBJECT_TYPE_LITERAL = "L"

BNODE_BASE = 'bnode:'
BNODE_BASE_LEN = len('bnode:')
    
#from Ft.Rdf import RDF_MS_BASE,RDF_SCHEMA_BASE
#for some reason we need this to be unicode for the xslt engine:
RDF_MS_BASE=u'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
OBJECT_TYPE_XMLLITERAL='http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral'
RDF_SCHEMA_BASE=u"http://www.w3.org/2000/01/rdf-schema#"

_bNodeCounter  = 0
#like this so this will be a valid bNode token (NTriples only allows alphanumeric, no _ or - etc.
try:
    import uuid
    _sessionBNodeUUID = 'x'+ uuid.uuid4().hex
except ImportError:
    try:
        from Ft.Lib import Uuid
        _sessionBNodeUUID = "x%032xx" % Uuid.GenerateUuid()
    except ImportError:
        import random
        _sessionBNodeUUID = "x%032xx" % random.getrandbits(16*8)

#todo rename this class to MutableStatement
#write a Statement class derived from tuple, add a BaseStatement marker class

class BaseStatement(object):
    __slots__ = ()
    
    SUB_POS = 0
    PRED_POS = 1
    OBJ_POS = 2
    OBJTYPE_POS = 3
    SCOPE_POS = 4
    LIST_POS = 5

class Statement(tuple, BaseStatement):
    #__slots__ = ('listpos', )

    def __new__(cls, subject, predicate, object,
             objectType=OBJECT_TYPE_LITERAL, scope=''):
        return tuple.__new__(cls, (subject, predicate, object,
             objectType,scope) )

    subject = property(lambda self: self[0])
    predicate = property(lambda self: self[1])
    object = property(lambda self: self[2])
    objectType = property(lambda self: self[3])
    scope =  property(lambda self: self[4])
    listpos =  None

class _BaseTriple(object):

    def __hash__( self):
        #for now don't include scope
        return hash(self[:4])

    def __eq__(self, other):
        #for now don't compare scope        
        if isinstance(other, (tuple,list)):
            return self[:4] == other[:4]
        else:
            return False

    def __ne__(self, other):
        #for now don't compare scope        
        if isinstance(other, (tuple,list)):
            return self[:4] != other[:4]
        else:
            return False
    
    def __cmp__(self, other):
        #for now don't compare scope
        if isinstance(other, (tuple,list)):
            return cmp(self[:4],other[:4])
        else:
            return False

class Triple(_BaseTriple, Statement):
    pass

class MutableStatement(list, BaseStatement):
    __slots__ = ()
        
    def __init__(self, subject, predicate, object,
             objectType=OBJECT_TYPE_LITERAL, scope=''):
        super(MutableStatement, self).__init__( (subject, predicate, object,
             objectType,scope) )

    subject = property(lambda self: self[0], lambda self, x: self.__setitem__(0, x))
    predicate = property(lambda self: self[1], lambda self, x: self.__setitem__(1, x))
    object = property(lambda self: self[2], lambda self, x: self.__setitem__(2, x))    
    objectType = property(lambda self: self[3], lambda self, x: self.__setitem__(3, x))
    scope =  property(lambda self: self[4], lambda self, x: self.__setitem__(4, x))
    listpos =  None
    
    #def append(self, o): raise TypeError("append() not allowed")
    def extend(self, o): raise TypeError("extend() not allowed")
    def pop(self): raise TypeError("pop() not allowed")    

class MutableTriple(_BaseTriple, MutableStatement):
    pass

class StatementWithOrder(Statement):
    
    def __new__(cls, subject, predicate, object,
             objectType=OBJECT_TYPE_LITERAL, scope='', listpos=()):        
        obj = tuple.__new__(cls, (subject, predicate, object,
             objectType,scope) )
        obj.listpos = tuple(listpos)
        return obj

    def __repr__(self):
        return 'StatementWithOrder'+repr((self+(self.listpos,)))


class ParseException(utils.NestedException):
    def __init__(self, msg = ''):                
        utils.NestedException.__init__(self, msg,useNested = True)

def generateBnode(name=None, prefix=''):
    """
    Generates blank nodes (bnodes), AKA anonymous resources
    """
    global _bNodeCounter, _sessionBNodeUUID
    _bNodeCounter += 1
    name = name or str(_bNodeCounter)    
    return BNODE_BASE + prefix + _sessionBNodeUUID +  name
        
def NTriples2Statements(stream, defaultScope='', baseuri=None,
    charencoding='utf8', incrementHook=None):
    makebNode = lambda bNode: BNODE_BASE + bNode
    stmtset = {}
    for stmt in _parseTriples(
        stream,  makebNode, charencoding=charencoding, baseuri=baseuri,
        yieldcomments=incrementHook):        
        if stmt[0] is Removed:
            stmt, forContext = stmt[1], stmt[2]
            if incrementHook:
                stmt = incrementHook.remove(stmt, forContext)
            stmt = stmt[:4] #don't include scope in key
            if stmt in stmtset:
                del stmtset[stmt]
        elif stmt[0] is Comment:
            incrementHook.comment(stmt[1])
        else:
            if incrementHook:
                stmt = incrementHook.add(stmt)
            #don't include scope in key
            stmtset[stmt[:4]] = stmt[4]                
    for stmt, scope in stmtset.iteritems():
        yield Statement(stmt[0], stmt[1], stmt[2],stmt[3],
                                 scope or defaultScope)
                                 
def _parseSPARQLResults(json, defaultScope=None):
    #this currently isn't used
    stmts = []
    for stmt in json['results']['bindings']:
        stmts.append( Statement(_decodeJsonRDFValue(stmt['s'])[0],
            _decodeJsonRDFValue(stmt['p'])[0],
            *_decodeJsonRDFValue(stmt['o'])+(defaultScope,)  ))
    return stmts

def _parseRDFJSON(jsonstring, defaultScope=None):
    '''
    Parses JSON that followes the format described in _encodeRDFAsJson
    Returns a list of RDF Statements
    '''
    stmts = []
    jsonDict = json.loads(jsonstring)
    if 'quads' in jsonDict:
        del jsonDict['quads']
        items = jsonDict.iteritems()
    else:
        items = ((defaultScope, jsonDict),)
    for scope, tripleDict in items:
        for subject, values in tripleDict.iteritems():
            if subject == 'triples':
                continue
            if subject.startswith('_:'):
                subject = BNODE_BASE+subject[2:] #bNode            
            for pred, objects in values.iteritems():
                for count, object in enumerate(objects):                    
                    value, objectType = _decodeJsonRDFValue(object)        
                    if pred == '#member':
                        stmtpred = base+'_'+str(count+1)
                    else:
                        stmtpred = pred
                    #todo:
                    assert pred not in ['#first','#rest'], 'not yet implemented' 
                    stmts.append( 
                        Statement(subject, stmtpred, value, objectType, scope) )
    return stmts

def _decodeJsonRDFValue(object):
    objectType = object['type']
    value = object['value']
    if objectType == 'uri':
        objectType = OBJECT_TYPE_RESOURCE
    elif objectType == 'bnode':
        objectType = OBJECT_TYPE_RESOURCE
        value = BNODE_BASE + value                        
    elif objectType == 'literal':
        if 'xml:lang' in object:
            objectType = object['xml:lang']
        else:
            objectType = OBJECT_TYPE_LITERAL
    elif objectType == 'typed-literal':
        objectType = object['datatype']
    else:
        raise ParseException('unexpected object type', objectType)
    return value, objectType

def _encodeStmtObject(stmt, iscompound=False):
    return encodeStmtObject(stmt.object, stmt.objectType, iscompound)
    
def encodeStmtObject(object, type, iscompound=False):    
    '''
    return a string encoding the object in one of these ways: 
    (from http://www.w3.org/TR/2006/NOTE-rdf-sparql-json-res-20061004/)
    {"type":"literal", ["xml:lang":" L ",] "value":" S"},
    {"type":"typed-literal", "datatype":" D ", "value":" S "},
    {"type":"uri|bnode", "value":"U"", "hint":"compound"} ] },
    '''
    jsonobj = {}    
    if type == OBJECT_TYPE_RESOURCE:
        if object.startswith(BNODE_BASE):
            bnode = object[BNODE_BASE_LEN:]
            jsonobj['type'] = 'bnode'
            jsonobj['value'] = bnode
        else:
            jsonobj['type'] = 'uri'
            jsonobj['value'] = object
        
        if iscompound:
            jsonobj['hint'] = 'compound'
    else:        
        if type.find(':') > -1:
            jsonobj['type'] = "typed-literal"
            jsonobj["datatype"] = type
            jsonobj['value'] = object
        else:
            jsonobj['type'] = "literal"            
            if type != OBJECT_TYPE_LITERAL:
                #datatype is the lang
                jsonobj["xml:lang"] = type                    
            jsonobj['value'] = object

    return jsonobj

def _encodeRDFAsJson(nodes):    
    '''
Encode a nodeset of RxPath resource nodes as JSON, using this format:
{ 'quads':'1' , 'contexturi' :  
    { 
      'resourceuri' : { 'pred' : [
      #from http://www.w3.org/TR/2006/NOTE-rdf-sparql-json-res-20061004/
    {"type":"literal", ["xml:lang":" L ",] "value":" S"},
    {"type":"typed-literal", "datatype":" D ", "value":" S "},
    {"type":"uri|bnode", "value":"U"", "hint":"compound"} ] },
        'containerresuri' : { '#member' : [] },
        'listresourceuri' : { '#first' : [], '#rest' : ['rdf:nil'] }
    }
 }          
    '''    
    out = '{'
    lastres = None
    compoundResources = [] #we want closure on these resources
    for res in nodes:
        assert hasattr(res,'uri'), '_encodeRDFAsJson only can encode resource nodes'
        if lastres:
            out +=','
        lastres = res
        if res.uri.startswith(BNODE_BASE):
            uri = '_:' + res.uri[BNODE_BASE_LEN:]
        else:
            uri = res.uri
        out += '"' + uri + '": {'
        lastpred = None
        #assume ordered predicated
        for pred in res.childNodes:
            if lastpred == pred.stmt.predicate:                
                isCompound = False #todo: pred.childNodes[0].isCompound()
                out += ", " + json.dumps(_encodeStmtObject(pred.stmt, isCompound))
                if isCompound:
                    compoundResources.append(pred.childNodes[0])
            else:
                if lastpred:
                    out += '], '
                lastpred = pred.stmt.predicate
                out +=  '"' + lastpred + '" : ['
                isCompound = False #todo: pred.childNodes[0].isCompound()
                out += json.dumps(_encodeStmtObject(pred.stmt, isCompound))
        if lastpred:
            out += ']\n'
        out += '}\n'
    #for node in compoundResources:
    #    '"' + node.uri + ': {'
    #    add "rdf:rest" and add listIds 
    out += '}'
    return out

def parseRDFFromString(contents, baseuri, type='unknown', scope=None,
                       options=None, getType=False):
    '''
    returns an iterator of Statements
    
    type can be anyone of the following:
        "rdfxml", "ntriples", "sjson", "yaml", and "unknown"
    If Redland is installed "turtle" and "rss-tag-soup" will also work.

    baseuri is the base URI to be used for relative URIs in the RDF source
    '''    
    stmts, type = _parseRDFFromString(contents, baseuri, type, scope, options)
    if getType:
        return stmts, type
    else:
        return stmts

def _parseRDFFromString(contents, baseuri, type='unknown', scope=None,
                       options=None):
    from rx import RxPath
    if scope is None: scope = ''  #workaround 4suite bug
    options = options or {}

    if type.startswith('http://rx4rdf.sf.net/ns/wiki#rdfformat-'):
        type = type.split('-', 1)[1]
        
    if isinstance(contents, unicode):
        contents = contents.encode('utf8')
            
    try:
        while type == 'unknown':
            if isinstance(contents, (list, tuple)):
                if not contents:
                    return contents, 'statements'
                if isinstance(contents[0], (tuple, BaseStatement)):
                    return contents, 'statements' #looks like already a list of statements
                #otherwise assume sjson
                type='sjson' 
                break
            elif isinstance(contents, dict):
                type='sjson' #assume sjson
                break
            
            startcontents = contents[:256].lstrip()
            if not startcontents: #empty
                return [], 'statements'
                
            if startcontents[0] in '{[':
                type='sjson' #assume sjson
                break
            elif startcontents.startswith('#?zml'):
                from rx import zml
                xml = zml.zmlString2xml(contents, mixed=False, URIAdjust=True)
            try:
                import htmlfilter
                ns, prefix, local = htmlfilter.getRootElementName(contents)
                if ns == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#':
                    type = 'rdfxml'
                elif local == 'rx':
                    type = 'rxml_xml'
                elif ns == 'http://purl.org/atom/ns#' or local == 'rss':
                    type = "rss-tag-soup"
                else:
                    raise ParseException(
                        "RDF parse error: Unsupported XML vocabulary: " + local)
            except:
                #hmmm, try our NTriples parser
                try:
                    #convert generator to list to force parsing now 
                    return list(NTriples2Statements(
                                StringIO.StringIO(contents), scope,
                                baseuri, **options))
                except:
                    #maybe its n3 or turtle, but we can't detect that
                    #but we'll try rxml_zml
                    type = 'rxml_zml'
                            
        if type in ['ntriples', 'ntjson']:
            #use our parser
            return NTriples2Statements(StringIO.StringIO(contents), scope,
                                       baseuri, **options), type
        elif type == 'rxml_xml' or type == 'rxml_zml':
            if type == 'rxml_zml':
                from rx import zml
                contents = zml.zmlString2xml(contents, mixed=False, URIAdjust=True)
                start = contents[:200].lstrip()
                if not (start.startswith('<rx>') or start.startswith('<rx:rx')):
                    contents ='<rx:rx>'+ contents+'</rx:rx>'
            from rx import rxml            
            return rxml.rx2statements(StringIO.StringIO(contents), scope=scope), type
        elif type == 'turtle' or type == 'rss-tag-soup':
            try: #only redland's parser supports these
                import RDF
                parser=RDF.Parser(type)
                stream = parser.parse_string_as_stream(contents, baseuri)
                return RxPath.redland2Statements(stream, scope), type 
            except ImportError:
                raise ParseException("RDF parse error: "+ type+
                    " is only supported by Redland, which isn't installed")
        elif type == 'rdfxml':
            try:
                #if rdflib is installed, use its RDF/XML parser because it doesn't suck            
                import rdflib
                try:
                    ts = rdflib.Graph()
                except AttributeError:
                    #for old versions of rdflib (e.g. 2.0.6)
                    import rdflib.TripleStore
                    ts = rdflib.TripleStore.TripleStore()
                
                import StringIO as pyStringIO #cStringIO can't have custom attributes
                contentsStream = pyStringIO.StringIO(contents)
                #xml.sax.expatreader.ExpatParser.parse() calls
                #xml.sax.saxutils.prepare_input_source which checks for 
                #a 'name' attribute to set the InputSource's systemId
                contentsStream.name = baseuri 
                ts.parse(contentsStream)
                return RxPath.rdflib2Statements(
                            ts.triples( (None, None, None)),scope), type
            except ImportError:
                try: #try redland's parser
                    import RDF
                    parser=RDF.Parser('rdfxml')
                    stream = parser.parse_string_as_stream(contents, baseuri)
                    return RxPath.redland2Statements(stream, scope), type 
                except ImportError:
                    #fallback to 4Suite's obsolete parser
                    try:
                        from Ft.Rdf import Util
                        model, db=Util.DeserializeFromString(contents,scope=baseuri)
                        statements = RxPath.Ft2Statements(model.statements())
                        #we needed to set the scope to baseuri for the parser to
                        #resolve relative URLs, so we now need to reset the scope
                        for s in statements:
                            s.scope = scope
                        return statements, type
                    except ImportError:
                        raise ParseException("no RDF/XML parser installed")
        elif type == 'json':            
            return _parseRDFJSON(contents, scope), type
        elif type == 'sjson' or type == 'yaml':
            import sjson
            if isinstance(contents, str):
                if type == 'yaml':
                    import yaml
                    content = yaml.safe_load(contents)
                else:
                    contents = json.loads(contents)    

            options['scope'] = scope
            #XXX generateBnode doesn't detect collisions, maybe gen UUID instead            
            if 'generateBnode' not in options:
                options['generateBnode']=generateBnode
            
            return sjson.tostatements(contents, options), type
        else:
            raise ParseException('unsupported type: ' + type)
    except:
        #import traceback; traceback.print_exc()
        raise ParseException()

def parseRDFFromURI(uri, type='unknown', modelbaseuri=None, scope=None,
                    options=None, getType=False):
    'returns an iterator of Statements'
    options = options or {}
    if not modelbaseuri:
        modelbaseuri = uri
    if type == 'unknown':
        if uri[-3:] == '.nt':
            type = 'ntriples'
        elif uri[-4:] == '.rdf':
            type = 'rdfxml'

    import urllib2
    stream = urllib2.urlopen(uri)
    contents = stream.read()
    stream.close()
    return parseRDFFromString(contents, modelbaseuri, type, scope, options, getType)
     
def RxPathDOMFromStatements(statements, uri2prefixMap=None, uri=None,schemaClass=None):
    from rx import RxPath, RxPathSchema
    model = RxPath.MemModel(statements)    
    #default to no inferencing:
    return RxPath.createDOM(model, uri2prefixMap or {}, modelUri=uri,
                        schemaClass = schemaClass or RxPathSchema.BaseSchema) 

def serializeRDF(statements, type, uri2prefixMap=None,
                         fixUp=None, fixUpPredicate=None):
    stringIO = StringIO.StringIO()
    serializeRDF_Stream(statements, stringIO, type, uri2prefixMap=uri2prefixMap,
                         fixUp=fixUp, fixUpPredicate=fixUpPredicate)
    return stringIO.getvalue()
    
def serializeRDF_Stream(statements, stream, type, uri2prefixMap=None,
                         fixUp=None, fixUpPredicate=None):
    '''    
    type can be one of the following:
        "rdfxml", "ntriples", "ntjson", "json", "yaml", or "sjson"
    '''
    from rx import RxPath
    if type.startswith('http://rx4rdf.sf.net/ns/wiki#rdfformat-'):
        type = type.split('-', 1)[1]

    if type == 'rdfxml':
        try:
            #if rdflib is installed
            import rdflib.TripleStore
            ts = rdflib.TripleStore.TripleStore()
            if uri2prefixMap:
                ts.ns_prefix_map = uri2prefixMap
            
            for stmt in statements:
                ts.add( RxPath.statement2rdflib(stmt) )                    
            ts.serialize(format='xml', stream=stream)
        except ImportError:
            try: #try redland's parser
                import RDF
                serializer = RDF.RDFXMLSerializer()
                model = RDF.Model()
                for stmt in statements:
                   model.add_statement(RxPath.statement2Redland(stmt))
                if uri2prefixMap:
                    for uri,prefix in uri2prefixMap.items():
                        if prefix != 'rdf': #avoid duplicate ns attribute bug
                            serializer.set_namespace(prefix.encode('utf8'), uri)
                out = serializer.serialize_model_to_string(model)
                stream.write(out)
            except ImportError:
                try:
                    #fall back to 4Suite
                    from Ft.Rdf.Drivers import Memory    
                    db = Memory.CreateDb('', 'default')
                    import Ft.Rdf.Model
                    model = Ft.Rdf.Model.Model(db)
                    from rx import RxPath
                    for stmt in statements:
                        model.add(RxPath.statement2Ft(stmt) )  
                    from Ft.Rdf.Serializers.Dom import Serializer as DomSerializer
                    serializer = DomSerializer()
                    outdoc = serializer.serialize(model, nsMap = uri2prefixMap)
                    from Ft.Xml.Lib.Print import PrettyPrint
                    PrettyPrint(outdoc, stream=stream)
                except ImportError:
                    raise ParseException("no RDF/XML serializer installed")
    elif type == 'ntriples':
        writeTriples(statements, stream)
    elif type == 'ntjson':
        writeTriples(statements, stream, writejson=True)
    elif type == 'sjson' or type == 'yaml':
        import sjson 
        #XXX use uri2prefixMap
        return json.dump( sjson.tojson(statements),stream)
    elif type == 'yaml':
        import sjson, yaml 
        #XXX use uri2prefixMap
        return yaml.safe_dump( sjson.tojson(statements), stream)
    elif type == 'json':
        rdfDom = RxPathDOMFromStatements(statements, uri2prefixMap)
        subjects = [s.subject for s in statements]
        nodes = [node for node in rdfDom.childNodes
                     if node.uri in subjects and not node.isCompound()]
        out = _encodeRDFAsJson(nodes)
        stream.write(out)

def canWriteFormat(format):
    if format in ('ntriples', 'ntjson', 'json', 'sjson'):
        return True
    elif format == 'yaml':
        try:
            import yaml
            return True
        except ImportError:
            return False
    elif format == 'rdfxml':
        for pkg in ('rdflib.TripleStore', 'RDF', 'Ft.Rdf.Serializers.Dom'):        
            try:
                __import__(pkg)
                return True
            except ImportError:
                pass
        return False
    else:
        return False
    
#see w3.org/TR/rdf-testcases/#ntriples 
#todo: assumes utf8 encoding and not string escapes for unicode 
Removed = object()
Comment = object()
def _parseTriples(lines, bNodeToURI = lambda x: x, charencoding='utf8',
                  baseuri=None, yieldcomments=False):
    remove = False
    graph = None
    lineCounter = 0
    assumeJson = False
    for line in lines:
        lineCounter += 1
        line = line.strip()
        if not line: #trailing whitespace
            break;
        if not isinstance(line, unicode):
           line = line.decode(charencoding)
        if line.startswith("#!json"):
            assumeJson = True
            continue
        if line.startswith('#!remove'):
            #this extension to NTriples allows us to incrementally update
            #a model using NTriples
            tokens = line.split(None)
            if len(tokens) > 1:
                removeForContext = tokens[1]
            else:
                removeForContext = ''
            remove = True
            continue
        if line.startswith('#!graph'):
            #this extension to NTriples allows us to support named graphs
            tokens = line.split(None)
            if len(tokens) > 1:
                graph = tokens[1]
            else:
                graph = None
            continue
        elif line[0] == '#': #comment            
            remove = False
            graph = None
            if yieldcomments:
                yield (Comment, line[1:])
            continue
        
        if assumeJson:
            jsondict = json.loads(line)
            for k, v in jsondict.items():
                if k == 'id':
                    subject = v
                    if subject.startswith('_:'): #bNode
                        subject = bNodeToURI(subject[2:])
                else:
                    predicate = k
                    object, objectType = _decodeJsonRDFValue(v)
        else:
            subject, predicate, object, objectType = _parseNTriplesLine(
                                                    line, baseuri, bNodeToURI)

        if remove:
            remove = False
            yield (Removed, (subject, predicate, object, objectType, graph), removeForContext)
        else:
            yield (subject, predicate, object, objectType, graph)
        graph = None

def _parseNTriplesLine(line, baseuri, bNodeToURI):
    subject, predicate, object = line.split(None,2)
    if subject.startswith('_:'):
        subject = subject[2:] #bNode
        subject = bNodeToURI(subject)
    else:
        subject = subject[1:-1] #uri
        if not subject: #<> refers to the baseuri
            if baseuri is None:
                raise RuntimeError("Unable to parse NTriples on line %i:" 
        "it contains a '<>' but no baseuri was specified." % lineCounter)
            subject = baseuri
        
    if predicate.startswith('_:'):
        predicate = predicate[2:] #bNode
        predicate = bNodeToURI(predicate)
    else:
        assert predicate[0] == '<' and predicate[-1] == '>', 'malformed predicate: %s' % predicate
        predicate = predicate[1:-1] #uri
        
    object = object.strip()        
    if object[0] == '<': #if uri
        object = object[1:object.find('>')]
        objectType = OBJECT_TYPE_RESOURCE
        if not object: #<> refers to the baseuri
            if baseuri is None:
                raise RuntimeError("Unable to parse NTriples on line %i:" 
        "it contains a '<>' but no baseuri was specified." % lineCounter)
            object = baseuri
    elif object.startswith('_:'):
        object = object[2:object.rfind('.')].strip()
        object = bNodeToURI(object)
        objectType = OBJECT_TYPE_RESOURCE
    else:                        
        quote = object[0] 
        endquote = object.rfind(quote)
        literal = object[1:endquote]
        if literal.find('\\') != -1:
            literal = re.sub(r'(\\[tnr"\\])|(\\u[\dA-F]{4})|(\\U[\dA-F]{8})', 
                 lambda m: str(m.group(0)).decode('unicode_escape'), literal)
        if object[endquote+1]=='@':
            lang = object[endquote+2:object.rfind('.')].strip()
            objectType = lang
        elif object[endquote+1]=='^':                
            objectType = object[endquote+3:object.rfind('.')].strip()
        else:    
            objectType = OBJECT_TYPE_LITERAL
        object = literal

    return subject, predicate, object, objectType
                   
def writeTriples(stmts, stream, enc='utf8', writejson=False):
    r'''
    stmts is an iterable of statements (or the equivalent tuples)

    Note that the default encoding is 'utf8'; to conform with standard NTriples spec,
    use 'ascii' instead.
    '''
    subject = 0
    predicate = 1
    object = 2
    objectType = 3
    scope = 4
    
    if writejson:
        stream.write("#!json\n")
    
    import re
    wspcProg = re.compile(r'\s')

    for stmt in stmts:       
        if stmt[0] is Comment:
            stream.write("#" + stmt[1].encode(enc, 'backslashreplace') + "\n")
            continue
        if stmt[0] is Removed:
            stmt = stmt[1]
            if getattr(stmt, 'removeForContext', False): #hack
                stream.write("#!remove "+stmt[scope].encode(enc)+"\n")
            else:
                stream.write("#!remove\n")
 
        for i in range(5):
            if i == object and stmt[objectType] != OBJECT_TYPE_RESOURCE:
                continue
            if wspcProg.search(stmt[i]): 
                raise RuntimeError("unable to write NTriples, statement "
                    "contains an invalid URI: %s" % stmt[i])
                      
        if stmt[scope]: 
            stream.write("#!graph "+stmt[scope].encode(enc)+"\n")
        
        if writejson:
            stream.write(stmt2json(stmt)+'\n')
            continue                   
            
        if stmt[subject].startswith(BNODE_BASE):
            stream.write('_:' + stmt[subject][BNODE_BASE_LEN:].encode(enc) ) 
        else:
            subjectURI = stmt[subject]
            stream.write("<" + subjectURI.encode(enc) + ">")
            
        if stmt[predicate].startswith(BNODE_BASE):
            stream.write( '_:' + stmt[predicate][BNODE_BASE_LEN:].encode(enc) ) 
        else:            
            stream.write(" <" + stmt[predicate].encode(enc) + ">")
        if stmt[objectType] == OBJECT_TYPE_RESOURCE:
            if wspcProg.search(stmt[subject]):
                raise RuntimeError("unable to write NTriples, statement scope "
                "is an invalid URI: %s" % stmt[scope])

            if stmt[object].startswith(BNODE_BASE):
                stream.write(' _:' + stmt[object][BNODE_BASE_LEN:].encode(enc)  + " .\n") 
            else:
                stream.write(" <" + stmt[object].encode(enc)  + "> .\n")
        else:           
            escaped = (stmt[object].replace('\\', r'\\').encode(enc, 'backslashreplace'))
            #fix differences between python and ntriples escaping:
            #* ascii range is escaped as \xXX instead of \u00XX,
            #* hex digits are lowercase instead of upper
            def fixEscaping(match):
                s = match.group(0)
                if s[0] == '\\':
                    if s == r'\\':
                        return r'\\'
                    elif s[1] == 'x':
                        return r'\u00' + s[2:].upper()
                    else:
                        assert s[1] in ('U','u')
                        return '\\' + s[1] + s[2:].upper()
                elif s[0] == '\n':
                    return r'\n'
                elif s[0] == '\r':
                    return r'\r'
                elif s[0] == '\t':
                    return r'\t'
                elif s[0] == '"':
                    return r'\"'
                else:
                    return '\u00' + hex(ord(s[0]))[2:].upper()
                                
            escaped = re.sub(r'(\\\\)|(\\x[\da-f]{2})|(\\u[\da-f]{4})'
                             r'|(\\U[\da-f]{8})|([\0-\31"])', fixEscaping, escaped)
           
            if stmt[objectType] == OBJECT_TYPE_LITERAL:
                stream.write(' "' + escaped + '" .\n')
            elif stmt[objectType].find(':') == -1: #must be a lang code
                stream.write(' "' + escaped + '"@' + stmt[objectType].encode(enc))
                stream.write(" .\n")
            else: #objectType must be a RDF datatype
                stream.write(' "' + escaped + '"^^' + stmt[objectType].encode(enc))
                stream.write(" .\n")

class Res(dict):
    ''' Simplifies building RDF statements with a dict-like object
    representing a resource with a dict of property/values

       usage:
       Res.nsMap = { ... } #global namespace map
       
       res = Res(resourceName, nsMap) #2nd param is optional instance override of global nsMap
       
       res['q:name'] = 'foo' #add a statement with property 'q:name' and object literal 'foo'
       
       res['q:name'] = Res('q:name2') #add a statement with property 'q:name' and object resource 'q:name2'
       
       #if prefix not found in nsMap it is treated as an URI
       res['http://foo'] = Res('http://bar') #add a statement with property http://foo and object resource 'http://bar'

       #if resourceName starts with '_:' it is treated as a bNode
       res['_:bNode1']
       
       #if you want multiple statements with the same property, use a list as the value, e.g.:
       res.setdefault('q:name', []).append(child)
       
       #retrieve the properties in the resource's dictionary as a NTriples string
       res.toTriples()

       #return a NTriples string by recursively looking at each resource that is the object of a statement
       res.toTriplesDeep()
    '''
    
    nsMap =  { 'owl': 'http://www.w3.org/2002/07/owl#',
           'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#' }

    def __init__(self, uri=None, nsMap = None):
        if nsMap is not None:
            self.nsMap = nsMap
        if uri is None:
            uri = '_:'+ generateBnode()[BNODE_BASE_LEN:]
        self.uri = self.getURI(uri)

    def __eq__(self, other):
        return self.uri == other.uri

    def __ne__(self, other):
        return self.uri != other.uri
    
    def __cmp__(self, other):
        return cmp(self.uri, other.uri)

    def __hash__(self):
        return hash(self.uri)        

    def __getitem__(self, key):
        return super(Res, self).__getitem__(self.getURI(key))
    
    def __setitem__(self, key, item):    
        return super(Res, self).__setitem__(self.getURI(key), item)

    def __delitem__(self, key):
        return super(Res, self).__delitem__(self.getURI(key))

    def __contains__(self, key):
        return super(Res, self).__contains__(self.getURI(key))

    def getURI(self, key):
        if key.startswith('_:'):
            return key #its a bNode
        index = key.find(':')
        if index == -1: #default ns
            prefix = ''
            local = key
        else:
            prefix = key[:index]
            local = key[index+1:]
        if self.nsMap.get(prefix) is not None:
            return self.nsMap[prefix] + local 
        else:#otherwise assume its a uri
            return key
    
    def toStatementsDeep(self):
        stmts = []
        curlist = [ self ]
        done = [ self ]
        while curlist:
            #print [x.uri for x in reslist], [x.uri for x in done]
            res = curlist.pop()
            stmts2, reslist = res.toStatements(done)
            done.extend(reslist)
            curlist.extend(reslist)
            stmts.extend(stmts2) 
        return stmts
        
    def toStatements(self, doneList = None):
        stmts = []
        reslist = []
        if self.uri.startswith('_:'):
            s = BNODE_BASE+self.uri[2:]
        else:
            s = self.uri
        for p, v in self.items():
            if p.startswith('_:'):
                #but note that in RDF, its technically illegal
                #for the predicate to be a bnode 
                p = BNODE_BASE + p[2:] 
            if not isinstance(v, (type(()), type([])) ):
                v = (v,)
            for o in v:                                    
                if isinstance(o, Res):                    
                    if o.uri.startswith('_:'):
                        oUri = BNODE_BASE+o.uri[2:]
                    else:
                        oUri = o.uri
                    stmts.append(Statement(s, p, oUri, objectType=OBJECT_TYPE_RESOURCE))
                    if doneList is not None and o not in doneList:
                        reslist.append(o)
                else: #todo: datatype, lang                    
                    stmts.append(Statement(s, p, o, objectType=OBJECT_TYPE_LITERAL))
        if doneList is None:
            return stmts
        else:
            return stmts, reslist

    def toTriplesDeep(self):
        t = ''
        curlist = [ self ]
        done = [ self ]
        while curlist:
            #print [x.uri for x in reslist], [x.uri for x in done]
            res = curlist.pop()
            t2, reslist = res.toTriples(done)
            done.extend(reslist)
            curlist.extend(reslist)
            t += t2
        return t

    def toTriples(self, doneList = None):
        triples = ''
        reslist = []
        if not self.uri.startswith('_:'):
            s = '<' + self.uri + '>'
        else:
            s = self.uri
        for p, v in self.items():
            if not p.startswith('_:'):
                p = '<' + p + '>'
            if not isinstance(v, (type(()), type([])) ):
                v = (v,)
            for o in v:                                    
                triples += s + ' ' + p
                if isinstance(o, Res):
                    if o.uri.startswith('_:'):
                        triples += ' '+ o.uri + '. \n'
                    else:
                        triples += ' <'+ o.uri + '>. \n'                        
                    if doneList is not None and o not in doneList:
                        reslist.append(o)
                else: #todo: datatype, lang
                    escaped = o.replace('\\', r'\\').replace('\"', r'\"').replace('\n', r'\n').replace('\r', r'\r').replace('\t', r'\t')
                    triples += ' "' + escaped.encode('utf8') + '" .\n'
        if doneList is None:
            return triples
        else:
            return triples, reslist

def addStatements(rdfDom, stmts):
    '''
    Update the DOM (and so the underlying model) with the given list of statements.
    If the statements include RDF list or container statements, it must include all items of the list
    '''
    #we have this complete list requirement because otherwise we'd have to figure out
    #the head list resource and update its children and possible also do this for every nested list 
    #resource not included in the statements (if the model exposed them)
    listLinks = {}
    listItems = {}
    tails = []
    containerItems = {}
    newNodes = []
    for stmt in stmts:
        #print 'stmt', stmt
        if stmt.predicate == RDF_MS_BASE+'first':
            listItems[stmt.subject] = stmt
            #we handle these below
        elif stmt.predicate == RDF_MS_BASE+'rest':                
            if stmt.object == RDF_MS_BASE+'nil':
                tails.append(stmt.subject)
            else:
                listLinks[stmt.object] = stmt.subject
        elif stmt.predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
            containerItems[(stmt.subject, int(stmt.predicate[len(RDF_MS_BASE)+1:]) )] = stmt
        else:
            subject = rdfDom.findSubject(stmt.subject)
            if not subject:
                subject = rdfDom.addResource(stmt.subject)
                assert rdfDom.findSubject(stmt.subject)
            newNode = subject.addStatement(stmt)
            if newNode:
                newNodes.append(newNode)

    #for each list encountered
    for tail in tails:            
        orderedItems = [ listItems[tail] ]            
        #build the list from last to first
        head = tail
        while listLinks.has_key(head):
            head = listLinks[head]
            orderedItems.append(listItems[head])            
        orderedItems.reverse()
        for stmt in orderedItems:
            listid = stmt.subject
            stmt = Statement(head, *stmt[1:]) #set the subject to be the head of the list
            subject = rdfDom.findSubject(stmt.subject) or rdfDom.addResource(stmt.subject)
            newNode = subject.addStatement(stmt, listid)
            if newNode:
                newNodes.append(newNode)
        
    #now add any container statements in the correct order
    containerKeys = containerItems.keys()
    containerKeys.sort()
    for key in containerKeys:
        stmt = containerItems[key]
        listid = stmt.predicate
        head = stmt.subject
        #change the predicate 
        stmt = Statement(stmt[0], RDF_SCHEMA_BASE+u'member', *stmt[2:]) 
        subject = rdfDom.findSubject(stmt.subject) or rdfDom.addResource(stmt.subject)
        newNode = subject.addStatement(stmt, listid)
        if newNode:
            newNodes.append(newNodes)
    return newNodes

        

def diffResources(sourceDom, resourceNodes):
    ''' Given a list of Subject nodes from another RxPath DOM, compare
    them with the resources in the source DOM. This assumes that the
    each Subject node contains all the statements it is the subject
    of.

    Returns the tuple (Subject or Predicate nodes to add list, Subject
    or Predicate nodes to remove list, Re-ordered resource dictionary)
    where Reordered is a dictionary whose keys are RDF list or
    collection Subject nodes from the source DOM that have been
    modified or reordered and whose values is the tuple (added node
    list, removed node list) containing the list item Predicates nodes
    added or removed.  Note that a compoundresource could be
    re-ordered but have no added or removed items and so the lists
    will be empty.
    
    This diff routine punts on blank node equivalency; this means bNode
    labels must match for the statements to match. The exception is
    RDF lists and containers -- in this case the bNode label or exact
    ordinal value of the "rdf:_n" property is ignored -- only the
    order is compared.  '''
    removals = []
    additions = []
    reordered = {}
    for resourceNode in resourceNodes:
        currentNode = sourceDom.findSubject(resourceNode.uri)
        if currentNode: 
            isCompound = currentNode.isCompound()
            isNewCompound = resourceNode.isCompound()
            if isNewCompound != isCompound:
                #one's a compound resource and the other isn't
                #(or they're different types of compound resource)
                if isNewCompound and isCompound and isCompound != \
                    RDF_MS_BASE + 'List' and isNewCompound != RDF_MS_BASE + 'List':
                    #we're switching from one type of container (Seq, Alt, Bag) to another
                    #so we just need to add and remove the type statements -- that will happen below
                    diffResource = True
                else:
                    #remove the previous resource
                    removals.append(currentNode) 
                    #and add the resource's current statements
                    #we add all the its predicates instead just adding the
                    #resource node because it isn't a new resource
                    for predicate in resourceNode.childNodes:
                        additions.append( predicate) 
                    
                    diffResource = False
            else:
                diffResource = True
        else:
            diffResource = False
            
        if diffResource:
            def update(currentChildren, resourceChildren, added, removed):
                changed = False
                for tag, alo, ahi, blo, bhi in opcodes:#to turn a into b
                    if tag in ['replace', 'delete']:
                        changed = True
                        for currentPredicate in currentChildren[alo:ahi]:
                            #if we're a list check that the item hasn't just been reordered, not removed
                            if not isCompound or \
                                toListItem(currentPredicate) not in resourceNodeObjects:
                                    removed.append( currentPredicate)
                    if tag in ['replace','insert']:
                        changed = True
                        for newPredicate in resourceChildren[blo:bhi]:
                            #if we're a list check that the item hasn't just been reordered, not removed
                            if not isCompound or \
                                toListItem(newPredicate) not in currentNodeObjects:                            
                                    added.append( newPredicate )                    
                    #the only other valid value for tag is 'equal'
                return changed
            
            if isCompound:
                #to handle non-membership statements we split the childNode lists
                #and handle each separately (we can do that the RxPath spec says all non-membership statements will come first)
                i = 0
                for p in currentNode.childNodes:
                    if p.stmt.predicate in [RDF_MS_BASE + 'first', RDF_SCHEMA_BASE + 'member']:
                        break
                    i+=1
                currentChildren = currentNode.childNodes[:i]
                j = 0
                for p in resourceNode.childNodes:
                    if p.stmt.predicate in [RDF_MS_BASE + 'first', RDF_SCHEMA_BASE + 'member']:
                        break
                    j+=1                
                resourceChildren = resourceNode.childNodes[:j]
                
                #if it's a list or collection we just care about the order, ignore the predicates
                import difflib
                toListItem = lambda x: (x.stmt.objectType, x.stmt.object)
                currentListNodes = currentNode.childNodes[i:]
                currentNodeObjects = map(toListItem, currentListNodes)
                resourceListNodes = resourceNode.childNodes[j:]
                resourceNodeObjects = map(toListItem, resourceListNodes)
                opcodes = difflib.SequenceMatcher(None, currentNodeObjects,
                                           resourceNodeObjects).get_opcodes()
                if opcodes:                    
                    currentAdded = []
                    currentRemoved = []
                    #if the list has changed
                    if update(currentListNodes, resourceListNodes, currentAdded, currentRemoved):
                           reordered[ currentNode ] = ( currentAdded, currentRemoved )
            else:
                currentChildren = currentNode.childNodes
                resourceChildren = resourceNode.childNodes
                
            opcodes = utils.diffSortedList(currentChildren,resourceChildren,
                    lambda a,b: cmp(a.stmt, b.stmt) )
            update(currentChildren,resourceChildren,additions, removals)
        else: #new resource (add all the statements)
            additions.append(resourceNode)
    return additions, removals, reordered

def mergeDOM(sourceDom, updateDOM, resources, authorize=None):
    '''
    Performs a 2-way merge of the updateDOM into the sourceDom.
    
    Resources is a list of resource URIs originally contained in
    update DOM before it was edited. If present, this list is
    used to create a diff between those resources statements in
    the source DOM and the statements in the update DOM.

    All other statements in the update DOM are added to the source
    DOM. (Conflicting bNode labels are not re-labeled as we assume
    update DOM was orginally derived from the sourceDOM.)

    This doesn't modify the source DOM, instead it returns a pair
    of lists (Statements to add, nodes to remove) that can be used to
    update the DOM, e.g.:
    
    >>> statements, nodesToRemove = mergeDOM(sourceDom, updateDom ,resources)
    >>> for node in nodesToRemove:
    >>>    node.parentNode.removeChild(node)
    >>> addStatements(sourceDom, statements)    
    '''
    #for each of these resources compare its statements in the update dom
    #with those in the source dom and add or remove the differences    
    newNodes = []
    removeResources = []            
    resourcesToDiff = []
    for resUri in resources:
        assert isinstance(resUri, (unicode, str))
        resNode = updateDOM.findSubject(resUri)
        if resNode: #do a diff on this resource
            #print 'diff', resUri
            resourcesToDiff.append(resNode)
        else:#a resource no longer in the rxml has all their statements removed
            removeNode = sourceDom.findSubject(resUri)
            if removeNode:
                removeResources.append(removeNode)
                        
    for resNode in updateDOM.childNodes:
        #resources in the dom but not in resources just have their statements added
        if resNode.uri not in resources:                    
            if resNode.isCompound():
                #if the node is a list or container we want to compare with the list in model
                #because just adding its statements with existing list statements would mess things up
                #note: thus we must assume the list in the updateDOM is complete
                #print 'list to diff', resNode
                resourcesToDiff.append(resNode)
            else:
                sourceResNode  = sourceDom.findSubject(resNode.uri)
                if sourceResNode:
                    #not a new resource: add each statement that doesn't already exist in the sourceDOM
                    for p in resNode.childNodes:
                        if not sourceResNode.findPredicate(p.stmt):
                            newNodes.append(p)                    
                else:
                    #new resource: add the subject node
                    newNodes.append(resNode)
        else:
            assert resNode in resourcesToDiff #resource in the list will have been added above
                       
    additions, removals, reordered = diffResources(sourceDom, resourcesToDiff)

    newNodes.extend( additions )
    removeResources.extend( removals)
    if authorize:
        authorize(newNodes, removeResources, reordered)

    newStatements = reduce(lambda l, n: l.extend(n.getModelStatements()) or l, newNodes, [])
    
    #for modified lists we just remove the all container and collection resource
    #and add all its statements    
    for compoundResource in reordered.keys():
        removeResources.append(compoundResource)
        newCompoundResource = updateDOM.findSubject( compoundResource.uri)
        assert newCompoundResource
        newStatements = reduce(lambda l, p: l.extend(p.getModelStatements()) or l,
                        newCompoundResource.childNodes, newStatements)
    return newStatements, removeResources

def addDOM(sourceDom, updateDOM, authorize=None):
    ''' Add the all statements in the update RxPath DOM to the source
    RxPathDOM. If the updateDOM contains RDF lists or containers that
    already exist in sourceDOM they are replaced instead of added
    (because just adding the statements could form malformed lists or
    containers). bNode labels are renamed if they are used in the
    existing model. If you don't want to relabel conflicting bNodes
    use mergeDOM with an empty resource list.

    This doesn't modify the source DOM, instead it returns a pair
    of lists (Statements to add, nodes to remove) that can be used to
    update the DOM in the same manner as mergeDOM.
    '''
    stmts = updateDOM.model.getStatements()    
    bNodes = {}
    replacingListResources = []
    for stmt in stmts:                
        def updateStatement(attrName):
            '''if the bNode label is used in the sourceDom choose a new bNode label'''
            uri = getattr(stmt, attrName)
            if uri.startswith(BNODE_BASE):
                if bNodes.has_key(uri):
                    newbNode = bNodes[uri]
                    if newbNode:
                        setattr(stmt, attrName, newbNode)
                else: #encountered for the first time
                    if sourceDom.findSubject(uri):
                        #generate a new bNode label
                           
                        #todo: this check doesn't handle detect inner list bnodes
                        #most of the time this is ok because the whole list will get removed below
                        #but if the bNode is used by a inner list we're not removing this will lead to a bug
                        
                        #label used in the model, so we need to rename this bNode
                        newbNode = generateBnode()
                        bNodes[uri] = newbNode
                        setattr(stmt, attrName, newbNode)
                    else:
                        bNodes[uri] = None
            else:                
                if not bNodes.has_key(uri):                    
                    resNode = updateDOM.findSubject(uri)
                    if resNode and resNode.isCompound() and sourceDom.findSubject(uri):
                        #if the list or container resource appears in the source DOM we need to compare it
                        #because just adding its statements with existing list statements would mess things up
                        #note: thus we must assume the list in the updateDom is complete                         
                        replacingListResources.append(resNode)
                    bNodes[uri] = None
        updateStatement('subject')        
        if stmt.objectType == OBJECT_TYPE_RESOURCE:
            updateStatement('object')
    #todo: the updateDOM will still have the old bnode labels, which may mess up authorization
            
    additions, removals, reordered = diffResources(sourceDom,replacingListResources)
    assert [getattr(x, 'stmt') for x in additions] or not len(additions) #should be all predicate nodes

    #now filter out any statements that already exist in the source dom:
    #(note: won't match statements with bNodes since they have been renamed
    alreadyExists = []
    newStmts = []
    for stmt in stmts:
        resNode = sourceDom.findSubject(stmt.subject)
        if resNode and resNode.findPredicate(stmt):
            alreadyExists.append(stmt)
        else:
            newStmts.append(stmt)
    
    if authorize:
        #get all the predicates in the updatedom except the ones in replacingListResources        
        newResources = [x for x in updateDOM.childNodes if x not in replacingListResources]
        newPredicates = reduce(lambda l, s: l.extend(
            [p for p in s.childNodes if p.stmt not in alreadyExists]) or l,
                                       newResources, additions)
        authorize(newPredicates, removals, reordered)
        
    #return statements to add, list resource nodes to remove
    #note: additions should contained by stmts, so we don't need to return them
    return newStmts, reordered.keys() 

def stmt2json(stmt):
    '''
    { "id" : "http://uri", "predicate" : "json-encoded literal" }
    '''
    uri = stmt[0]
    if uri.startswith(BNODE_BASE):
        uri = '_:' + uri[BNODE_BASE_LEN:]
        
    return json.dumps({ "id" : uri, stmt[1] : _encodeStmtObject(stmt) })

try:
    from hashlib import md5 # python 2.5 or greater
except ImportError:
    from md5 import new as md5

class Graph(object):
    """
    based on:

    Vanilla RDF Graph Isomorphism Tester
    Author: Sean B. Palmer, inamidst.com
    Uses the pyrple algorithm
    Usage: ./rdfdiff-vanilla.py <ntriplesP> <ntriplesQ>
    Requirements:
       Python2.3+
       http://inamidst.com/proj/rdf/ntriples.py
    References:
       http://inamidst.com/proj/rdf/rdfdiff.py
       http://miscoranda.com/comments/129#id2005010405560004
    """

    def __init__(self, stmts, quad=True):
      self.statements = set()
      self.cache = {}
      for stmt in stmts:
        if stmt[3] == OBJECT_TYPE_RESOURCE:
            obj = stmt[2]
        else:
            obj = '"'+ stmt[2]+'"'+stmt[3]
        if quad:
            scope = str(stmt[4])
        else:
            scope = None
        t = (str(stmt[0]), str(stmt[1]), str(obj), scope)
        self.statements.add(t)

    def isBnode(self, uri):
      isbnode = uri.startswith(BNODE_BASE) or uri.startswith('_:')
      return isbnode

    def _hashtuple(self):
      result = []
      for (subj, pred, objt, scope) in self.statements:
         if self.isBnode(subj):
            tripleHash = md5(str(self.vhashmemo(subj)))
         else: tripleHash = md5(subj)

         for term in (pred, objt, scope):
            if term is None:
                continue
            if self.isBnode(term):
               tripleHash.update(str(self.vhashmemo(term)))
            else: tripleHash.update(term)

         result.append(tripleHash.digest())
      result.sort()
      return result
      
    def __hash__(self):
      result = self._hashtuple()
      return hash(tuple(result))

    def vhashmemo(self, term, done=False):
      if self.cache.has_key((term, done)):
         return self.cache[(term, done)]

      result = self.vhash(term, done=done)
      self.cache[(term, done)] = result
      return result

    def vhash(self, term, done=False):
      result = []
      for stmt in self.statements:
         if term in stmt:
            for pos in xrange(4):
               if stmt[pos] is None:
                   continue
               if not self.isBnode(stmt[pos]):
                  result.append(stmt[pos])
               elif done or (stmt[pos] == term):
                  result.append(pos)
               else: result.append(self.vhash(stmt[pos], done=True))
      result.sort()
      return tuple(result)

    def normalizeBNodes(self):
        for t in self.statements:
            sub = t[0]
            if self.isBnode(sub):
                sub = self.vhashmemo(sub)
            pred = t[1]
            if self.isBnode(pred):
                pred = self.vhashmemo(pred)
            obj = t[2]
            if self.isBnode(obj):
                obj = self.vhashmemo(obj)
            scope = t[3]
            if scope is None:
                yield (sub, pred, obj)
            else:
                if self.isBnode(scope):
                    scope = self.vhashmemo(scope)
                yield (sub, pred, obj, scope)

def graph_compare(p, q, asQuads=True):
    '''
    Return True if the given collections of statements are graph-isomorphic.
    '''
    return hash(Graph(p, asQuads)) == hash(Graph(q, asQuads))
