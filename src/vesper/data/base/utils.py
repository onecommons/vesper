#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
    Various utility functions and classes for vesper
'''
import re, random, urllib2
try:
    import cStringIO
    StringIO = cStringIO
except ImportError:
    import StringIO

from vesper import utils
from vesper.backports import *
import bisect
from xml.dom import HierarchyRequestErr

EMPTY_NAMESPACE = None
EMPTY_PREFIX = None
XMLNS_NAMESPACE = u"http://www.w3.org/2000/xmlns/"
XML_NAMESPACE = u"http://www.w3.org/XML/1998/namespace"
XHTML_NAMESPACE = u"http://www.w3.org/1999/xhtml"

def SplitQName(qname):
    l = qname.split(':',1)
    if len(l) < 2:
        return None, l[0]
    return tuple(l)

def GenerateUuid():
    return random.getrandbits(16*8) #>= 2.4

def UuidAsString(uuid):
    """
    Formats a long int representing a UUID as a UUID string:
    32 hex digits in hyphenated groups of 8-4-4-4-12.
    """   
    s = '%032x' % uuid
    return '%s-%s-%s-%s-%s' % (s[0:8],s[8:12],s[12:16],s[16:20],s[20:])

def CompareUuids(u1, u2):
    """Compares, as with cmp(), two UUID strings case-insensitively"""
    return cmp(u1.upper(), u2.upper())

OBJECT_TYPE_RESOURCE = "R"
OBJECT_TYPE_LITERAL = "L"

BNODE_BASE = 'bnode:'
BNODE_BASE_LEN = len('bnode:')

def isBnode(uri):
  isbnode = uri.startswith(BNODE_BASE) or uri.startswith('_:')
  return isbnode

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

class ResourceUri(object):
    '''
    Marker class
    '''
    @staticmethod
    def new(s):
        if isinstance(s, unicode):
            return ResourceUriU(s)
        else:
            return ResourceUriS(s)

class ResourceUriS(str, ResourceUri):
    __slots__ = ()

class ResourceUriU(unicode, ResourceUri):
    __slots__ = ()

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
    for stmt in parseTriples(
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
                                 
def _parseRDFJSON(jsonstring, defaultScope=None):
    '''
    Parses JSON that follows this format:

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
    
def encodeStmtObject(object, type, iscompound=False, scope=None, valueName='value'):    
    '''
    return a string encoding the object in one of these ways: 
    (from http://www.w3.org/TR/2006/NOTE-rdf-sparql-json-res-20061004/)
    {"type":"literal", ["xml:lang":" L ",] "value":" S"},
    {"type":"typed-literal", "datatype":" D ", "value":" S "},
    {"type":"uri|bnode", "value":"U"", "hint":"compound"} ] },
    '''
    jsonobj = {}    
    if type == OBJECT_TYPE_RESOURCE:
        if isinstance(object, (str,unicode)) and object.startswith(BNODE_BASE):
            bnode = object[BNODE_BASE_LEN:]
            jsonobj['type'] = 'bnode'
            jsonobj[valueName] = bnode
        else:
            jsonobj['type'] = 'uri'
            jsonobj[valueName] = object
        
        if iscompound:
            jsonobj['hint'] = 'compound'
    else:        
        if type.find(':') > -1:
            jsonobj['type'] = "typed-literal"
            jsonobj["datatype"] = type
            jsonobj[valueName] = object
        else:
            jsonobj['type'] = "literal"            
            if type != OBJECT_TYPE_LITERAL:
                #datatype is the lang
                jsonobj["xml:lang"] = type                    
            jsonobj[valueName] = object
    if scope is not None:
        jsonobj['context'] = scope
    return jsonobj

def parseRDFFromString(contents, baseuri, type='unknown', scope=None,
                       options=None, getType=False):
    '''
    returns an iterator of Statements
    
    type can be anyone of the following:
        "rdfxml", "ntriples", "pjson", "yaml", and "unknown"
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
    from vesper.data import base
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
                #otherwise assume pjson
                type='pjson' 
                break
            elif isinstance(contents, dict):
                type='pjson' #assume pjson
                break
            
            startcontents = contents[:256].lstrip()
            if not startcontents: #empty
                return [], 'statements'
                
            if startcontents[0] in '{[':
                type='pjson' #assume pjson
                break
            else:
                from vesper import multipartjson
                if multipartjson.looks_like_multipartjson(startcontents):
                    type = 'mjson'
                    break
            try:
                from vesper.utils import htmlfilter
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
        elif type == 'rss-tag-soup':
            try: #only redland's parser supports these
                import RDF
                parser=RDF.Parser(type)
                stream = parser.parse_string_as_stream(contents, baseuri)
                return base.redland2Statements(stream, scope), type 
            except ImportError:
                raise ParseException("RDF parse error: "+ type+
                    " is only supported by Redland, which isn't installed")
        elif type == 'rdfxml' or type == 'turtle':
            try:
                #if rdflib is installed, use its RDF/XML parser because it doesn't suck            
                import rdflib
                from vesper.data.store.rdflib_store import rdflib2Statements                
                ts = rdflib.ConjunctiveGraph()                
                import StringIO as pyStringIO #cStringIO can't have custom attributes
                contentsStream = pyStringIO.StringIO(contents)
                #xml.sax.expatreader.ExpatParser.parse() calls
                #xml.sax.saxutils.prepare_input_source which checks for 
                #a 'name' attribute to set the InputSource's systemId
                contentsStream.name = baseuri 
                ts.parse(contentsStream, preserve_bnode_ids=True)
                return rdflib2Statements(
                  (s,p,o, scope) for (s,p,o) in ts.triples((None, None, None))
                ), type
            except ImportError:
                try: #try redland's parser
                    import RDF
                    parser=RDF.Parser(type)
                    stream = parser.parse_string_as_stream(contents, baseuri)
                    return base.redland2Statements(stream, scope), type 
                except ImportError:
                    #fallback to 4Suite's obsolete parser
                    try:
                        from Ft.Rdf import Util
                        from vesper.data.store import ft
                        model, db=Util.DeserializeFromString(contents,scope=baseuri)
                        statements = ft.Ft2Statements(model.statements())
                        #we needed to set the scope to baseuri for the parser to
                        #resolve relative URLs, so we now need to reset the scope
                        for s in statements:
                            s.scope = scope
                        return statements, type
                    except ImportError:
                        raise ParseException("no RDF/XML parser installed")
        elif type == 'json':            
            return _parseRDFJSON(contents, scope), type
        elif type == 'pjson' or type == 'yaml' or type == 'mjson':
            from vesper import pjson
            if isinstance(contents, str):
                if type == 'yaml':
                    import yaml
                    contents = yaml.safe_load(contents)
                elif type == 'mjson':
                    from vesper import multipartjson
                    content = multipartjson.loads(contents, False)
                else:
                    contents = json.loads(contents)    

            options['scope'] = scope
            #XXX generateBnode doesn't detect collisions, maybe gen UUID instead            
            if 'generateBnode' not in options:
                options['generateBnode']='uuid'            
            if 'useDefaultRefPattern' not in options:
                options['useDefaultRefPattern']=False
            if not contents:
                return [], type
            stmts = pjson.tostatements(contents, **options), type
            return stmts
        else:
            raise ParseException('unsupported type: ' + type)
    except:
        #XXX why isn't this working?:
        #raise ParseException("error parsing "+type)
        raise

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

    stream = urllib2.urlopen(uri)
    contents = stream.read()
    stream.close()
    return parseRDFFromString(contents.decode('utf8'), modelbaseuri, type, scope, options, getType)

def serializeRDF(statements, type, uri2prefixMap=None, options=None):
    stringIO = StringIO.StringIO()
    serializeRDF_Stream(statements, stringIO, type, uri2prefixMap, options)
    return stringIO.getvalue()
    
def serializeRDF_Stream(statements,stream,type,uri2prefixMap=None,options=None):
    '''    
    type can be one of the following:
        "rdfxml", "ntriples", "ntjson", "yaml", "mjson", or "pjson"
    '''
    from vesper.data import base
    if type.startswith('http://rx4rdf.sf.net/ns/wiki#rdfformat-'):
        type = type.split('-', 1)[1]

    if type == 'rdfxml':
        try:
            #if rdflib is installed            
            import rdflib
            from vesper.data.store.rdflib_store import statement2rdflib
            graph = rdflib.ConjunctiveGraph()
            if uri2prefixMap:
                for url, prefix in uri2prefixMap.items():
                    graph.store.bind(prefix, url)
            for stmt in statements:
                s, p, o, c = statement2rdflib(stmt)
                graph.store.add((s, p, o), context=c, quoted=False)                    
             
            graph.serialize(stream, format="pretty-xml", max_depth=3)
        except ImportError:
            try: #try redland's parser
                import RDF
                serializer = RDF.RDFXMLSerializer()
                model = RDF.Model()
                for stmt in statements:
                   model.add_statement(base.statement2Redland(stmt))
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
                    from rx import base
                    for stmt in statements:
                        model.add(base.statement2Ft(stmt) )  
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
    elif type == 'pjson' or type == 'mjson':
        isMjson = type == 'mjson'
        from vesper import pjson, multipartjson
        #XXX use uri2prefixMap
        options = options or {}
        objs = pjson.tojson(statements, preserveTypeInfo=True, asList=isMjson)
        if isMjson:
            return multipartjson.dump(objs, stream, **options)
        else:
            return json.dump(objs, stream, **options)
    elif type == 'yaml':
        import yaml
        from vesper import pjson
        #for yaml options see http://pyyaml.org/wiki/PyYAMLDocumentation#Theyamlpackage
        #also note default_style=" ' | >  for escaped " unescaped ' literal | folded >        
        #use default_flow_style: True always flow (json-style output), False always block    
        # default_flow_style=None block if nested collections other flow
        defaultoptions = dict(default_style="'")
        if options: defaultoptions.update(options)
        return yaml.safe_dump( pjson.tojson(statements, preserveTypeInfo=True), stream, **defaultoptions)

def canWriteFormat(format):
    if format in ('ntriples', 'ntjson', 'json', 'pjson', 'mjson'):
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
class _Removed(object): 
    def __repr__(self): return 'Removed' 
Removed = _Removed()
Comment = object()
def parseTriples(lines, bNodeToURI = lambda x: x, charencoding='utf8',
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

def peekpair(seq):
    '''
    yield next, peek
    '''
    iter_ = iter(seq)
    curr = iter_.next()
    while 1:
      try:
          next = iter_.next()
          yield curr, next
      except StopIteration: 
          yield curr, None
          return
      curr = next

class OrderedModel(object):
    '''
    Sort statements and normalize RDF lists and containers
    '''

    def __init__(self, stmts):
        children = []
        subjectDict = {}
        lists = {}
        listNodes = {}

        for stmt in stmts:
            if (stmt.predicate in (RDF_MS_BASE+'first', RDF_MS_BASE+'rest')
                or (stmt.object == RDF_MS_BASE+'List' and stmt.predicate ==
                RDF_MS_BASE+'type')): #its a list

                if stmt.subject not in lists:
                    lists[stmt.subject] = 1
                if stmt.predicate == RDF_MS_BASE+'rest':
                    lists[stmt.object] = 0 #the object is not at the head of the list

                listNodes.setdefault(stmt.subject, []).append(stmt)
            else:
                if stmt.subject in subjectDict:
                    subjectDict[stmt.subject].append(stmt)
                else:
                    subjectDict[stmt.subject] = [stmt]
                    if children and children[-1] > stmt.subject:                        
                        bisect.insort(children, stmt.subject)
                    else:
                        children.append(stmt.subject)

        for uri, head in lists.items():            
            if head: 
                #don't include non-head list-nodes as resources
                #but add the statements from the tail nodes
                bisect.insort(children, uri)                
                subjectDict.setdefault(uri, []).extend( listNodes[uri] )

        self.resources, self.subjectDict, self.listNodes = children, subjectDict, listNodes

    def groupbyProp(self):
        for res in self.resources:
            stmts = self.getProperties(res)
            values = []
            for stmt, next in peekpair(stmts):
                values.append( (stmt.object, stmt.objectType) )
                if not next or next.predicate != stmt.predicate:
                    yield res, stmt.predicate, values
                    values = []        

    def _addListItem(self, children, listID, head):
        stmts = self.listNodes[listID] #model.getStatements(listID)

        nextList = None
        for stmt in stmts:                      
            if stmt.predicate == RDF_MS_BASE+'first':
                #change the subject to the head of the list
                stmt = Statement(head, *stmt[1:]) 
                children.append(stmt) #(stmt, listID) )
            elif stmt.predicate == RDF_MS_BASE+'rest':
                if stmt.object != RDF_MS_BASE+'nil':
                    nextList = stmt.object
                if nextList == listID:
                    raise  HierarchyRequestErr('model error -- circular list resource: %s' % str(listID))
            elif stmt.predicate != RDF_MS_BASE+'type':  #rdf:type statement ok, assumes its rdf:List
                raise  HierarchyRequestErr('model error -- unexpected triple for inner list resource')
        return nextList

    def getProperties(self, uri, useRdfsMember=True):
        '''        
        Statements are sorted by (predicate uri, object value) unless they are RDF list or containers.
        If the RDF list or container has non-membership statements (usually just rdf:type) those will appear first.
        '''
        stmts = self.subjectDict[uri]
        stmts.sort()

        children = []
        containerItems = []

        listItem = nextList = None        
        for stmt in stmts:
            assert stmt.subject == uri, uri + '!=' + stmt.subject
            if stmt.predicate == RDF_MS_BASE+'first':
                listItem = stmt
            elif stmt.predicate == RDF_MS_BASE+'rest':
                if stmt.object != RDF_MS_BASE+'nil':
                    nextList = stmt.object                    
            elif stmt.predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
                ordinal = int(stmt.predicate[len(RDF_MS_BASE+'_'):])
                containerItems.append((ordinal, stmt))
            elif not (stmt.predicate == RDF_MS_BASE+u'type' and 
                       stmt.object == RDF_SCHEMA_BASE+u'Resource'):
                #don't include the redundent rdf:type rdfs:Resource statement
                children.append(stmt)# (stmt, None) )

        if listItem:
            children.append(listItem)#(listItem, None))
            while nextList:
                nextList = _addListItem(children, nextList, listItem.subject)

        #add any container items in order, setting rdf:member instead of rdf:_n
        #ordinals = containerItems.keys()
        containerItems.sort()        
        for ordinal, stmt in containerItems:            
            if useRdfsMember:
                #realPredicate = stmt.predicate
                stmt = Statement(stmt[0], RDF_SCHEMA_BASE+u'member', *stmt[2:])
            children.append(stmt) #(stmt, realPredicate) )

        return children

    # def getModelStatements(self, stmt, listID, prevListID, next):
    #     '''
    #     Returns a list of statements this Predicate Element represents
    #     as they appear in model. This will be different from the stmt
    #     property in the case of RDF lists and container predicates.
    #     Usually one statement but may be up to three if the Predicate
    #     node is a list item.
    #     '''
    #     if listID:
    #         stmt = Statement(*stmt)
    #         if stmt.predicate == RDF_SCHEMA_BASE+'member':
    #             #replace predicate with listID
    #             stmt = base.Statement(stmt[0], listID, *stmt[2:])
    #             return (stmt,)
    #         else:
    #             #replace subject with listID
    #             stmt = base.Statement(listID, *stmt[1:])                
    #             listStmts = [ stmt ]
    #             if prevListID:
    #                 listStmts.append( base.Statement(
    #                 prevListID, RDF_MS_BASE+'rest', listID,
    #                 objectType=OBJECT_TYPE_RESOURCE, scope=stmt.scope))
    #             if not next:
    #                 listStmts.append( base.Statement( listID,
    #                     RDF_MS_BASE+'rest', RDF_MS_BASE+'nil',
    #                     objectType=OBJECT_TYPE_RESOURCE, scope=stmt.scope))
    #             return tuple(listStmts)
    #     else:
    #         return (stmt, )
    # 
    # def getStatementsFromResource(children):
    #     return reduce(lambda l, p: l.extend(p.getModelStatements()) or l, children, [])

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

    def _hashtuple(self):
      result = []
      for (subj, pred, objt, scope) in self.statements:
         if isBnode(subj):
            tripleHash = md5(str(self.vhashmemo(subj)))
         else: tripleHash = md5(subj)

         for term in (pred, objt, scope):
            if term is None:
                continue
            if isBnode(term):
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
               if not isBnode(stmt[pos]):
                  result.append(stmt[pos])
               elif done or (stmt[pos] == term):
                  result.append(pos)
               else: result.append(self.vhash(stmt[pos], done=True))
      result.sort()
      return tuple(result)

    def normalizeBNodes(self):
        for t in self.statements:
            sub = t[0]
            if isBnode(sub):
                sub = self.vhashmemo(sub)
            pred = t[1]
            if isBnode(pred):
                pred = self.vhashmemo(pred)
            obj = t[2]
            if isBnode(obj):
                obj = self.vhashmemo(obj)
            scope = t[3]
            if scope is None:
                yield (sub, pred, obj)
            else:
                if isBnode(scope):
                    scope = self.vhashmemo(scope)
                yield (sub, pred, obj, scope)

def graph_compare(p, q, asQuads=True):
    '''
    Return True if the given collections of statements are graph-isomorphic.
    '''
    return hash(Graph(p, asQuads)) == hash(Graph(q, asQuads))
