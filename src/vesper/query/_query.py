#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
The Json Query Language lets query a variety of types of data sources and retrieve data as JSON. 

This tutorial.

First let's create a store with some JSON. For readability, we'll use native Python dictionaries and lists instead of long string of JSON.   

 >>> from vesper import app
 >>> datastore = app.createStore({
 ...    "id"  : 1,
 ...    "foo" : "bar"
 ... })

The pjson module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default ``createStore`` will use a simple in-memory store.
 
Now we can start querying the database. Let's start with query that retrieves all records from the store: 

 >>> datastore.query('''
 ... { * }
 ... ''')
 [{'foo': 'bar', 'id': '@1'}]
 
Find all JSON objects. This is equivalent to the "SELECT * FROM table" SQL except that JQL has no notions of tables. If we wanted to select specified. 

 >>> datastore.query('''
 ... { foo }
 ... ''')
 [{'foo': 'bar'}]

This is equivalent to the SQL statement "SELECT foo FROM table".
Note that the objects that don't have foo and bar properties are not selected by the query. 
This is because the above query is shorthand for this query:

 >>> datastore.query('''
 ... { "foo" : foo,
 ... }
 ... ''')
 [{'foo': 'bar'}]

Including the `foo` properties names in the where clause only selects where the property exists. 

We could give the propery different names just as can "SELECT foo AS fob FROM table" in SQL.


"""
from vesper.backports import *
from vesper.data.base import Tupleset, ColumnInfo, EMPTY_NAMESPACE, ResourceUri
from vesper import utils, pjson
import StringIO
import vesper.utils._utils
import time

SUBJECT = 0
PROPERTY = 1
OBJECT = 2
OBJTYPE_POS = 3
LIST_POS = 5

BooleanType = bool
ObjectType = object
NumberType = float
StringType = unicode

QueryOpTypes = ( Tupleset, ObjectType, StringType, NumberType,
    BooleanType )
NullType = type(None)

class QueryException(Exception):    

    def __init__(self, msg, op=None):
        Exception.__init__(self, msg)
        self.op = None

def runQuery(query, model):
    (ast, err) = buildAST(query)
    return evalAST(ast, model)

def getResults(query, model, bindvars=None, explain=None, debug=False,
    forUpdate=False, captureErrors=False, contextShapes=None, useSerializer=True,
    printast=False, queryCache=None):
    '''
    Returns a dict with the following keys:
        
    - `results`: the result of the query (either a list or None if the query failed)
    - `errors`: An error string if the query failed or an empty list if it succeeded.
    
    :Parameters:
     query
       the query
     model
       the store upon which to execute the query
    bindvars
       a dictionary used to resolve any `bindvars` in the query
    explain
       if True, the result will include a key named `explain` whose value is a 
       string "explaining" the query plan to execute it.
    debug
       if True, the result will include a very verbose trace of the query execution.
    forUpdate
       include in the query results enough information so that the response 
       objects can be modified and those changes saved back into the store.
    captureErrors
       if True, exceptions raise during query execution will be caught and appended 
       to the `errors` key. By default, such exceptions will be propagated when they occur.
    contextShapes
       A dictionary that specifies alternative constructors used when creating
       `dicts` and `lists` in the query results.
    useSerializer
       If value is a boolean, indicates whether pjson serialization is used or 
       not (default: True). If value is a dict it is passed as keyword arguments
       to the `pjson.Serializer` constructor.
    '''
    #XXX? add option to include `resources` in the result,
    # a list describing the resources (used for track changes)
    start = time.clock()
    response = utils.attrdict()
    errors = []
    
    (ast, parseErrors) = buildAST(query)
    errors.extend(parseErrors)
    
    response['results'] = []
    
    if explain:
        explain = StringIO.StringIO()

    if debug and not hasattr(debug, 'write'):
        debug = StringIO.StringIO()
    
    if ast != None:        
        try:
            results = list(evalAST(ast, model, bindvars, explain, debug, 
                    forUpdate, contextShapes, useSerializer, queryCache))
            #XXX: if forUpdate add a pjson header including namemap
            #this we have a enough info to reconstruct refs and datatypes without guessing
            #if forUpdate: 
            #   #need a context.datamap
            #   pjson.addHeader(context.datamap, response)
            response['results'] = results
        except QueryException, qe:
            if captureErrors:
                errors.append('error: %s' % qe.message)
            else:
                raise
        except Exception, ex:
            if captureErrors:
                import traceback
                errors.append("unexpected exception: %s" % traceback.format_exc())
            else:
                raise
    
    response['elapsed'] = time.clock() - start
    response['errors'] = errors
    if explain:
        response['explain'] = explain.getvalue()        

    if debug and hasattr(debug, 'getvalue'):
        response['debug'] = debug.getvalue()
    
    if printast:
        import pprint
        response['ast'] = pprint.pformat(ast)
    
    return response

def buildAST(query, namemap=None):
    "parse a query, returning (ast, [error messages])"
    from vesper.query import parse, engine
    return parse.parse(query, engine.SimpleQueryEngine.queryFunctions, False, namemap)

def _parsePjson(parseContext, v):
    #XXX handle pjson dicts
    if isinstance(v, (str, unicode)):
        ref = parseContext.lookslikeIdRef(v)
        if ref:
            return ResourceUri(ref)
    return v

def evalAST(ast, model, bindvars=None, explain=None, debug=False, 
    forUpdate=False, contextShapes=None, useSerializer=True, queryCache=None):
    from vesper.query import engine
    
    astNameMap = getattr(ast,'namemap', None)
    if isinstance(useSerializer, dict):        
        if astNameMap is not None:
            useSerializer['nameMap'] = astNameMap
        serializer = pjson.Serializer(**useSerializer)
    elif useSerializer:
        serializer = pjson.Serializer(astNameMap)
    else:
        serializer = None
    if bindvars:
        if serializer:
            parseContext = serializer.parseContext
        else:
            parseContext = pjson.ParseContext(astNameMap)
        if parseContext:
            for k, v in bindvars.items():
                if isinstance(v, (list, tuple)):
                    bindvars[k] = [_parsePjson(parseContext, i) for i in v]
                else:
                    bindvars[k] = _parsePjson(parseContext, v)

    queryContext = QueryContext(model, ast, explain, bindvars, debug, 
            forUpdate=forUpdate, shapes=contextShapes, 
            serializer=serializer, cache=queryCache)
    result = ast.evaluate(engine.SimpleQueryEngine(),queryContext)
    if explain:
        result.explain(explain)
    for row in result:
        yield row #row is list of joined statements

class QueryContext(object):
    currentRow = None
    currentValue = None
    defaultShapes = {}
    currentProjects = None
    projectValues = None
    finalizedAggs = False
    groupby = None
    complexPredicateHack = False
    
    def __init__(self, initModel, ast, explain=False, bindvars=None, debug=False,
            depth=0, forUpdate=False, shapes=None, serializer=None, cache=None):
        self.initialModel = initModel
        self.currentTupleset = initModel        
        self.explain=explain
        self.ast = ast
        self.bindvars = bindvars or {}
        self.debug=debug
        self.depth=depth
        self.forUpdate = forUpdate
        self.constructStack = []
        self.engine = None        
        self.accumulate = {}
        self.shapes = shapes or self.defaultShapes.copy()
        self.serializer = serializer
        if cache is None:            
            self.objCache = {}
        else:
            self.objCache = cache

    def __copy__(self):
        copy = QueryContext(self.initialModel,self.ast,self.explain,self.bindvars,
            self.debug, self.depth, self.forUpdate, self.shapes, 
            self.serializer, self.objCache)
        copy.currentTupleset = self.currentTupleset
        copy.currentValue = self.currentValue
        copy.currentRow = self.currentRow
        copy.constructStack = self.constructStack
        copy.engine = self.engine
        #don't copy other attributes
        return copy

    def __repr__(self):
        return 'QueryContext' + repr([repr(r) for r in ['model',
            self.initialModel,'tupleset', self.currentTupleset, 'row',
                self.currentRow, 'value', self.currentValue]])
