#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
The Json Query Language lets query a variety of types of data sources and retrieve data as JSON. 

This tutorial.

First let's create a store with some JSON. For readability, we'll use native Python dictionaries and lists instead of long string of JSON.   

 >>> from vesper import app
 >>> datastore = app.createStore({
 ...    "foo" : "bar"
 ... })

The pjson module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default ``createStore`` will use a simple in-memory store.
 
Now we can start querying the database. Let's start with query that retrieves all records from the store: 

 >>> datastore.query('''
 ... { * }
 ... ''')
 {'errors': [], 'results': [{'foo': 'bar'}]}
 
Find all JSON objects. This is equivalent to the "SELECT * FROM table" SQL except that JQL has no notions of tables. If we wanted to select specified. 

 >>> datastore.query('''
 ... { foo }
 ... ''')
 {'errors': [], 'results': [{'foo': 'bar'}]}

This is equivalent to the SQL statement "SELECT foo FROM table".
Note that the objects that don't have foo and bar properties are not selected by the query. 
This is because the above query is shorthand for this query:

 >>> datastore.query('''
 ... { "foo" : foo,
 ... }
 ... ''')
 {'errors': [], 'results': [{'foo': 'bar'}]}

Including the `foo` properties names in the where clause only selects where the property exists. 

We could give the propery different names just as can "SELECT foo AS fob FROM table" in SQL.


"""
from vesper.backports import *
from vesper.data.base import Tupleset, ColumnInfo, EMPTY_NAMESPACE
from vesper import utils
import StringIO
        
SUBJECT = 0
PROPERTY = 1
OBJECT = 2
OBJTYPE_POS = 3
LIST_POS = 5

class ResourceSet(Tupleset):
    '''
    (resource uri, {varname : [values+]}),*
    or maybe: tuples, collabels = []
    '''

BooleanType = bool
ObjectType = object
NumberType = float
StringType = unicode

QueryOpTypes = ( Tupleset, ResourceSet, ObjectType, StringType, NumberType,
    BooleanType )
NullType = type(None)

class QueryException(Exception): pass

def runQuery(query, model):
    (ast, err) = buildAST(query)
    return evalAST(ast, model)

def getResults(query, model, bindvars=None, explain=None, debug=False,forUpdate=False, captureErrors=False):
    '''
    Returns a dict with the following keys:
        
    - `results`: the result of the query (either a list or None if the query failed)
    - `errors`: An error string if the query failed or None if it succeeded
    - `resource`: a list describing the resources (used for track changes)
    
    :Parameters:
     query
       the query
     model
       the store       
    '''
    #XXX this method still under construction
    response = utils.attrdict()
    errors = []
    
    (ast, parseErrors) = buildAST(query)
    errors.extend(parseErrors)
    
    response['results'] = []
    
    if explain:
        explain = StringIO.StringIO()
    
    if debug:
        debug = StringIO.StringIO()
    
    if ast != None:        
        try:
            results = list(evalAST(ast, model, bindvars, explain, debug, forUpdate))
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
    response['errors'] = errors
    if explain:
        response['explain'] = explain.getvalue()        

    if debug:
        response['debug'] = debug.getvalue()        
    
    return response

def buildAST(query):
    "parse a query, returning (ast, [error messages])"
    from vesper.query import parse, engine
    return parse.parse(query, engine.SimpleQueryEngine.queryFunctions)
    
def evalAST(ast, model, bindvars=None, explain=None, debug=False, forUpdate=False):
    #rewriteAST(ast)
    from vesper.query import engine
    queryContext = QueryContext(model, ast, explain, bindvars, debug, forUpdate=forUpdate)
    result = ast.evaluate(engine.SimpleQueryEngine(),queryContext)
    if explain:
        result.explain(explain)
    for row in result:
        yield row #row is list of joined statements

class QueryContext(object):
    currentRow = None
    currentValue = None
    shapes = { dict : utils.defaultattrdict }
    currentProjects = None
    projectValues = None
    
    def __init__(self, initModel, ast, explain=False, bindvars=None, debug=False, depth=0, forUpdate=False):
        self.initialModel = initModel
        self.currentTupleset = initModel        
        self.explain=explain
        self.ast = ast
        self.bindvars = bindvars or {}
        self.debug=debug
        self.depth=depth
        self.constructStack = []
        self.engine = None
        self.forUpdate = forUpdate

    def __copy__(self):
        copy = QueryContext(self.initialModel, self.ast, self.explain, self.bindvars,
                                              self.debug, self.depth, self.forUpdate)
        copy.currentTupleset = self.currentTupleset
        copy.currentValue = self.currentValue
        copy.currentRow = self.currentRow
        copy.constructStack = self.constructStack
        copy.engine = self.engine
        return copy

    def __repr__(self):
        return 'QueryContext' + repr([repr(r) for r in ['model',
            self.initialModel,'tupleset', self.currentTupleset, 'row',
                self.currentRow, 'value', self.currentValue]])
