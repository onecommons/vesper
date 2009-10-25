"""
The Json Query Language lets query a variety of types of data sources and retrieve data as JSON. 

This tutorial.

First let's create a store with some JSON. For readability, we'll use native Python dictionaries and lists instead of long string of JSON.   

 >>> import raccoon
 >>> datastore = raccoon.createStore({
 ...    "foo" : "bar"
 ... })

The sjson module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default ``createStore`` will use a simple in-memory store.
 
Now we can start querying the database. Let's start with query that retrieves all records from the store: 

 >>> datastore.query('''
 ... { * }
 ... ''',)
 [{},{}]
 
Find all JSON objects. This is equivalent to the "SELECT * FROM table" SQL except that JQL has no notions of tables. If we wanted to select specified. 

 >>> datastore.query('''
 ... { foo, bar }
 ... ''', pretty=1) 

This is equivalent to the SQL statement "SELECT foo, bar FROM table".
Note that the objects that don't have foo and bar properties are not selected by the query. 
This is because the above query is shorthand for this query:

 >>> datastore.query('''
 ... { "foo" : foo,
 ...  "bar" : bar 
 ... }
 ... ''', pretty=1) 

Including the `foo` and `bar` properties names in the where clause only selects where the property exists. 

We could give the propery different names just as can "SELECT foo AS fob FROM table" in SQL.


"""


from rx.python_shim import *
from rx.RxPath import Tupleset, ColumnInfo, EMPTY_NAMESPACE
import rx.utils
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

def getResults(query, model, bindvars=None, explain=None, debug=False,addChangeMap=False):
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
    response = rx.utils.attrdict()
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
            results = list(evalAST(ast, model, bindvars, explain, debug))
            response['results'] = results
        except QueryException, qe:            
            errors.append('error: %s' % qe.message)
        except Exception, ex:
            import traceback
            errors.append("unexpected exception: %s" % traceback.format_exc())
        
    response['errors'] = errors
    if explain:
        response['explain'] = explain.getvalue()        

    if debug:
        response['debug'] = debug.getvalue()        
    
    # XXX may not be valid anymore
    """
    if addChangeMap:
        if not results:
            response['resources'] = []
        else:
            if not instance(results[0], dict):
                raise NotImplementedError('cant figure out addChangeMap')
            response['resources'] = [res['id'] for res in results]
    """
    return response

def buildAST(query):
    "parse a query, returning (ast, [error messages])"
    from jql import parse, engine
    return parse.parse(query, engine.SimpleQueryEngine.queryFunctions)
    
def evalAST(ast, model, bindvars=None, explain=None, debug=False):
    #rewriteAST(ast)
    from jql import engine
    queryContext = QueryContext(model, ast, explain, bindvars, debug)
    result = ast.evaluate(engine.SimpleQueryEngine(),queryContext)
    if explain:
        result.explain(explain)
    for row in result:
        yield row #row is list of joined statements

class QueryContext(object):
    currentRow = None
    currentValue = None
    shapes = { dict : rx.utils.defaultattrdict }
    
    def __init__(self, initModel, ast, explain=False, bindvars=None, debug=False, depth=0):
        self.initialModel = initModel
        self.currentTupleset = initModel        
        self.explain=explain
        self.ast = ast
        self.bindvars = bindvars or {}
        self.debug=debug
        self.depth=depth
        self.constructStack = []
        self.engine = None

    def __copy__(self):
        copy = QueryContext(self.initialModel, self.ast, self.explain, self.bindvars,
                                              self.debug, self.depth)
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
