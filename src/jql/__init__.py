"""
The Json Query Language lets query a variety of types of data sources and retrieve data as JSON. 

This tutorial.

First let's create a store with some JSON. For readability, we'll use native Python dictionaries and lists instead of long string of JSON.   

 >>> import raccoon
 >>> datastore = raccoon.createStore({
 ...    'foo' : 'bar'
 ... })

The sjson module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default ``createStore`` will use a simple in-memory store.
 
Now we can start querying the database. Let's start with query that retrieves all records from the store: 

 >>> datastore.query('''
 ... { * }
 ... ''',) 
 [{},{}]
 
Find all JSON object. This is equivalent to the "SELECT * FROM table" SQL except that JQL has no notions of tables. If we wanted to select specified. 

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


from rx.RxPath import Tupleset, ColumnInfo, EMPTY_NAMESPACE

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

try:
    from functools import partial
except ImportError:
    def partial(func, *args, **keywords):
            def newfunc(*fargs, **fkeywords):
                newkeywords = keywords.copy()
                newkeywords.update(fkeywords)
                return func(*(args + fargs), **newkeywords)
            newfunc.func = func
            newfunc.args = args
            newfunc.keywords = keywords
            return newfunc

def runQuery(query, model):
    ast = buildAST(query)
    return evalAST(ast, model)

def getResults(query, model, addChangeMap=False):
    '''
    Returns dictionary with the following keys:
        
    - `results`: the result of the query (either a list or None if the query failed)
    - `error`: An error string if the query failed or None if it succeeded
    - `resource`: a list describing the resources (used for track changes)
    
    :Parameters:
     query
       the query
     model
       the store       
    '''
    #XXX this method still under construction
    ast = buildAST(query)
    try:
        results = list(evalAST(ast, model))
        response = dict(results=results, error=None)
    except:
        error = 'error running query' #XXX
        response = dict(results=None, error=error)
    if addChangeMap:
        if not results:
            response['resources'] = []
        else:
            if not instance(results[0], dict):
                raise NotImplementedError('cant figure out addChangeMap')
            response['resources'] = [res['id'] for res in results]

    return response

def buildAST(query):
    from jql import parse
    return parse.parse(query)

def evalAST(ast, model, bindvars=(), explain=None, debug=False):
    #rewriteAST(ast)
    from jql import engine
    queryContext = QueryContext(model, ast, explain, debug)
    result = ast.evaluate(engine.SimpleQueryEngine(),queryContext)
    if explain:
        result.explain(explain)
    for row in result:
        yield row #row is list of joined statements

class QueryContext(object):
    currentRow = None
    currentValue = None
    
    def __init__(self, initModel, ast, explain=False, debug=False, depth=0, vars=None):
        self.initialModel = initModel
        self.currentTupleset = initModel        
        self.explain=explain
        self.ast = ast
        self.vars = vars
        self.debug=debug
        self.depth=depth
        self.constructStack = []

    def __copy__(self):
        copy = QueryContext(self.initialModel, self.ast, self.explain, 
                                              self.debug, self.depth, self.vars)
        copy.currentTupleset = self.currentTupleset
        copy.currentValue = self.currentValue
        copy.currentRow = self.currentRow
        copy.constructStack = self.constructStack
        return copy

    def __repr__(self):
        return 'QueryContext' + repr([repr(r) for r in ['model',
            self.initialModel,'tupleset', self.currentTupleset, 'row',
                self.currentRow, 'value', self.currentValue]])
