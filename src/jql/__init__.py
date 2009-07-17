from rx.RxPath import Tupleset, ColumnInfo, EMPTY_NAMESPACE

SUBJECT = 0
PROPERTY = 1
OBJECT = 2

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
    
    def __init__(self, initModel, ast, explain=False, debug=False, vars=None):
        self.initialModel = initModel
        self.currentTupleset = initModel        
        self.explain=explain
        self.ast = ast
        self.vars = vars
        self.debug=debug

    def __copy__(self):
        copy = QueryContext(self.initialModel, self.ast, self.explain, 
                                                        self.debug, self.vars)
        copy.currentTupleset = self.currentTupleset
        copy.currentValue = self.currentValue
        copy.currentRow = self.currentRow
        return copy

    def __repr__(self):
        return 'QueryContext' + repr([repr(r) for r in ['model',
            self.initialModel,'tupleset', self.currentTupleset, 'row',
                self.currentRow, 'value', self.currentValue]])
