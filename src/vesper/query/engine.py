#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
jql query engine
================

Defines the evaluation engine, which, given an jql ast and a query context, 
returns a generator which yields the results of the query.
"""

import operator, copy, sys, pprint, itertools

from vesper.query import jqlAST
from vesper.data import base
from vesper.utils import flattenSeq, flatten, debugp
from vesper import pjson
from vesper.query import *
from vesper.query.operations import validateRowShape, SimpleTupleset, MutableTupleset, IterationJoin
from vesper.backports import product

#############################################################
####################  Grouping  #############################
#############################################################
import vesper.pjson

RDF_MS_BASE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
NilResource = ResourceUri(RDF_MS_BASE+'nil')

def getColumns(keypos, row, tableType=Tupleset, outerjoin=False, includekey=False):
    """
    Given a row which may contain nested tuplesets as cells and a "key" cell position,
    yield (key, row) pairs where the returned rows doesn't include the key cell.
    
    `keypos` is a tuple of offsets. If `keypos` is an offset into a nested table, 
    yield one row for each row in the nested table, with the cells from the "parent" row
    repeated with each row. For example:
    
    >>> t = ('a1', [('b1', 'c1'), ('b2', 'c2')] )
    >>> list( getColumns((1,1), t, list) ) #group by 'c'
    [('c1', ['a1', 'b1']), ('c2', ['a1', 'b2'])]
    
    >>> t = ('a1', [('b1', [('c1','d1')]), ('b2', [('c2','d1')])] )
    >>> list( getColumns((1,1,1), t, list) ) #group by 'd'
    [('d1', ['a1', 'b1', 'c1']), ('d1', ['a1', 'b2', 'c2'])]

    >>> list( getColumns((1,1), t, list) ) #group by 'c'
    [([('c1', 'd1')], ['a1', 'b1']), ([('c2', 'd1')], ['a1', 'b2'])]
    """
    keypos = list(keypos)
    pos = keypos.pop(0)
    cols = []
    for i, cell in enumerate(row):
        if i == pos:
            keycell = cell
            if includekey:
                cols.append(cell)
        else:
            cols.append(cell)
    assert outerjoin or keycell
    if not keypos:
        yield keycell, cols
    else: #i above keypos
        assert isinstance(keycell, tableType), "cell %s (%s) is not %s p %s restp %s" % (keycell, type(keycell), tableType, pos, keypos)
        if outerjoin and not keycell:            
            nullrow = getNullRows(keycell.columns[0])
            for key, nestedcols in getColumns(keypos, nullrow, tableType,includekey=includekey):
                yield key, cols+nestedcols                
        for nestedrow in keycell:
            for key, nestedcols in getColumns(keypos, nestedrow, tableType,includekey=includekey):
                yield key, cols+nestedcols

def getColumnsColumns(keypos, columns, includekey=False):
    '''
    Return a column list that corresponds to the shape of the rows returned 
    by the equivalent call to `getColumns`.
    '''
    keypos = list(keypos)
    pos = keypos.pop(0)
    cols = []
    for i, c in enumerate(columns):
        if pos == i:
            if includekey:
                cols.append(c)
            if keypos:
                assert isinstance(c.type, Tupleset)
                nestedcols = getColumnsColumns(keypos, c.type.columns, includekey)
                if nestedcols:
                    cols.extend(nestedcols)
        else:
            cols.append(c)
    return cols
            
def groupbyUnordered(tupleset, groupby, debug=False, outerjoin=False, includekey=False):
    '''
    Group the given tupleset by the column specified by groupby
    yields a row the groupby key and a nested tupleset containing the non-key columns
    of the tupleset. The nested tupleset
    will have one row for each occurence of the key. If the groupby key
    is nested, the columns of ancestor tuplesets will be duplicated for each key
    
    (a1, (b1, (c1,c2)), (a2, (b2, (c1,c2)) => groupby(c)
    => (c1, ( (a1, (b1)), (a2, (b2)) ) ), (c2, ( (a1, (b1)), (a2, (b2)) ) )

    (a1, b1), (a1, b2) => groupby(a) => (a1, (b1,b2))
                       => groupby(b) => (b1, (a1)), (b2, (a1))

    [a, b] => [a, Tupleset[b]] => [b, Tupleset[a]]

    (a1, b1, c1), (a1, b2, c1) => groupby(a) => (a1, ( (b1, c1), (b2, c2) ) )
                               => groupby(b) => (b1, (a1, (c1))), (b2, (a1,(c2)) )
                               => groupby(c) => (c1, (b1, (a1))), (c2, (b2, (a1)))

    [a, b, c] => [a, NestedRows[b,c]] => [b, [a, NestedRows[c]] ]

    (a1, b1, c1), (a1, b1, c1) => groupby(a) => (a1, ( (b1, c1), (b1, c1) ) )
                               => groupby(b) => (b1, (a1, (c1, c1)) )
                               => groupby(c) => (c1, (b1, (a1)))

    columns is a list of indexes into the rows
    (all of the source columns except for the group by)
        
    '''
    #>>> t = ('a1', [('b1', ('c1','c2'))], ('a2', [('b2', ('c1','c2'))] ))    
    #>>> list(groupbyUnordered([t], (1,1)))
    #[[('c1', 'c2'), MutableTupleset[['a1', ('a2', [('b2', ('c1', 'c2'))]), 'b1']]]]

    resources = {}
    for row in tupleset:
        #print 'gb', groupby, row
        if debug: validateRowShape(tupleset.columns, row)
        for key, outputrow in getColumns(groupby, row, outerjoin=outerjoin, includekey=includekey):
            if isinstance(key, list):
                debugp('####unhashable!', 'debug cols', debug, 'key',key,
                    'groupby', groupby, 'row', row, 'columns', tupleset.columns)
            vals = resources.get(key)
            if vals is None:
                vals = MutableTupleset()
                resources[key] = vals
            vals.append(outputrow)

    for key, values in resources.iteritems():
        if debug:
            #debug will be a columns list
            try:
                validateRowShape(debug, [key, values]) 
            except AssertionError:
                print 'source columns', tupleset.columns 
                raise        
        yield [key, values]

def getColumn(pos, row):
    '''
    yield a sequence of values for the nested column
    '''
    pos = list(pos)
    #print 'gc', pos, row
    p = pos.pop(0)    
    cell = row[p]
    if pos:
        assert isinstance(cell, Tupleset), "cell %s %s p %s restp %s" % (type(cell), cell, p, pos)
        for nestedrow in cell:
            assert isinstance(nestedrow, (list, tuple)
                ) and not isinstance(nestedrow, Tupleset), "%s" % (type(nestedrow))
            for (c, p, row) in getColumn(pos, nestedrow):
                #print 'ct', c
                yield (c, p, row)
    else:
        yield (cell, p, row)

def chooseColumns(groupby, columns, includekey=False):
    '''
    "above" columns go first, omit groupby column
    '''
    groupby = list(groupby)
    pos = groupby.pop(0)
    outputcolumns = []
    groupbycol = None
    for i, c in enumerate(columns):
        if pos == i:
            if includekey: outputcolumns.append(c)
            if groupby:
                groupbycol = chooseColumns(groupby, c.type.columns, includekey)
            #else: skip column
        else:
            outputcolumns.append(c)
    if groupbycol:
        outputcolumns.append(groupbycol) #goes last
    return MutableTupleset(columns=outputcolumns)

def groupbyOrdered(tupleset, groupby, debug=False):
    '''
    More efficient version of groupbyUnordered -- use if the tupleset is
    ordered by column in the given pos
    '''
    previous = None
    for row in tupleset:
        cols = []
        keycell = None
        for i, cell in enumerate(row):
            if i == groupby:
                keycell = cell
            else:
                cols.append(cell)
        assert keycell is not None
        if keycell != previous:
            yield [previous, vals]
            vals = MutableTupleset()
            previous = keycell
        vals.append(cols)
    if previous is not None:
        yield [previous, vals]

#############################################################
################ Query Functions ############################
#############################################################

def numericbinop(a, b, func):
    return func(safeFloat(a), safeFloat(b))

class QueryFuncs(object):

    SupportedFuncs = {}

    def addFunc(self, name, func, type=None, opFactory=None, cost=None, 
                            needsContext=False, lazy=False, checkForNulls=9999,
                            isAggregate=False, initialValue=None, finalFunc=None):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        if cost is None or callable(cost):
            costfunc = cost
        else:
            costfunc = lambda *args: cost
        self.SupportedFuncs[name] = jqlAST.QueryFuncMetadata(func, type, 
                    opFactory, costFunc=costfunc, needsContext=needsContext, 
                    lazy=lazy, checkForNulls=checkForNulls, isAggregate=isAggregate,
                    initialValue=initialValue, finalFunc=finalFunc)

    def getOp(self, name, *args, **kw):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        funcMetadata = self.SupportedFuncs.get(name)
        if not funcMetadata:
            raise QueryException('query function not defined: ' + str(name))
        funcMetadata = self.SupportedFuncs[name]
        
        op = funcMetadata.opFactory(name,funcMetadata,*args)
        if '__saveValue' in kw:
            op.saveValue = kw['__saveValue']
        return op
    
    def __init__(self):
        #add basic functions
        #number functions and operators
        self.addFunc('add', lambda a, b: numericbinop(a, b, operator.add), NumberType)
        self.addFunc('sub', lambda a, b: numericbinop(a, b, operator.sub), NumberType)
        self.addFunc('mul', lambda a, b: numericbinop(a, b, operator.mul), NumberType)
        self.addFunc('div', lambda a, b: numericbinop(a, b, operator.div), NumberType)
        self.addFunc('mod', lambda a, b: numericbinop(a, b, operator.mod), NumberType)
        self.addFunc('negate', lambda a: -safeFloat(a), NumberType)
        #cast functions
        self.addFunc('bool', bool, BooleanType)
        self.addFunc('number', safeFloat, NumberType)
        self.addFunc('string', str, StringType) #XXX: unicode?
        self.addFunc('ref', ResourceUri, ObjectType)
        #string functions
        self.addFunc('upper', lambda a: a.upper(), StringType)
        self.addFunc('lower', lambda a: a.lower(), StringType)
        self.addFunc('trim', lambda a,chars=None: a.strip(chars), StringType, checkForNulls=1)
        self.addFunc('ltrim', lambda a,chars=None: a.lstrip(chars), StringType, checkForNulls=1)
        self.addFunc('rtrim', lambda a,chars=None: a.rstrip(chars), StringType, checkForNulls=1)

def followFunc(reverse, context, startid, propname=None, excludeInitial=False,
                                                        edgesOnly=False):
    '''
    Starting with the given id, follow the given property
    '''
    #XXX add unittest for circularity
    if isinstance(startid, (list, tuple)):
        tovisit = list(startid)
        startList = startid
    else:
        tovisit = [startid]
        startList = [startid]
                
    def getRows():
        while tovisit:
            visitId = tovisit.pop()            
            if not edgesOnly: 
                if not excludeInitial or visitId not in startList:
                    #we need to yield a MutableTuplset instead of a list so that
                    #so that flatten called during property construction flattens 
                    #this result allowing follow() to be used as a property expression.
                    yield MutableTupleset(columns, [visitId] )
            found = not edgesOnly
            if reverse:
                from_ = 2; to = 0
            else:
                from_ = 0; to = 2
            for row in context.initialModel.filter({ from_ :visitId, 1:propname}):
                found = True
                obj = row[to]
                if obj not in tovisit:
                    if not edgesOnly:
                        yield MutableTupleset(columns, [obj])
                    tovisit.append(obj)
            if not found:
                if not excludeInitial or visitId not in startList:
                    assert edgesOnly
                    yield MutableTupleset(columns, [visitId])

    columns = [ColumnInfo('', object)]
    return SimpleTupleset(getRows, columns=columns, op='follow', hint=startid, 
                                                          debug=context.debug)

def followOpFactory(key, metadata, startOp, propnameOp, 
    excludeInitial=jqlAST.Constant(False), edgesOnly=jqlAST.Constant(False)):
    if isinstance(propnameOp, jqlAST.Project):
        propnameOp = jqlAST.PropString(propnameOp.name)
    else:
        raise QueryException("argument must be a property reference", propnameOp)
    return jqlAST.AnyFuncOp(key, metadata, startOp, propnameOp, excludeInitial,
                                                                     edgesOnly)

def ifFunc(context, ifArg, thenArg, elseArg):
    ifval = ifArg.evaluate(context.engine, context)
    if ifval:
        thenval = thenArg.evaluate(context.engine, context)        
        return thenval
    else:
        elseval = elseArg.evaluate(context.engine, context)
        return elseval

def isBnode(context, v):
    if isinstance(v, ResourceUri):
        v = v.uri
    elif not hasattr(v, 'startswith'):
        return False
    return v.startswith(context.initialModel.bnodePrefix)

#############################################################
################ Evaluation Engine ##########################
#############################################################

def getNullRows(columns):
    '''
    Given a column list return a matching row with None for each cell
    '''
    nullrows = [None] * len(columns)
    for i, c in enumerate(columns):        
        if isinstance(c.type, Tupleset):
            nullrows[i] = MutableTupleset([getNullRows(c.type.columns)])
        else:
            nullrows[i] = ColumnInfo(c.labels, None)
    return nullrows

def _serializeValue(context, v, isId):
    if not context.serializer:
        if isinstance(v, ResourceUri):
            return v.uri
        else:
            return v
    if isId:
        return context.serializer.serializeId(str(v))
    scope = None #context.scope
    if isinstance(v, ResourceUri):
        return context.serializer.serializeRef(v.uri, scope)
    elif not isinstance(v, (str,unicode)) and scope is None:
        return v
    else:
        return context.serializer._value(v, base.OBJECT_TYPE_LITERAL, scope)

def _setConstructProp(shape, pattern, prop, v, name, listCtor, context):
    isId = context.serializer and context.serializer.parseContext.idName == name
    isSeq = isinstance(v, (list,tuple))
    if v == NilResource: #special case to force empty list
        val = listCtor() #[]
    elif v is None or (isSeq and not len(v)):
        if prop.ifEmpty == jqlAST.PropShape.omit:            
            return pattern
        elif prop.ifEmpty == jqlAST.PropShape.uselist:
            if not isSeq:
                assert v is None
                val = listCtor()#[]
            else:
                val = v
        elif prop.ifEmpty == jqlAST.PropShape.usenull:
            val = None #null
    elif (prop.ifSingle == jqlAST.PropShape.nolist
                and not isSeq):
        val = _serializeValue(context,v, isId)
    #elif (prop.ifSingle == jqlAST.PropShape.nolist
    #        and len(v) == 1):    
    #    val = flatten(v[0])
    else: #uselist
        if isSeq:
            val = listCtor( (_serializeValue(context,i,isId) for i in v) )
        else:
            val = listCtor( [_serializeValue(context,v,isId)] )
    
    if shape is jqlAST.Construct.dictShape:
        if not isId:
            if isinstance(name, ResourceUri):
                name = name.uri
            if context.serializer:
                name = context.serializer.serializeProp(name)
        pattern[name] = val
    elif shape is jqlAST.Construct.listShape:        
        pattern.append(val)
    else:        
        pattern = val

    return pattern

def _getAllProps(idvalue, rows, propsAlreadyOutput):
    props = {}
    for row in rows:
        propname = row[PROPERTY]
        if propname not in propsAlreadyOutput:
            props.setdefault(propname, []).append(row[2:]) 
    
    for propname, valuerows in props.items():
        proprows = [idvalue, propname]+zip(*valuerows)
        yield propname, proprows                    

def _getList(idvalue, rows):
    ordered = []
    for row in rows:
        predicate = row[PROPERTY]
        if predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
            ordinal = int(predicate[len(RDF_MS_BASE+'_'):])
            ordered.append( (ordinal, row[2:]) )
    
    ordered.sort()
    for (i, row) in ordered:
        rdfprop = RDF_MS_BASE+'_'+str(i)
        yield rdfprop, (idvalue, rdfprop) + row                    

def hasNonAggregateDependentOps(op):
    '''
    Return True if there are any ConstructProps that having dependent 
    expressions excluding aggregate functions and any of their arguments.
    '''            
    test = (lambda op: isinstance(op, (jqlAST.Select, jqlAST.ConstructSubject))
        or (isinstance(op, jqlAST.AnyFuncOp) and op.isAggregate()) )
    return not op.isIndependent(exclude=test)

def safeFloat(n):
    #XXX should not catch exceptions if context is in a strict convert mode
    #due that to match underlying e.g. with sql92/oracle convertion semantics
    try:
        #note: like sql, python trims strings when converting to float
        if isinstance(n, ResourceUri):
            n = n.uri
        return float(n)
    except ValueError:
        return 0.0
    except TypeError:
        return 0.0

def _aggMin(x, y):
    if y is not None and x is not None:        
        return min(x,y)
    if x is not None:
        return x        
    return y

def _aggMax(x, y):
    if y is not None and x is not None:
        return max(x,y)
    if x is not None:
        return x
    return y

def _aggAvg(x, y):
    if y is None: 
        return x
    else:
        return len(x) and (x[0]+ safeFloat(y), x[1]+1.0) or (safeFloat(y),1.0)

def _aggCount(context, x, y):
    if context.groupby:
        if y == jqlAST.Project('*') or context.groupby.args[0] == y:
            #count(*) or count(key) and groupby(key)
            #groupby rows look like [key, groupby]
            return len(context.currentRow[1])
        else:
            v = context.engine.evalAggregate(context, y, True)
            return len(filter(lambda x: x is not None, v))
    elif y == jqlAST.Project('*'):
        return x+1
    elif y.evaluate(context.engine, context) is not None:
        return x+1
    else:
        return x

class SimpleQueryEngine(object):
    
    queryFunctions = QueryFuncs() 
    queryFunctions.addFunc('follow', lambda *args: followFunc(False, *args),
                           Tupleset, followOpFactory, needsContext=True)
    queryFunctions.addFunc('rfollow', lambda *args: followFunc(True, *args),
                           Tupleset, followOpFactory, needsContext=True)
    queryFunctions.addFunc('isbnode', isBnode, BooleanType, needsContext=True)
    queryFunctions.addFunc('if', ifFunc, ObjectType, lazy=True)
    queryFunctions.addFunc('isref', lambda a: isinstance(a, base.ResourceUri), BooleanType)
    #aggregate funcs follow the semantics described here:
    #http://www.sqlite.org/lang_aggfunc.html
    for name, func, initialValue, finalFunc in [
        ('sum', lambda x,y: y is not None and (
                x and x+safeFloat(y) or safeFloat(y)) or x, None, None),
        ('total', lambda x,y: y is not None and (
                x and x+float(y) or safeFloat(y)) or x, 0, None),
        ('avg', _aggAvg, (), lambda n, *a: len(n) and n[0]/n[1] or 0),
        ('min', _aggMin, None, None),
        ('max', _aggMax, None, None)]:
        queryFunctions.addFunc(name, func, NumberType, lazy=False, 
            isAggregate=True, initialValue=initialValue, finalFunc=finalFunc)
    queryFunctions.addFunc('count', _aggCount, NumberType, lazy=True,
        needsContext=True, isAggregate=True, initialValue=0)

    def isPropertyList(self, context, v):
        '''
        return True if the given resource is a proplist (an internal 
            list resource generated pjson to preserve list order)
        '''
        if isinstance(v, ResourceUri):
            v = v.uri
        elif not hasattr(v, 'startswith'):
            return False
        return v.startswith(context.initialModel.bnodePrefix+'j:proplist:')

    def isListResource(self, context, v):
        '''
        return True if the given resource is a json list. 
        '''
        #this just checks the bnode pattern for lists generated by pjson 
        #other models/mappings may need a different way to figure this out
        if isinstance(v, ResourceUri):
            v = v.uri
        elif not hasattr(v, 'startswith'):
            return False
        if v == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil':
            return True
        prefix = context.initialModel.bnodePrefix+'j:'        
        return v.startswith(prefix+'t:list:') or v.startswith(prefix+'e:list:')

    def isEmbeddedBNode(self, context, v):
        '''
        return True if the given resource is a json list. 
        '''
        #this just checks the bnode pattern for embedded objects generated by pjson 
        #other models/mappings may need a different way to figure this out
        if isinstance(v, ResourceUri):
            v = v.uri
        elif not hasattr(v, 'startswith'):
            return False
        #if not isinstance(v, base.ResourceUri):
        #    assert not hasattr(v, 'startswith') or not v.startswith(context.initialModel.bnodePrefix)
        #    return False

        prefix = context.initialModel.bnodePrefix+'j:'
        return v.startswith(prefix+'e:') or v.startswith(prefix+'proplist:')
    
    def isIdVisible(self, context, v):
        '''
        return True if the id should be serialized. 
        '''
        #XXX by default, we exclude ids for bnodes generated by pjson, but we might 
        #want to add option to exclude all bnodes or user-configurable pattern
        if context.forUpdate:
            return True
        if isinstance(v, ResourceUri):
            v = v.uri
        elif not hasattr(v, 'startswith'):
            return False
        #if not isinstance(idvalue, base.ResourceUri):
        #    assert not hasattr(idvalue, 'startswith') or not idvalue.startswith(context.initialModel.bnodePrefix)
        #    return False
        return not v.startswith(context.initialModel.bnodePrefix+'j:')
    
    def findPropList(self, context, subject, predicate):
        #by default search for special proplist bnode pattern
        #other models/mappings may need to implement a different way to figure this out
        listid = context.initialModel.bnodePrefix+'j:proplist:'+subject+';'+predicate
        rows = context.initialModel.filter({
            SUBJECT: listid
        })
        #print 'findprop', listid, list(rows)
        return rows

    def getShape(self, context, shape):
        return context.shapes.get(shape, shape)()
        
    def evalSelect(self, op, context):
        context.engine = self
        if context.serializer:
            parseContext = vesper.pjson.ParseContext(op.namemap, context.serializer.parseContext)
            context.serializer = pjson.Serializer(parseContext = parseContext)
        
        if op.isIndependent(): #constant expression
            context.currentTupleset = MutableTupleset(
                        [ColumnInfo('', object)], ([1],), op='constant')
        else: 
            if op.where:
                context.currentTupleset = op.where.evaluate(self, context)
            if op.groupby:
                context.currentTupleset = op.groupby.evaluate(self, context)
            if op.orderby:            
                context.currentTupleset = op.orderby.evaluate(self, context)

        if not op.groupby and op.construct.hasAggFunc:
            #reduce all the rows to an aggregate result
            #and set context so that finalFunc is called during construct below
            perRow = hasNonAggregateDependentOps(op.construct)
            tuplecopy = self._evalAggFuncs(op.construct,context, perRow)            
            if perRow:
                context.currentTupleset = tuplecopy
            else: #one dummy row
                context.currentTupleset = MutableTupleset(
                                [ColumnInfo('', object)], ([1],))
            context.finalizedAggs = True
        
        if op.depth is not None:
            context.depth = op.depth
                                                
        result = op.construct.evaluate(self, context)
        if op.mergeall:
            def merge():
                shape = op.construct.shape
                merged = self.getShape(context, shape)
                for row in result:                    
                    #XXX constructed results maybe of mixed types
                    #because we construct a list instead an object if the resource is a list
                    assert isinstance(row, type(merged)) 
                    if shape is jqlAST.Construct.dictShape:
                        merged.update(row)
                    else:
                        merged.extend( row )
                yield merged
            return SimpleTupleset(merge, hint=result, op='merge result',
                                                        debug=context.debug)
        else:
            return result

    def evalAggregate(self, context, op, keepSeq):
        v = []
        #if groupby then currrentRow is [key, values] (with each value grouped into a list) 
        #else currrentRow is [key] + values
        currentRow = context.currentRow
        currentTupleset = context.currentTupleset
        currentProjects = context.currentProjects
        row = currentRow[1]
        columns = currentTupleset.columns[1].type.columns
        
        #group by [ [v1, v2], [v1, v2] ]
        
        for cell in row:
            context.currentTupleset = SimpleTupleset(cell,
                columns=columns, 
                hint=currentTupleset, 
                op='evalAggregate', debug=context.debug)
            context.currentRow = cell
            context.projectValues = None
            projectValues = {}             
            for project in currentProjects:
                projectValues[project.name] = project.evaluate(self, context)
            context.projectValues = projectValues
            v.append( flatten(op.evaluate(self, context), flattenTypes=Tupleset) )
        
        context.projectValues = None
        context.currentTupleset = currentTupleset
        context.currentRow = currentRow
        
        if not keepSeq and len(v) == 1:
            return v[0]
        else:
            return v

    def _evalList(self, context, op):        
        #context is the currentRow
        projectValues = {}             
        listVals = []
        listNames = []
        for project in context.currentProjects:
            val = project.evaluate(self, context)
            if isinstance(val, list) and len(val) > 1:
                listNames.append(project.name)
                listVals.append(val)
            else:
                projectValues[project.name] = val
        context.projectValues = projectValues
        if not listVals:
            return flatten(op.evaluate(self, context), flattenTypes=Tupleset)

        v = []
        #alternative idea: instead of product zip up lists, error if unequal lengths
        #length = len(listVals[0])
        #if not all(len(l) == length for l in listVals):
        #    raise QueryException('Expression references multiple lists of unequal lengths')

        #for pv in itertools.izip(*listVals):
        for pv in product(*listVals):
            context.projectValues.update( dict(zip(listNames, pv)) )
            v.append( flatten(op.evaluate(self, context), flattenTypes=Tupleset) )
        assert len(v) > 1
        return v
    
    def _evalAggFuncs(self, op, context, copyInput):
        assert not op.parent.groupby
        tupleset = context.currentTupleset
        subjectcol, subjectlabel, rowcolumns = self._findSubject(op, tupleset)
        if copyInput:
            tuplecopy = MutableTupleset(tupleset.columns, tupleset)
        else:
            tuplecopy = None
        for outerrow in tupleset:
            #print 'outerrow', subjectlabel, subjectcol, hex(id(tupleset)), outerrow
            for idvalue, row in getColumns(subjectcol, outerrow):
                context.currentRow = [idvalue] + row
                for prop in op.args:
                    if not isinstance(prop, jqlAST.ConstructProp) or not prop.hasAggFunc:
                        continue                    
                    ccontext = copy.copy(context)
                    ccontext.accumulate = context.accumulate
                    v = context.currentRow
                    ccontext.currentTupleset = SimpleTupleset(v,
                        columns=rowcolumns, 
                        hint=tupleset, 
                        op='eval agg on '+subjectlabel, debug=context.debug)
                    
                    ccontext.currentProjects = prop.projects
                    if not row or len(row[0]) <= 1:
                        #don't bother with all the expression-in-list handling below
                        prop.value.evaluate(self, ccontext)
                    else:
                        self._evalList(ccontext, prop.value)
                                
            if copyInput:
                tuplecopy.append(outerrow)
        return tuplecopy
                                            
    def _findSubject(self, op, tupleset):
        assert isinstance(tupleset, Tupleset), type(tupleset)
        if not op.parent.groupby and not op.id.getLabel(): 
            subjectcol = (0,) 
            rowcolumns = tupleset.columns
            subjectlabel = ''
        else:
            if op.parent.groupby:
                subjectlabel = op.parent.groupby.name
            else:
                subjectlabel = op.id.getLabel()
            colinfo = tupleset.findColumnPos(subjectlabel, True)
            if not colinfo:
                raise QueryException(
                    'construct: could not find subject label "%s" in %s'
                    % (subjectlabel, tupleset.columns))
            else: 
                subjectcol, colTupleset = colinfo
                #last pos will be offset into the tupleset:
                col = colTupleset.columns[subjectcol[-1]] 
                rowcolumns = ([ColumnInfo(subjectlabel, col.type)]
                                    + getColumnsColumns(subjectcol,tupleset.columns))
        return subjectcol, subjectlabel, rowcolumns

    def _setAllProps(self, context, islist, propsAlreadyOutput, idvalue, allPropsShape, 
                                                    allPropsPattern, propsetOp):
        #XXX what about scope -- should initialModel just filter by scope if set?
        rows = context.initialModel.filter({ SUBJECT: idvalue })
        if islist:
            propset = _getList(idvalue, rows)
        else:
            propset = _getAllProps(idvalue, rows, propsAlreadyOutput)
        
        listCtor = context.shapes.get(jqlAST.Construct.listShape, jqlAST.Construct.listShape)
        for propname, proprows in propset:
            ccontext = copy.copy(context)
            ccontext.currentTupleset = SimpleTupleset([proprows],
                columns=context.initialModel.columns,
                op='allprop project on %s'% propname, 
                debug=context.debug)
            ccontext.currentRow = proprows
            value = jqlAST.Project(OBJECT, constructRefs=True
                                        ).evaluate(self, ccontext)
            _setConstructProp(allPropsShape, allPropsPattern, 
                        propsetOp, value, propname, listCtor, context)

    def evalConstruct(self, op, context):
        '''
        Construct ops operate on currentValue (a cell -- that maybe multivalued)
        '''
        tupleset = context.currentTupleset
        subjectcol, subjectlabel, rowcolumns = self._findSubject(op, tupleset)
        listCtor = context.shapes.get(jqlAST.Construct.listShape, jqlAST.Construct.listShape)
        
        def construct():
          count = 0
          i = 0
          
          assert context.currentTupleset is tupleset
          #print 'construct cols', tupleset.columns
          for outerrow in tupleset:
            #print 'outerrow', subjectlabel, subjectcol, hex(id(tupleset)), outerrow
            for idvalue, row in getColumns(subjectcol, outerrow):
                if self.isPropertyList(context, idvalue):
                    continue #skip prop list descriptor resources
                elif op.parent.skipEmbeddedBNodes and self.isEmbeddedBNode(context, idvalue):
                    continue
                elif self.isListResource(context, idvalue):
                    shape = op.listShape
                    islist = True
                else:
                    shape = op.shape
                    islist = False
                if isinstance(idvalue, ColumnInfo) and idvalue.type is None:
                    #this is what a outer join null looks like 
                    if op.shape is op.listShape: #return an empty list
                        yield self.getShape(context, op.shape)
                    return
                i+=1                
                if op.parent.offset is not None and op.parent.offset < i:
                    continue
                context.constructStack.append(idvalue)
                context.currentRow = [idvalue] + row
                if context.debug: 
                    validateRowShape(tupleset.columns, outerrow)
                    #print >> context.debug, 'valid outerrow'
                #note: right outer joins break the following validateRowShape
                if context.debug: validateRowShape(rowcolumns, context.currentRow)
                
                pattern = self.getShape(context, shape)
                allpropsOp = None
                propsAlreadyOutput = set((pjson.PROPSEQ,)) #exclude PROPSEQ
                for prop in op.args:
                    if isinstance(prop, jqlAST.ConstructSubject):
                        #print 'seriliaze id', op.dictShape, idvalue, 'name', prop.name
                        if shape is op.listShape: 
                            #XXX this check prevents ids from being in list results, 
                            #instead prop.name check should be set correctly
                            continue
                        if not allpropsOp and not context.forUpdate:
                            continue
                        #print 'cs', prop.name, idvalue
                        if op.parent.groupby:
                            continue #don't output id if groupby is specified
                        if not prop.name: #omit 'id' if prop name is empty
                            continue
                        #suppress this id?
                        if not self.isIdVisible(context, idvalue):
                            continue
                        if isinstance(idvalue, ResourceUri):
                            outputId = idvalue.uri
                        else:
                            outputId = idvalue
                        if context.serializer:
                            outputId = context.serializer.serializeId(outputId)
                        if shape is op.dictShape:
                            pattern[prop.name] = outputId
                        elif shape is op.listShape:
                            pattern.append(outputId)
                        else:
                            pattern = outputId
                    else:
                        isAllProp = isinstance(prop.value, jqlAST.Project
                                                ) and prop.value.name == '*'
                        if isAllProp and not prop.nameFunc:
                            #* is free standing (no name expression), 
                            #find allprops after we've processed the other properties
                            if shape is op.valueShape:
                                raise QueryException("value construct can not specify '*'")
                            allpropsOp = prop
                        else:
                            ccontext = copy.copy(context)
                            if isinstance(prop.value, jqlAST.Select):
                                #evaluate the select using a new context with the
                                #the current column as the tupleset

                                #if id label is set use that 
                                label = prop.value.construct.id.getLabel()
                                if label:
                                    col = tupleset.findColumnPos(label, True)
                                else:
                                    col = None                                
                                if not col:
                                    if prop.value.where:
                                        #assume we're doing a cross join:
                                        ccontext.currentTupleset = ccontext.initialModel
                                    else: #this construct just references the parent rows
                                        ccontext.currentTupleset = tupleset
                                else:
                                    pos, rowInfoTupleset = col
                                    v = list([irow for cell, i, irow in getColumn(pos, outerrow)])
                                    #print '!!!v', col, label, len(v), v#, 'row', row
                                    #assert isinstance(col.type, Tupleset)
                                    ccontext.currentTupleset = SimpleTupleset(v,
                                    columns=rowInfoTupleset.columns,
                                    hint=v,
                                    op='nested construct value', debug=context.debug)
                                
                                #print '!!v eval', prop, ccontext.currentRow, rowcolumns
                                v = prop.value.evaluate(self, ccontext)
                                v = flatten(v, flattenTypes=Tupleset)
                            elif isAllProp:
                                assert prop.nameFunc
                                if ccontext.depth < 1:
                                    ccontext.depth = 1
                                v = self.buildObject(ccontext, ResourceUri(idvalue), True)
                            else:
                                ccontext.finalizedAggs = context.finalizedAggs
                                ccontext.accumulate = context.accumulate
                                ccontext.groupby = op.parent.groupby
                                v = context.currentRow
                                ccontext.currentTupleset = SimpleTupleset(v,
                                    columns=rowcolumns, 
                                    hint=tupleset, 
                                    op='construct on '+subjectlabel, debug=context.debug)
                            
                                ccontext.currentProjects = prop.projects
                                if (not prop.projects or prop.projects[0] is prop.value
                                    or prop.hasAggFunc or not row or len(row[0]) <= 1):
                                    #don't bother with all the expression-in-list handling below                                    
                                    v = flatten(prop.value.evaluate(self, ccontext),
                                                                flattenTypes=Tupleset)            
                                elif op.parent.groupby:
                                    v = self.evalAggregate(ccontext, prop.value, False)
                                else:
                                    v = self._evalList(ccontext, prop.value)                          
                        
                            #print '####PROP', prop.name or prop.value.name, 'v', v
                            if prop.nameFunc:
                                name = flatten(prop.nameFunc.evaluate(
                                                self, context), to=listCtor)
                                if not name: #don't include property in result
                                    continue
                            else:
                                name = prop.name or prop.value.name
                                               
                            pattern = _setConstructProp(shape, pattern, prop, v,
                                                        name, listCtor, context)
                            if prop.value.name and isinstance(prop.value, jqlAST.Project):
                                propsAlreadyOutput.add(prop.value.name)
                
                if allpropsOp:
                    if shape is op.dictShape: 
                        #don't overwrite keys already created
                        propsAlreadyOutput.update(pattern)
                    
                    self._setAllProps(context, islist, propsAlreadyOutput, 
                                        idvalue, shape, pattern, allpropsOp)                    
                        
                currentIdvalue = context.constructStack.pop()
                assert idvalue == currentIdvalue
                yield pattern
                count+=1
                if op.parent.limit is not None and op.parent.limit < count:
                    break

        #columns = [ColumnInfo('construct', object)]            
        return SimpleTupleset(construct, hint=tupleset, op='construct', #columns=columns, 
                                                    debug=context.debug)

    def evalOrderBy(self, op, context):
        #XXX only order by if orderby is different then current order of tupleset
        tupleset = context.currentTupleset
        tupleset = MutableTupleset(tupleset.columns, tupleset, hint=tupleset, op='order by')

        assert all(isinstance(s.exp, jqlAST.Project) for s in op.args), 'only property name lists currently implemented'        
        #print 'c', tupleset.columns, [s.exp.name for s in op.args]
        def getpos(project):
            if project.isPosition():
                return (project.name,)
            else:
                return tupleset.findColumnPos(project.name) 
        positions = [getpos(s.exp) for s in op.args]

        reverse = all(not s.asc for s in op.args) #all desc
        if not reverse and not all(s.asc for s in op.args):
            #mixed asc and desc
            orders = [s.asc for s in op.args]
            def orderbyCmp(row1, row2):
                for pos, order in zip(positions, orders):
                    v1 = flatten( (c[0] for c in getColumn(pos, row1)))
                    v2 = flatten( (c[0] for c in getColumn(pos, row2)))
                    c = cmp(v1,v2)
                    if c == -1:
                        return order and -1 or 1
                    elif c == 1:
                        return order and 1 or -1
                return 0

            tupleset.sort(cmp=orderbyCmp)
        else:
            def extractKey(row):
                return [flatten( (c[0] for c in getColumn(pos, row))) for pos in positions]

            tupleset.sort(key=extractKey, reverse=reverse)

        return tupleset

    def evalGroupBy(self, op, context):
        tupleset = context.currentTupleset
        label = op.name
        position = tupleset.findColumnPos(label)
        assert position is not None, 'cant find %s in %s %s' % (label, tupleset, tupleset.columns)
        coltype = object        
        columns = [
            ColumnInfo(label, coltype),
            ColumnInfo('#groupby', chooseColumns(position, tupleset.columns) )
        ] 
        debug = context.debug
        return SimpleTupleset(
            lambda: groupbyUnordered(tupleset, position, debug=debug and columns),
            columns=columns, 
            hint=tupleset, op='groupby op on '+label,  debug=debug)

    def costGroupBy(self, op, context):
        return 1.0

    def _groupby(self, tupleset, joincond, msg='group by ',debug=False):
        '''
        group the given tupleset by the column specified by given join condition
        and return a tupleset whose first column is the group by key.
        '''
        #XXX use groupbyOrdered if we know tupleset is ordered by groupby key
        position = tupleset.findColumnPos(joincond.position)
        assert position is not None, 'cant find %r in %s %s' % (
                    joincond.position, tupleset, tupleset.columns)
        coltype = object
        #include the key we're going to group by as a value columns in case we
        #need all of the values in the key per row
        #XXX: analyze construct and only set this if its needed
        includekey = True
        columns = [
            ColumnInfo(joincond.parent.name or '', coltype),
            ColumnInfo(joincond.getPositionLabel(),
                                chooseColumns(position,tupleset.columns, includekey) )
        ]
        outerjoin = joincond.join in ('r')
        return SimpleTupleset(
            lambda: groupbyUnordered(tupleset, position,
                                    debug and columns, outerjoin, includekey),
            columns=columns,
            hint=tupleset, op=msg + repr((joincond.join, joincond.position)),  debug=debug)

    def reorderWithListInfo(self, context, op, listval):
        if isinstance(op.name, int):
            #it's an index into a statement row
            propname = context.currentRow[PROPERTY]
        else:
            propname = op.name
        subject = context.currentRow[SUBJECT]

        if not context.initialModel.canHandleStatementWithOrder:
            #statement-level list order info not supported by the model, try to find the list resource
            pred = propname #XXX convert to URI
            if pred.startswith(RDF_MS_BASE+'_'):
                #pjson parser will never generate a proplist resource for these
                #(instead it'll create a nested list resource)
                return (False, listval)
            #XXX handle scope here?
            rows = pjson.findPropList(context.initialModel, str(subject), pred)
            ordered = []
            rows = list(rows)
            if rows:
                leftovers = list(listval)
                for row in rows:
                    predicate = row[1]
                    if predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
                        ordinal = int(predicate[len(RDF_MS_BASE+'_'):])
                        if row[2] in listval:
                            #only include list items that matched the result
                            ordered.append( (ordinal, row[2]) )
                            try:
                                leftovers.remove(row[2])
                            except ValueError:
                                pass #this can happen if the list has duplicate items
            else:
                return (False, listval)
        else:
            if isinstance(op.name, int):
                listposLabel = LIST_POS
            else:
                listposLabel = propname+':pos'
            #XXX this is expensive, just reuse the getColumn results used by build listval
            listcol = context.currentTupleset.findColumnPos(listposLabel)
            assert listcol
            listpositions = flatten((c[0] for c in getColumn(listcol, context.currentRow)))
            if not listpositions or not isinstance(listpositions, tuple): #no position info, so not a json list
                return (False, listval)
            ordered = []
            leftovers = []
            for i, positions in enumerate(listpositions):
                if positions:
                    for p in positions:
                        ordered.append( (p, listval[i]) )
                else:
                    leftovers.append( listval[i] )


        ordered.sort()
        #include any values left-over in listval
        return (True, [v for p, v in ordered] + leftovers)

    def evalJoin(self, op, context):
        return self._evalJoin(op, context)

    #def evalNotExists(self, op, context):
    #    return self._evalJoin(op, context, 'a')

    #def evalSemiJoin(self, op, context):
        #semi-join (?id in { foo = 1})
    #    return self._evalJoin(op, context, 's')

    def evalUnion(self, op, context):
        #foo = 1 or bar = 2
        #Union(filter(foo=1), filter(bar=2))
        #columns: subject foo bar
        #merge join, add null if match isn't found
        #args = self.consolidateJoins(args)
        for joincond in op.args:
            assert isinstance(joincond, jqlAST.JoinConditionOp)
            result = joincond.op.evaluate(self, context)
            assert isinstance(result, Tupleset)

            current = self._groupby(result, joincond,debug=context.debug)

    def _evalJoin(self, op, context):
        args = sorted(op.args, key=lambda arg:
            #put non-inner joins and filters with complex predicates last
            #XXX: we should do semantic ordering earlier so it shows up in the ast
            #and maybe mark each group so we can do this cost-based ordering per group
            (getattr(arg.leftPosition, 'startswith', lambda s:False)('#@'),
            getattr(arg.op, 'complexPredicates', False), arg.join != 'i',
                                                arg.op.cost(self, context)) )

        tmpop = None
        if not args or args[0].join != 'i':
            #XXX we also need to do this in query like
            #{?parent id, 'child' : {* where(parent = ?parent)} }
            #because otherwise any value of the parent property will be treated
            #as an object reference even if it doesn't exist
            #but for efficiency it shouldn't be inserted first
            tmpop = jqlAST.JoinConditionOp(jqlAST.Filter())
            tmpop.parent = op
            args.insert(0, tmpop)

        #evaluate each op, then join on results
        #XXX optimizations:
        # 1. if the result of a prior filter can used for the filter
        # use that as source of the filter
        # 2. estimate and compare cost of using the prior result so next filter
        # can use that as source (compare with cost of filtering with current source)
        # 3.if we know results are ordered properly we can do a MergeJoin
        #   (more efficient than IterationJoin):
        #lslice = slice( joincond.position, joincond.position+1)
        #rslice = slice( 0, 1) #curent tupleset
        #current = MergeJoin(result, current, lslice,rslice)
        previous = None
        #print 'evaljoin', args
        while args:
            joincond = args.pop(0)
            assert isinstance(joincond, jqlAST.JoinConditionOp)
            #if isinstance(joincond.op, jqlAST.Filter) and not joincond.op.args and args:
                #skipping empty filter but this break stuff
                #joincond = args.pop(0)
            if previous and isinstance(joincond.op, jqlAST.Filter) and joincond.op.complexPredicates:
                fcontext = copy.copy(context)
                fcontext.currentTupleset = previous
            else:
                fcontext = context

            result = joincond.op.evaluate(self, fcontext)
            assert isinstance(result, Tupleset), repr(result) + repr(joincond.op)
            #group by the join key so that it is first column in the result
            #(except when its a cross-join, which has no key).
            #note: this guarantees one row on each side.
            if joincond.join == 'x':
                current = result
            else:
                current = self._groupby(result, joincond,debug=context.debug)
            
            if previous:
                def mergeColumns(left, right):
                    def find(col):
                        try:
                            return left.index(col)
                        except ValueError:
                            return -1
                    #skip right-side columns that match a left column
                    #except for the first one, since we are joining on that one
                    indexToLeft = [-1]+[find(col) for col in right[1:]]
                    newright = [col for i, col in enumerate(right) if indexToLeft[i] < 0]
                    return left+newright, newright, indexToLeft

                def bindjoinFunc(joincond, current, indexToLeft, nullrows, leftpos, previous):
                    '''
                    jointypes: inner, left outer, semi- and anti-
                    '''
                    jointype = joincond.join
                    def mergeRow(leftRow, rightRow):
                        #for each shared column, merge values
                        rows = []
                        for i, cell in enumerate(rightRow):
                            if indexToLeft[i] > -1:
                                lv = leftRow[ indexToLeft[i] ]
                                #print 'mergerow', lv, 'li', indexToLeft[i], 'ri', i, 'cell', cell
                                #only include values that both rows have
                                for c in cell:
                                    if c not in lv:
                                        lv.append(c)
                            else:
                                rows.append(cell)
                        return rows

                    def evalCrossJoin(leftRow, row):
                        if isinstance(joincond.position, jqlAST.QueryOp):
                            rcontext = copy.copy(context)
                            ts = SimpleTupleset([leftRow+row],
                                    columns=previous.columns+current.columns,
                                    op='complex join condition',
                                    debug=rcontext.debug)
                            rcontext.currentTupleset = ts
                            return list(joincond.position.evaluate(self, rcontext))
                        else:
                            return True


                    if isinstance(leftpos, int): #simple case
                        def joinFunc(leftRow, rightTable, lastRow):
                            match = []
                            if jointype != 'x': #if not cross-join
                                filters = {0 : leftRow[leftpos] }
                                hints={ 'makeindex' : 0 }
                            else:
                                filters = {}
                                hints = None
                            for row in rightTable.filter(filters, hints):
                                if jointype=='a': #antijoin, skip leftRow if found
                                    return
                                elif jointype=='s': #semijoin
                                    yield []
                                    match = True
                                elif jointype=='x': #crossjoin
                                    if evalCrossJoin(leftRow, row):
                                        match.append(row)
                                else:
                                    yield mergeRow(leftRow, row)
                                    match = True

                            if not match:
                                yield nullrows
                            elif jointype=='x':
                                #with crossjoins, instead of calling groupby on the
                                #join key (there isn't one) group the rows together now
                                row =  [MutableTupleset(None,match)]
                                #validateRowShape([ColumnInfo('', MutableTupleset(current.columns))], row)
                                yield row
                    else:
                        def joinFunc(leftRow, rightTable, lastRow):
                            match = []
                            for c in getColumn(leftpos, leftRow):
                                if jointype != 'x': #if not cross-join
                                    filters = {0 : c[0] }
                                    hints={ 'makeindex' : 0 }
                                else:
                                    filters = {}
                                    hints = None
                                for row in rightTable.filter(filters, hints):
                                    if jointype=='a': #antijoin, skip leftRow if found
                                        return
                                    elif jointype=='s': #semijoin
                                        yield []
                                        match = True
                                    elif jointype=='x': #crossjoin
                                        if evalCrossJoin(leftRow, row):
                                            match.append(row)
                                    else:
                                        match.append(mergeRow(leftRow, row))

                            if not match:
                                yield nullrows
                            elif jointype!='s':
                                #with crossjoins, instead of calling groupby on the
                                #join key (there isn't one) group the rows together now
                                row =  [MutableTupleset(None,match)]
                                #validateRowShape([ColumnInfo('', MutableTupleset(current.columns))], row)
                                yield row

                    return joinFunc

                if isinstance(joincond.leftPosition, int):
                    leftpos = joincond.leftPosition
                else:
                    leftpos = previous.findColumnPos(joincond.leftPosition)
                    assert leftpos is not None, 'cant find left pos %r in <%s> %s' % (
                              joincond.leftPosition, previous, previous.columns)
                    if len(leftpos) == 1:
                        leftpos = leftpos[0]

                coltype = object #XXX
                assert current.columns and len(current.columns) >= 1
                assert previous.columns is not None
                #columns: (left + right)
                indexToLeft = None
                jointype = joincond.join
                if jointype in ('i','l'):
                    columns, rightColumns, indexToLeft = mergeColumns(
                                              previous.columns, current.columns)
                    if not isinstance(leftpos, int):
                        rightColumns = [ColumnInfo('', MutableTupleset(rightColumns))]
                    columns = previous.columns + rightColumns
                elif jointype in ('a','s'):
                    columns = previous.columns
                elif jointype == 'x':
                    columns = previous.columns + [ColumnInfo('', 
                                              MutableTupleset(current.columns))]
                elif jointype == 'r':
                    columns = current.columns + previous.columns
                else:
                    assert False, 'unknown jointype: '+ jointype

                if jointype=='l':
                    nullrows = getNullRows(rightColumns)
                elif jointype=='a':
                    nullrows = [] #no match, so include leftRow
                else:
                    nullrows = None

                if jointype == 'r': #right outer
                    #XXX this is broken (but unused)
                    previous = IterationJoin(current, previous, bindjoinFunc(
                        'l', previous, indexToLeft, nullrows, leftpos, previous),
                                    columns,joincond.name,debug=context.debug)
                else:
                    previous = IterationJoin(previous, current, bindjoinFunc(
                     joincond, current, indexToLeft, nullrows, leftpos, previous),
                                    columns,joincond.name,debug=context.debug)
            else:
                previous = current

        return previous

    def _findSimplePredicates(self, op, context):
        simpleops = (jqlAST.Eq,) #only Eq supported for now
        complexargs = []
        simplefilter = {}
        for pred in op.args:
            complexargs.append(pred)
            if not isinstance(pred, simpleops):
                continue
            if isinstance(pred.left, jqlAST.Project) and pred.right.isIndependent():
                proj = pred.left
                other = pred.right
            elif isinstance(pred.right, jqlAST.Project) and pred.left.isIndependent():
                proj = pred.right
                other = pred.left
            else:
                continue
            if not proj.isPosition():
                continue
            if proj.name in simplefilter:
                #position already taken, treat as complex
                continue
            value = other.evaluate(self, context)
            if proj.name == OBJECT:
                #XXX if value is not a json value type, need a hook so there
                #can be date-store specific objectType
                #e.g. a date() query function could return a date object
                #as it stands, pjson.getDataType will raise an error 
                if context.serializer:
                    parseContext = context.serializer.parseContext
                else:
                    parseContext = None
                value, objectType = vesper.pjson.getDataType(value, parseContext)
                simplefilter[3] = objectType
            simplefilter[proj.name] = value
            complexargs.pop()
        return simplefilter, complexargs

    def evalFilter(self, op, context):
        '''
        Find slots
        '''
        simplefilter, complexargs = self._findSimplePredicates(op, context)

        columns = []
        for label, pos in op.labels:
            columns.append( ColumnInfo(label, object) )        
        saveValue = flatten([a.saveValue for a in op.args if a.saveValue])
        if saveValue:            
            assert not isinstance(saveValue, list)            
            assert not op.complexPredicates #not yet supported
        
        def colmap(row):
            for label, pos in op.labels:
                yield row[pos]

        tupleset = context.currentTupleset        
        
        #first apply all the simple predicates that we assume are efficient
        if simplefilter or not complexargs:
            #XXX: optimization: if cost is better filter on initialmodel
            #and then find intersection of result and currentTupleset
            tupleset = SimpleTupleset(
                lambda tupleset=tupleset: tupleset.filter(simplefilter),
                columns = complexargs and tupleset.columns or columns,
                colmap = not complexargs and colmap or None,
                hint=tupleset, op='selectWithValue1', debug=context.debug)

        if not complexargs:            
            return tupleset

        if op.isIndependent(): #constant expression
            tupleset = MutableTupleset([ColumnInfo('', object)], ([1],), op='constant')

        #now create a tupleset that applies the complex predicates to each row
        def getArgs():
            for i, pred in enumerate(complexargs):
                for arg in jqlAST.flattenOp(pred, jqlAST.And):
                     yield (arg.cost(self, context), i, arg)

        #XXX if filter is dependent on labels of other objects don't copy the
        #context until evaluating each row and merge the row with the currentRow  
        fcontext = copy.copy( context )
        if op.complexPredicates:
            fcontext.complexPredicateHack = True
        def filterRows():
            args = [x for x in getArgs()]
            args.sort() #sort by cost
            #for cost, i, arg in args:
            #    if arg.left.isIndependent():
                    #XXX evalFunc, etc. to use value
                    #XXX memoize results lazily
            #        arg.left.value = arg.left.evaluate(self, fcontext)
            
            alwaysmatch = saveValue and len(args) == 1

            for row in tupleset:
                value = None
                if saveValue: #XXX 
                    row = list(row)
                    row.append(value)
                #print len(row), row
                for cost, i, arg in args:
                    fcontext.currentRow = row
                    value = arg.evaluate(self, fcontext)
                    if arg.saveValue:
                        row[-1] = value
                    elif not value:
                        break

                if not value and not alwaysmatch:
                    continue
                yield saveValue and tuple(row) or row

        opmsg = 'complexfilter:'+ str(complexargs)
        
        return SimpleTupleset(filterRows, hint=tupleset,columns=columns,
                colmap=colmap, op=opmsg, debug=context.debug)

    def buildObject(self, context, v, handleNil):
        if handleNil and v == NilResource:
            #special case to force empty list
            return []
        refFunc = self.queryFunctions.getOp('isref')
        isrefQ = refFunc.execFunc(context, v)
        isref = isinstance(v, base.ResourceUri)
        assert isrefQ == isref, "q %s r %s" % (isrefQ,isref)
        bnodeFunc = self.queryFunctions.getOp('isbnode')
        isbnode = bnodeFunc.execFunc(context, v)
        if ( (isref and context.depth > 0) or isbnode):
                            #and v not in context.constructStack):

            #XXX because we can generate a duplicate objects it'd be nice
            #to cache the results. but this is a little complicated because
            #of depth -- cache different versions based on needed depth
            #have context track maxdepth, so we know how deep the object is
            #don't use cache object if maxdepth > context.depth
            #    v = context.constructCache[v]
            #    return v

            query = jqlAST.Select(
                        jqlAST.Construct([jqlAST.Project('*')]),
                        jqlAST.Join())
            ccontext = copy.copy(context)
            ccontext.currentTupleset = SimpleTupleset([[v]],
                    columns=[ColumnInfo('', object)],
                    op='recursive project on %s'%v,
                    debug=context.debug)
            if not isbnode:
                ccontext.depth -= 1
            result = list(query.evaluate(self, ccontext))
            if result:
                assert len(result) == 1, (
                    'only expecting one construct for %s, get %s' % (v, result))
                #context.constructCache[v] = result
                obj = result[0]
                if not isinstance(obj, dict):
                    v = obj
                else:
                    #only write out dict if has more property than just the id
                    #XXX handle case where id name isn't 'id'
                    count = int('id' in obj)
                    if len(obj) > count:
                        v = obj
        return v

    def evalProject(self, op, context):
        '''
        Operates on current row and returns a value

        Project only applies to * -- other Projections get turned into Filters
        (possible as outer joins) and so have already been processed
        '''
        # projection can be IterationJoin or a MergeJoin, the latter will walk
        # thru all resources in the db unless filter constraints can be used to deduce
        # a view or data engine join that can be used, e.g. an index on type
        # In fact as an optimization since most query results will have a limited
        # range of types we could first project on type and use that result to
        # choose the type index. On the other hand, if that projection uses an
        # iteration join it might be nearly expensive as doing the iteration join
        # on the subject only
        if context.projectValues: #already evaluated
            return context.projectValues[op.name]

        if isinstance(op.name, int):
            pos = None
            if op.name == SUBJECT and op.varref:
                pos = context.currentTupleset.findColumnPos(op.varref)
            if not pos:
                pos = (op.name,)
        else:
            pos = context.currentTupleset.findColumnPos(op.name)
            #print '!!', op.name, context.currentTupleset.columns
            if not pos:
                #print 'raise', context.currentTupleset.columns, 'row', context.currentRow
                raise QueryException("'%s' projection not found" % op.name)

        if op.constructRefs:
            val = flatten( (c[0] for c in getColumn(pos, context.currentRow)), keepSeq=True)
            assert isinstance(val, list)            
            isJsonList, val = self.reorderWithListInfo(context, op, val)    
            handleNil = isJsonList or len(val) > 1            
            val = [self.buildObject(context, v, handleNil) for v in val]
            #XXX add serialization option to flatten singleton lists, e.g.:
            #if context.flatten and isJsonList and not (len(val) == 1 and not isinstance(val[0], list))
            #(preserves [] and [[]] but not ['a'], instead serialize as 'a')
            if isJsonList:
                return val
            elif not val:            
                return None #empty
            elif len(val) == 1:
                return val[0]
            else:
                return val
        else:
            def renderVal(c):
                '''
                Output the value based on the current namemap
                '''
                val, pos, row = c
                objectTypePos = pos+1                 
                #return pjson.output(context.parsecontext, val, row[objectTypePos])
                return c[0]
            val = flatten( ( renderVal(c) for c in getColumn(pos, context.currentRow)) )
            return val

    def costProject(self, op, context):
        #if op.name == "*": 
        return 1.0 #XXX

    def evalLabel(self, op, context):
        position = context.currentTupleset.findColumnPos(op.name)
        assert position is not None, 'missing label: '+ op.name
        if position is None:
            return None
        return flatten( (c[0] for c in getColumn(position, context.currentRow)) )        

    def costLabel(self, op, context):
        return 1.0

    def costFilter(self, op, context):
        return 1.0 #XXX

        #simple cheaper than complex
        #dependent cheaper then
        #subject cheaper object cheaper than predicate
        SIMPLECOST = [1, 4, 2]
        #we have to evaluate each row
        COMPLEXCOST = []
        #we have to evaluate each row
        DEPENDANTCOST = []

        cost = 0 #no-op
        for i, pred in enumerate(op.args):
            if not pred:
                continue
            assert pred.getType() == BooleanType
            assert pred.left
            #XXX dont yet support something like: where customcompare(prop1, prop2)
            assert pred.right is None, 'multi-prop compares not yet implemented'

            if pred.left.isIndependent():
                if isinstance(pred, jqlAST.Eq): #simple
                    positioncost = SIMPLECOST
                else:
                    positioncost = COMPLEXCOST
            else:
                positioncost = DEPENDANTCOST

            cost += (pred.left.cost(self, context) * positioncost[i])

        return cost

    def costJoin(self, op, context):
        return 2.0 #XXX

        args = list(flattenSeq(op.args))
        #like costAndOp:
        if args:                    
            total = reduce(operator.add, [a.cost(self, context) for a in args], 0.0)
            cost = total / len(args)
        else:
            cost = 1.0

        if op.finalPosition == 1: #assume matching predicates are more expensive
            cost += 5
        if op.finalPosition == OBJTYPE_POS:
            cost += 10
        #if op.joinPosition < 0: #its absolute:
        #    cost /= 10          #independent thus cheaper?
        return cost 

    def evalConstant(self, op, context):
        return op.value

    def costConstant(self, op, context):
        return 0.0

    def evalBindVar(self, op, context):
        try:
            return context.bindvars[op.name]
        except KeyError:
            raise QueryException('bindvar "%s" not found' % op.name)
        #else:
        #    del context.bindvarUnused[op.name]

    def costBindVar(self, op, context):
        return 0.0

    def evalEq(self, op, context):
        lvalue = op.left.evaluate(self, context)
        if op.right:
            rvalue = op.right.evaluate(self, context)
        else:
            rvalue = context.currentValue
        
        if context.complexPredicateHack:
            #semantics: if one side is a list and the other isn't
            #do contains instead of equals
            llist = isinstance(lvalue, (list,tuple))
            rlist = isinstance(rvalue, (list,tuple))
            if not llist and rlist:
                return lvalue in rvalue
            elif llist and not rlist:
                return rvalue in lvalue
        return lvalue == rvalue

    def costEq(self, op, context): #XXX
        assert len(op.args) == 2, op
        return op.args[0].cost(self, context) + op.args[1].cost(self, context)

    def evalCmp(self, op, context):
        lvalue = op.left.evaluate(self, context)
        if op.right:
            rvalue = op.right.evaluate(self, context)
        else:
            rvalue = context.currentValue
        result = cmp(lvalue, rvalue)
        if result == 0 and (op.op == '<=' or op.op == '>='):
            return True
        elif result < 0 and op.op[0] == '<':
            return True
        elif result > 0 and op.op[0] == '>':
            return True        
        return False

    def costCmp(self, op, context): #XXX
        assert len(op.args) == 2
        return op.args[0].cost(self, context) + op.args[1].cost(self, context)

    def evalAggFuncOp(self, op, context):
        if context.finalizedAggs:
            result = context.accumulate.get(id(op), op.metadata.initialValue)
            func = op.metadata.finalFunc
            if func:
                return func(result, *op.args)
            else:
                return result
        elif context.groupby:
            if op.metadata.lazy:
                #if lazy is set on an aggfunc, it needs to handle all the logic
                #see _aggCount for an example
                return op.execFunc(context, op.metadata.initialValue, *op.args)
            
            v = context.engine.evalAggregate(context, op.args[0], True)
            assert isinstance(v, list)
            reduction = reduce(op.metadata.func, v, op.metadata.initialValue)
            if op.metadata.finalFunc:
                return finalFunc(reduction)
            else:
                return reduction
        else:
            if op.metadata.lazy:
                values = op.args
            else:
                values = [arg.evaluate(self, context) for arg in op.args]            
            last = context.accumulate.get(id(op), op.metadata.initialValue)
            result = op.execFunc(context, last, *values)
            context.accumulate[id(op)] = result
            return None
        
    def evalAnyFuncOp(self, op, context):
        if op.metadata.isAggregate:
            return self.evalAggFuncOp(op, context)
        
        listvalues = []
        if op.metadata.lazy:
            values = op.args
        else:
            checknulls = op.metadata.checkForNulls
            values = []
            for i, arg in enumerate(op.args):
                v = arg.evaluate(self, context)
                if checknulls > i and v is None:
                    return None
                if context.complexPredicateHack and isinstance(v, (list,tuple)):
                    listvalues.append( (i, v) )
                values.append(v)
            #XXX if op.metadata.signature:
            #    values = [c(v) for c,v in zip(op.metadata.signature, values)]
        
        if len(listvalues) == 1: #XXX replace complexpredicates hack!
            index, listval = listvalues[0]
            result = []
            for v in listval:
                values[index] = v
                result.append( op.execFunc(context, *values))
        else:
            result = op.execFunc(context, *values)
        return result

    def costAnyFuncOp(self, op, context):
        if op.metadata.costFunc:
            cost = op.metadata.costFunc(self, context)        
            if not op.isIndependent():
                return cost * 50 #dependent is much more expensive
            else:
                return cost
        else:
            return 1 #XXX

    def evalNot(self, op, context):
        left = op.args[0]
        lvalue = left.evaluate(self, context)
        return not lvalue

    def costNot(self, op, context):
        return 1

    def evalOr(self, op, context):
        '''
        Return left value if left evaluates to true, otherwise return right value.
        '''
        left = op.args[0]
        lvalue = left.evaluate(self, context)
        if lvalue:
            return lvalue
        right = op.args[1]
        rvalue = right.evaluate(self, context)
        return rvalue
        
    def costOr(self, op, context):
        return 1
        return reduce(operator.add, [a.cost(self, context) for a in op.args], 0.0)

    def evalAnd(self, op, context):
        '''
        Return right value if both left and right are true otherwise return False
        '''
        left = op.args[0]
        lvalue = left.evaluate(self, context)
        right = op.args[1]
        rvalue = right.evaluate(self, context)
        if lvalue and rvalue:
            return rvalue
        else:
            return False
        
    def costAnd(self, op, context):
        return 1
        return reduce(operator.add, [a.cost(self, context) for a in op.args], 0.0)

    def evalIn(self, op, context):
        left = op.args[0]
        lvalue = left.evaluate(self, context)        
        if not isinstance(lvalue, (list,tuple)):
            llist = [lvalue]
        else:
            llist = list(lvalue)
        args = op.args[1:]

        #context = copy.copy( context )
        #XXX sort by cost
        for arg in args:
            rvalue = arg.evaluate(self, context)
            for lvalue in llist:
                if isinstance(rvalue, Tupleset):
                    for row in rvalue:
                        if lvalue == row[0]:
                            return True
                    return False
                elif isinstance(rvalue, (list,tuple)):
                    if lvalue in rvalue:
                        return True
                elif rvalue == lvalue:
                    return True
        return False

    def costIn(self, op, context):
        return 1
        return reduce(operator.add, [a.cost(self, context) for a in op.args], 0.0)

