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
from vesper.data.base import RDF_MS_BASE
from vesper.utils import flattenSeq, flatten, debugp
from vesper import pjson
from vesper.query import *
from vesper.query.operations import validateRowShape, SimpleTupleset, MutableTupleset, IterationJoin

#############################################################
####################  Grouping  #############################
#############################################################

def getColumns(keypos, row, tableType=Tupleset, outerjoin=False):
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

    >>> list( getColumns((1,1), t, list) )
    [([('c1', 'd1')], ['a1', 'b1']), ([('c2', 'd1')], ['a1', 'b2'])]
    """
    keypos = list(keypos)
    pos = keypos.pop(0)
    cols = []
    for i, cell in enumerate(row):
        if i == pos:
            keycell = cell
        else:
            cols.append(cell)
    assert outerjoin or keycell
    if not keypos:
        yield keycell, cols
    else: #i above keypos
        assert isinstance(keycell, tableType), "cell %s (%s) is not %s p %s restp %s" % (keycell, type(keycell), tableType, pos, keypos)
        if outerjoin and not keycell:            
            nullrow = getNullRows(keycell.columns[0])
            for key, nestedcols in getColumns(keypos, nullrow, tableType):
                yield key, cols+nestedcols                
        for nestedrow in keycell:
            for key, nestedcols in getColumns(keypos, nestedrow, tableType):
                yield key, cols+nestedcols

def getColumnsColumns(keypos, columns):
    '''
    Return a column list that corresponds to the shape of the rows returned 
    by the equivalent call to `getColumns`.
    '''
    keypos = list(keypos)
    pos = keypos.pop(0)
    cols = []
    for i, c in enumerate(columns):
        if pos == i:
            if keypos:
                assert isinstance(c.type, Tupleset)
                nestedcols = getColumnsColumns(keypos, c.type.columns)
                if nestedcols:
                    cols.extend(nestedcols)
        else:
            cols.append(c)
    return cols
            
def groupbyUnordered(tupleset, groupby, debug=False, outerjoin=False):
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
        for key, outputrow in getColumns(groupby, row, outerjoin=outerjoin):
            vals = resources.get(key)
            if vals is None:
                vals = MutableTupleset()
                resources[key] = vals
            vals.append(outputrow)

    for key, values in resources.iteritems():
        #print 'gb out', [key, values]
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

def chooseColumns(groupby, columns):
    '''
    "above" columns go first, omit groupby column
    '''
    groupby = list(groupby)
    pos = groupby.pop(0)
    outputcolumns = []
    groupbycol = None
    for i, c in enumerate(columns):
        if pos == i:
            if groupby:
                groupbycol = chooseColumns(groupby, c.type.columns)
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

class QueryFuncs(object):

    SupportedFuncs = {
        (EMPTY_NAMESPACE, 'true') :
          jqlAST.QueryFuncMetadata(lambda *args: True, BooleanType, None, True,
                            lambda *args: 0),
        (EMPTY_NAMESPACE, 'false') :
          jqlAST.QueryFuncMetadata(lambda *args: False, BooleanType, None, True,
                            lambda *args: 0),
    }

    def addFunc(self, name, func, type=None, cost=None, needsContext=False, 
                                                            lazy=False, isAggregate=False):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        if cost is None or callable(cost):
            costfunc = cost
        else:
            costfunc = lambda *args: cost
        self.SupportedFuncs[name] = jqlAST.QueryFuncMetadata(func, type, 
                        costFunc=costfunc, needsContext=needsContext, lazy=lazy,
                        isAggregate=isAggregate)

    def getOp(self, name, *args):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        funcMetadata = self.SupportedFuncs.get(name)
        if not funcMetadata:
            raise QueryException('query function not defined: ' + str(name))
        funcMetadata = self.SupportedFuncs[name]
        
        return funcMetadata.opFactory(name,funcMetadata,*args)
    
    def __init__(self):
        #add basic functions
        self.addFunc('add', lambda a, b: float(a)+float(b), NumberType)
        self.addFunc('sub', lambda a, b: float(a)-float(b), NumberType)
        self.addFunc('mul', lambda a, b: float(a)*float(b), NumberType)
        self.addFunc('div', lambda a, b: float(a)/float(b), NumberType)
        self.addFunc('mod', lambda a, b: float(a)%float(b), NumberType)
        self.addFunc('negate', lambda a: -float(a), NumberType)
        self.addFunc('bool', lambda a: bool(a), BooleanType)
        self.addFunc('upper', lambda a: a.upper(), StringType)
        self.addFunc('lower', lambda a: a.lower(), StringType)
        self.addFunc('trim', lambda a,chars=None: a.strip(chars), StringType)
        self.addFunc('ltrim', lambda a,chars=None: a.lstrip(chars), StringType)
        self.addFunc('rtrim', lambda a,chars=None: a.rstrip(chars), StringType)        

def recurse(context, startid, propname=None):
    '''
    Starting with the given id, follow the given property
    '''
    #XXX expand propname
    #XXX add unittest for circularity
    if isinstance(startid, (list, tuple)):
        tovisit = list(startid)
    else:
        tovisit = [startid]

    def getRows():
        while tovisit:
            startid = tovisit.pop()
            yield [startid]            
            for row in context.initialModel.filter({0:startid, 1:propname}):
                obj = row[2]
                if obj not in tovisit:
                    yield [obj]
                    tovisit.append(obj)

    columns = [ColumnInfo('', object)]
    return SimpleTupleset(getRows, columns=columns, op='recurse', hint=startid, debug=context.debug)

def ifFunc(context, ifArg, thenArg, elseArg):
    ifval = ifArg.evaluate(context.engine, context)
    if ifval:
        thenval = thenArg.evaluate(context.engine, context)        
        return thenval
    else:
        elseval = elseArg.evaluate(context.engine, context)
        return elseval

def aggFunc(context, arg, func=None):
    groupbyrow = False
    if isinstance(arg, jqlAST.Project):
        if arg.name == '*':
            groupbyrow = True
        else:
            parent = arg.parent
            while parent:
                if isinstance(parent, jqlAST.Select):
                    groupby = parent.groupby
                    break
                parent = parent.parent
            
            if arg.name == groupby.args[0].name:
                groupbyrow = True
        
    if groupbyrow:
        #special case to support constructions like count(*)
        #groupby rows look like [key, groupby]
        v = context.currentRow[1]
    else:
        v = context.engine._evalAggregate(context, arg, True)
    return func(v)

def isBnode(context, v):    
    return hasattr(v, 'startswith') and (
                v.startswith(context.initialModel.bnodePrefix))

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
            nullrows[i] = ColumnInfo(c.label, None)
    return nullrows

# def serializeValue(context, v):
#     scope = None
#     if isinstance(v, ResourceURI):
#         objectType = RESOURCE
#     elif not isinstance(v, (str,unicode)) and scope is None:
#         return v
#     else:
#         return context.serializer.serializeValue(v, objectType, scope)
        
def _setConstructProp(shape, pattern, prop, v, name):
    isSeq = isinstance(v, (list,tuple))
    #print '_setConstructProp', v, isSeq, prop.ifEmpty
    if v == RDF_MS_BASE+'nil': #special case to force empty list
        val = []
    elif v is None or (isSeq and not len(v)):
        if prop.ifEmpty == jqlAST.PropShape.omit:            
            return pattern
        elif prop.ifEmpty == jqlAST.PropShape.uselist:
            if not isSeq:
                assert v is None
                val = []
            else:
                val = v
        elif prop.ifEmpty == jqlAST.PropShape.usenull:
            val = None #null
    elif (prop.ifSingle == jqlAST.PropShape.nolist
                and not isSeq):
        val = v
    #elif (prop.ifSingle == jqlAST.PropShape.nolist
    #        and len(v) == 1):    
    #    val = flatten(v[0])
    else: #uselist
        if isSeq:
            val = v
        else:
            val = [v]
    
    if shape is jqlAST.Construct.dictShape:
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

class SimpleQueryEngine(object):
    
    queryFunctions = QueryFuncs() 
    queryFunctions.addFunc('recurse', recurse, Tupleset, needsContext=True)
    queryFunctions.addFunc('isbnode', isBnode, BooleanType, needsContext=True)
    queryFunctions.addFunc('if', ifFunc, ObjectType, lazy=True)
    queryFunctions.addFunc('isref', lambda a: isinstance(a, base.ResourceUri), BooleanType)
    for name, func in [('count', lambda a: len(a)), 
                        ('sum', lambda a: sum(a)),
                       ('avg', lambda a: sum(a)/len(a)), 
                       ('min', lambda a: min(a)),
                       ('max', lambda a: max(a))]:        
        queryFunctions.addFunc(name, partial(aggFunc, func=func), NumberType, 
                                                 lazy=True, isAggregate=True)

    def isPropertyList(self, context, idvalue):
        '''
        return True if the given resource is a proplist (an internal 
            list resource generated pjson to preserve list order)
        '''
        if not hasattr(idvalue, 'startswith'):
            return False
        return idvalue.startswith(context.initialModel.bnodePrefix+'j:proplist:')        

    def isListResource(self, context, idvalue):
        '''
        return True if the given resource is a json list. 
        '''
        #this just checks the bnode pattern for lists generated by pjson 
        #other models/mappings may need a different way to figure this out
        if not hasattr(idvalue, 'startswith'):
            return False
        prefix = context.initialModel.bnodePrefix+'j:'        
        return idvalue.startswith(prefix+'t:list:') or idvalue.startswith(
                                                            prefix+'e:list:')

    def isEmbeddedBNode(self, context, idvalue):
        '''
        return True if the given resource is a json list. 
        '''
        #this just checks the bnode pattern for embedded objects generated by pjson 
        #other models/mappings may need a different way to figure this out
        if not hasattr(idvalue, 'startswith'):
            return False
        prefix = context.initialModel.bnodePrefix+'j:'
        return idvalue.startswith(prefix+'e:') or idvalue.startswith(prefix+'proplist:')
    
    def isIdVisible(self, context, idvalue):
        '''
        return True if the given resource is a json list. 
        '''
        #XXX by default, we exclude ids for bnodes generated by pjson, but we might 
        #want to add option to exclude all bnodes or user-configurable pattern
        if context.forUpdate:
            return True
        if not hasattr(idvalue, 'startswith'):
            return False            
        return not idvalue.startswith(context.initialModel.bnodePrefix+'j:')        
    
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
        if op.isIndependent(): #constant expression
            context.currentTupleset = MutableTupleset([ColumnInfo('', object)], ([1],))
        else: 
            if op.where:
                context.currentTupleset = op.where.evaluate(self, context)
            if op.groupby:
                context.currentTupleset = op.groupby.evaluate(self, context)
            if op.orderby:            
                context.currentTupleset = op.orderby.evaluate(self, context)

        if op.depth is not None:
            context.depth = op.depth
                                                
        #print 'where ct', context.currentTupleset
        result = op.construct.evaluate(self, context)
        if op.mergeall:
            def merge():
                shape = op.construct.shape
                merged = self.getShape(context, shape)
                for row in result:
                    if shape is jqlAST.Construct.dictShape:
                        merged.update(row)
                    else:
                        merged.extend( row )
                yield merged
            return SimpleTupleset(merge, hint=result, op='merge result',
                                                        debug=context.debug)
        else:
            return result

    def _evalAggregate(self, context, op, keepSeq=False):
        v = []
        #currrentRow is [key, values]
        currentRow = context.currentRow
        currentTupleset = context.currentTupleset
        #print 'currentRow', debugp(currentRow), 'currentTp', debugp(currentTupleset.columns)
        columns = currentTupleset.columns[1].type.columns
        currentProjects = context.currentProjects
        
        for cell in currentRow[1]:
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
            #print 'projects', projectValues
            v.append( flatten(op.evaluate(self, context), flattenTypes=Tupleset) )
        
        context.projectValues = None
        context.currentTupleset = currentTupleset
        context.currentRow = currentRow
        
        if not keepSeq and len(v) == 1:
            return v[0]
        else:
            return v

    def evalConstruct(self, op, context):
        '''
        Construct ops operate on currentValue (a cell -- that maybe multivalued)
        '''
        tupleset = context.currentTupleset
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
                    print >> context.debug, 'valid outerrow'
                #note: right outer joins break the following validateRowShape
                if context.debug: validateRowShape(rowcolumns, context.currentRow)
                
                pattern = self.getShape(context, shape)
                allpropsOp = None
                propsAlreadyOutput = set((pjson.PROPSEQ,)) #exclude PROPSEQ
                for prop in op.args:
                    if isinstance(prop, jqlAST.ConstructSubject):
                        if shape is op.listShape:
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
                        if shape is op.dictShape:
                            pattern[prop.name] = idvalue
                        elif shape is op.listShape:
                            pattern.append(idvalue)
                        else:
                            pattern = idvalue
                    elif isinstance(prop.value, jqlAST.Project) and prop.value.name == '*':
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
                            assert label
                            col = tupleset.findColumnPos(label, True)                            
                            if not col:
                                #assume we're doing a cross join:
                                ccontext.currentTupleset = ccontext.initialModel
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
                        else:
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
                            else:                                
                                v = self._evalAggregate(ccontext, prop.value)
                        
                        #print '####PROP', prop.name or prop.value.name, 'v', v
                        if prop.nameFunc:
                            name = flatten(prop.nameFunc.evaluate(self, ccontext))                            
                        else:
                            name = prop.name or prop.value.name                            
                        pattern = _setConstructProp(shape, pattern, prop, v, name)                        
                        if prop.value.name and isinstance(prop.value, jqlAST.Project):
                            propsAlreadyOutput.add(prop.value.name)
                
                if allpropsOp:
                    if shape is op.dictShape: 
                        #don't overwrite keys already created
                        propsAlreadyOutput.update(pattern)

                    #XXX what about scope -- should initialModel just filter by scope if set?
                    rows = context.initialModel.filter({ SUBJECT: idvalue })
                    if islist:
                        propset = _getList(idvalue, rows)
                    else:
                        propset = _getAllProps(idvalue, rows, propsAlreadyOutput)
                        
                    for propname, proprows in propset:
                        ccontext = copy.copy(context)
                        ccontext.currentTupleset = SimpleTupleset([proprows],
                            columns=context.initialModel.columns,
                            op='allprop project on %s'% propname, 
                            debug=context.debug)
                        ccontext.currentRow = proprows
                        value = jqlAST.Project(OBJECT, constructRefs=True).evaluate(self, ccontext)
                        _setConstructProp(shape, pattern, allpropsOp, value, propname)                    

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
        tupleset = MutableTupleset(context.currentTupleset.columns, context.currentTupleset)

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
            lambda: groupbyUnordered(tupleset, position,
                debug=debug and columns),
            columns=columns, 
            hint=tupleset, op='groupby op on '+label,  debug=debug)

    def costGroupBy(self, op, context):
        return 1.0

    def _groupby(self, tupleset, joincond, msg='group by ',debug=False):
        #XXX use groupbyOrdered if we know tupleset is ordered by groupby key
        position = tupleset.findColumnPos(joincond.position)
        assert position is not None, 'could find %r in %s %s' % (
                    joincond.position, tupleset, tupleset.columns)
        coltype = object
        #print 'groupby', joincond.parent.name, joincond.parent        
        columns = [
            ColumnInfo(joincond.parent.name or '', coltype),
            ColumnInfo(joincond.getPositionLabel(),
                                    chooseColumns(position,tupleset.columns) )
        ]
        outerjoin = joincond.join in ('r')
        return SimpleTupleset(
            lambda: groupbyUnordered(tupleset, position,
                debug=debug and columns, outerjoin=outerjoin),
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
            rows = pjson.findPropList(context.initialModel, subject, pred)
            ordered = []
            rows = list(rows)
            if rows:
                for row in rows:
                    predicate = row[1]
                    if predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
                        ordinal = int(predicate[len(RDF_MS_BASE+'_'):])
                        assert row[2] in listval, '%s not in %s' % (row[2], listval)
                        if row[2] in listval:
                            ordered.append( (ordinal, row[2]) )                        
            else:
                return (False, listval)
        else:
            if isinstance(op.name, int):
                listposLabel = LIST_POS
            else:
                listposLabel = propname+':pos'
            listcol = context.currentTupleset.findColumnPos(listposLabel) 
            assert listcol
            listpositions = flatten(c[0] for c in getColumn(listcol, context.currentRow))
            if not listpositions: #no position info, so not a json list
                return (False, listval)
            ordered = []
            for i, positions in enumerate(listpositions):
                for p in positions:
                    ordered.append( (p, listval[i]) )
        
        ordered.sort()        
        return (True, [v for p, v in ordered])

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
        #XXX context.currentTupleset isn't set when returned
        args = sorted(op.args, key=lambda arg: 
                #put non-inner joins last
                (arg.join != 'i', arg.op.cost(self, context)) )
        
        tmpop = None
        if not args or args[0].join != 'i':
            tmpop = jqlAST.JoinConditionOp(jqlAST.Filter())
            #tmpop = jqlAST.JoinConditionOp(
            #    jqlAST.Filter(jqlAST.Not(
            #        self.queryFunctions.getOp('isbnode', jqlAST.Project(0)))))
            tmpop.parent = op
            args.insert(0, tmpop)

        
        #else:
        #   combine separate filters into one filter
        #   by default, filters that are operating on the same projection
        #   engine subclasses that support table-based stores will combine projections on the same table
        #   args = self.consolidateJoins(args)

        #evaluate each op, then join on results
        #XXX optimizations:
        # 1. if the result of a projection can used for a filter, apply it and
        # use that as source of the filter
        # 2. estimate and compare cost of projecting the prior result so next filter
        # can use that as source (compare with cost of filtering with current source)
        # 3.if we know results are ordered properly we can do a MergeJoin
        #   (more efficient than IterationJoin):
        #lslice = slice( joincond.position, joincond.position+1)
        #rslice = slice( 0, 1) #curent tupleset is a
        #current = MergeJoin(result, current, lslice,rslice)
        previous = None
        #print 'evaljoin', args
        while args:
            joincond = args.pop(0)
            assert isinstance(joincond, jqlAST.JoinConditionOp)
            #if isinstance(joincond.op, jqlAST.Filter) and not joincond.op.args and args:
                #skipping empty filter but this break stuff
                #joincond = args.pop(0)
            result = joincond.op.evaluate(self, context)
            assert isinstance(result, Tupleset), repr(result) + repr(joincond.op)
            #print 'results', joincond.op, result

            current = self._groupby(result, joincond,debug=context.debug)
            if previous:
                def bindjoinFunc(jointype, current):
                    '''
                    jointypes: inner, left outer, semi- and anti-
                    '''
                    if jointype=='l':
                        nullrows = getNullRows(current.columns)
                    elif jointype=='a':
                        nullrows = [] #no match, so include leftRow
                    else:
                        nullrows = None

                    def joinFunc(leftRow, rightTable, lastRow):
                        match = False
                        for row in rightTable.filter({0 : leftRow[0]},
                            hints={ 'makeindex' : 0 }):
                                if jointype=='a': #antijoin, skip leftRow if found
                                    return
                                elif jointype=='s': #semijoin
                                    yield []
                                else:
                                    yield row
                                match = True
                        if not match:
                            yield nullrows

                    return joinFunc

                coltype = object #XXX
                assert current.columns and len(current.columns) >= 1
                assert previous.columns is not None
                #columns: (left + right)
                jointype = joincond.join
                if jointype in ('i','l'):
                    columns = previous.columns + current.columns
                elif jointype in ('a','s'):
                    columns = previous.columns
                elif jointype == 'r':
                    columns = current.columns + previous.columns
                else:
                    assert False, 'unknown jointype: '+ jointype
                if jointype == 'r': #right outer 
                    previous = IterationJoin(current, previous,
                                    bindjoinFunc('l', previous),
                                    columns,joincond.name,debug=context.debug)
                else:
                    previous = IterationJoin(previous, current,
                                    bindjoinFunc(jointype, current),
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

        #now create a tupleset that applies the complex predicates to each row
        def getArgs():
            for i, pred in enumerate(complexargs):
                for arg in jqlAST.flattenOp(pred, jqlAST.And):
                     yield (arg.cost(self, context), i, arg)

        fcontext = copy.copy( context )
        def filterRows():
            args = [x for x in getArgs()]
            args.sort() #sort by cost
            #for cost, i, arg in args:
            #    if arg.left.isIndependent():
                    #XXX evalFunc, etc. to use value
                    #XXX memoize results lazily
            #        arg.left.value = arg.left.evaluate(self, fcontext)

            for row in tupleset:
                value = None
                for cost, i, arg in args:                    
                    fcontext.currentRow = row
                    #fcontext.currentValue = row[i]
                    value = arg.evaluate(self, fcontext)
                    #if a filter function returns a tupleset 
                    #yield those rows instead of the input rows
                    #otherwise, treat return value as a boolean and use it to
                    #filter the input row
                    if isinstance(value, Tupleset):
                        assert len(args)==1
                        for row in value:
                            yield row
                        return
                    if not value:
                        break

                if not value:
                    continue                
                yield row

        opmsg = 'complexfilter:'+ str(complexargs)
        return SimpleTupleset(filterRows, hint=tupleset,columns=columns,
                colmap=colmap, op=opmsg, debug=context.debug)
    
    def buildObject(self, context, v, handleNil):
        if handleNil and v == RDF_MS_BASE+'nil': 
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
        # on the subject only.        
        if context.projectValues: #already evaluated
            return context.projectValues[op.name]
        
        if isinstance(op.name, int):
            pos = (op.name,)
        else:
            pos = context.currentTupleset.findColumnPos(op.name)
            if not pos:
                #print 'raise', context.currentTupleset.columns, 'row', context.currentRow
                raise QueryException("'%s' projection not found" % op.name)
        
        #val = flatten( (c[0] for c in getColumn(pos, context.currentRow)), keepSeq=op.constructRefs)
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
        #XXX
        lvalue = op.left.evaluate(self, context)
        if op.right:
            rvalue = op.right.evaluate(self, context)
        else:
            rvalue = context.currentValue
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

    def evalAnyFuncOp(self, op, context):
        if op.metadata.lazy:
            values = op.args
        else:
            values = [arg.evaluate(self, context) for arg in op.args]
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

