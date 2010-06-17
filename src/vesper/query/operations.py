#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
Tupleset sub-classes used by the jql query engine
=================================================
"""
import sys, pprint, itertools

from vesper.data import base
from vesper.utils import flatten, debugp
from vesper.query import *

def _colrepr(self):
    colstr =''
    if self.columns:
        def x(c):
            if len(c.labels) > 1:
                s = repr(c.labels)
            else:
                s = repr(c.labels[0])
            if isinstance(c.type, Tupleset):
                s += '['+','.join( map(x,c.type.columns) ) +']'
            return s
        colstr = ','.join( map(x,self.columns) )
    return 'C('+colstr+')'

def validateRowShape(columns, row):
    if columns is None:
        return
    assert isinstance(row, (tuple,list)), row
    #this next assertion should be ==, not <=, but can't figure how that is happening 
    assert len(columns) <= len(row), '(c %d:%s, r %d:%s)'%(len(columns), columns,  len(row), row)
    for (i, (ci, ri)) in enumerate(itertools.izip(columns, row)):
        if isinstance(ci.type, Tupleset):
            assert isinstance(ri, Tupleset), (ri, type(ri), ci.type, i)
            if ri:
                #validate the first row of the tupleset
                return validateRowShape(ci.type.columns, ri[0])
        else:
            assert not isinstance(ri, Tupleset), (ri, ci.type, i)

class SimpleTupleset(Tupleset):
    '''
    Applies a filter to source tupleset
    '''
    
    def __init__(self, generatorFuncOrSeq=(),hint=None, op='',
                                columns=None, debug=False, colmap=None):
        if not callable(generatorFuncOrSeq):
            #assume its a sequence
            self.generator = lambda: iter(generatorFuncOrSeq)
            self.seqSize = len(generatorFuncOrSeq)
            self.hint = hint or generatorFuncOrSeq
        else:
            self.generator = generatorFuncOrSeq
            self.seqSize = sys.maxint
            self.hint = hint #for debugging purposes
        self.op=op #msg for debugging
        self.debug = debug #for even more debugging
        self.cache = None
        self.columns = columns
        self.colmap = colmap
        if debug:
            self._filter = self.filter
            self.filter = self._debugFilter
            self.debug = debug

    def _debugFilter(self, conditions=None, hints=None):
        results = tuple(self._filter(conditions, hints))        
        print >>self.debug, self.__class__.__name__,hex(id(self)), '('+self.op+')', \
          'on', self.hint, 'cols', _colrepr(self), 'filter:', repr(conditions),\
          'results:'
        pprint.pprint(results,self.debug)
        [validateRowShape(self.columns, r) for r in results]
        for row in results:
            yield row

    def size(self):    
        return self.seqSize
        
    def filter(self, conditions=None, hints=None):
        '''Returns a iterator of the tuples in the set
           where conditions is a position:value mapping
        '''
        if hints and 'makeindex' in hints:
            makecache = hints['makeindex']
        else:
            makecache = None

        if self.cache is not None and makecache == self.cachekey:
            assert len(conditions)==1 and makecache in conditions
            for row in self.cache.get(conditions[makecache], ()):
                yield row
            return
        elif makecache is not None:
            cache = {} #no cache or cache for different key so create a new cache

        if 0:#self.debug:
            rows = list(self.generator())
            print 'SimpleTupleset',hex(id(self)), '('+self.op+')', \
                            '(before filter) on', self.hint, 'results', rows
        else:
            rows = self.generator()

        colmap = self.colmap
        for row in rows:
            if makecache is not None:
                key =row[makecache]
                cache.setdefault(key,[]).append(row)

            match = row
            if conditions:
                for pos, test in conditions.iteritems():
                    if row[pos] != test:
                        match = None
                        break #no match

            if match is not None:
                if colmap:
                    row = tuple(colmap(row))
                yield row

        #only set the cache for future use until after we've iterated through
        #all the rows
        if makecache is not None:
            self.cache = cache
            self.cachekey = makecache

    def __repr__(self):
        return 'SimpleTupleset ' + hex(id(self)) + ' for ' + self.op

    def explain(self, out, indent=''):
        print >>out, indent, repr(self), _colrepr(self),'with:'
        indent += ' '*4
        if isinstance(self.hint, Tupleset):            
            self.hint.explain(out,indent)
        else:
            print >>out, self.hint

class MutableTupleset(list, Tupleset):
    '''
    Subclass list to give Tupleset interface
    '''
    columns = None

    def __init__(self, columns=None, seq=()):        
        self.columns = columns
        return list.__init__(self, [row for row in seq])
    
    def filter(self, conditions=None, hints=None):
        '''Returns a iterator of the tuples in the set
           where conditions is a position:value mapping
        '''                
        for row in self:
            if conditions:
                for pos, test in conditions.iteritems():
                    if row[pos] != test:
                        break #no match
                else:
                    yield row
            else:
                yield row
    
    def size(self):
        return len(self)
    
    def __repr__(self):
        #return 'MutableTupleset('+repr(self.columns)+','+ list.__repr__(self)+')'
        if self.columns:
            return 'MutableTupleset'+repr(self.columns)
        else:
            return 'MutableTupleset'+ list.__repr__(self)
                 
def joinTuples(tableA, tableB, joinFunc):
    '''
    given two tuple sets and join function
    yield an iterator of the resulting rows
    '''
    lastRowA = None
    for rowA in tableA:
        for resultRow in joinFunc(rowA, tableB, lastRowA):
            if resultRow is not None:
                yield rowA, resultRow
            lastRowA = rowA, resultRow

def crossJoin(rowA,tableB,lastRowA):
    '''cross join'''
    for row in tableB:
        yield row
    
class Join(base.Tupleset):
    '''
    Corresponds to an join of two tuplesets
    Can be a inner join or right outer join, depending on joinFunc
    '''
    def __init__(self, left, right, joinFunc=crossJoin, columns=None,
                                                    msg='', debug=False):
        self.left = left
        self.right = right
        self.joinFunc = joinFunc
        self.columns = columns
        self.msg = msg
        self.debug = debug
        if debug:
            self._filter = self.filter
            self.filter = self._debugFilter
            self.debug = debug

    def _debugFilter(self, conditions=None, hints=None):
        results = tuple(self._filter(conditions, hints))        
        print >>self.debug, self.__class__.__name__,hex(id(self)), '('+self.msg+')', 'on', \
            (self.left, self.right), 'cols', _colrepr(self), 'filter:', repr(conditions), 'results:'
        pprint.pprint(results, self.debug)
        [validateRowShape(self.columns, r) for r in results]
        for row in results:
            yield row

    def getJoinType(self):
        return self.joinFunc.__doc__

    def __repr__(self):
        return self.__class__.__name__+' '+ hex(id(self))+' with: '+(self.msg
            or self.getJoinType())

    def explain(self, out, indent=''): 
        print >>out, indent, repr(self), _colrepr(self)
        indent += ' '*4
        self.left.explain(out,indent)
        self.right.explain(out,indent)        
            
class IterationJoin(Join):
    '''
    Corresponds to an join of two tuplesets
    Can be a inner join or right outer join, depending on joinFunc
    '''
        
    def filter(self, conditions=None, hints=None):
        for left, right in joinTuples(self.left, self.right, self.joinFunc):
            row = left + right
            if conditions:
                for key, value in conditions.iteritems():
                    if flatten(row[key]) != value: #XXX
                        #print '@@@@skipped@@@', row[key], '!=', repr(value), flatten(row[key])
                        break
                else:
                    yield row                
            else:
                yield row

class MergeJoin(Join):
    '''
    Assuming the left and right tables are ordered by the columns 
    used by the join condition, do synchronized walk through of each table.
    '''
        
    def __init__(self, left, right, lpos, rpos, msg=''):
        self.left = left
        self.right = right
        self.leftJoinSlice = lpos
        self.rightJoinSlice = rpos
        self.msg = msg
        
    def _filter(self, conditions=None):
        li = iter(self.left)
        ri = iter(self.right)
        lpos = self.leftJoinSlice 
        rpos=self.rightJoinSlice
        
        l = li.next(); r = ri.next()
        while 1:        
            while l[lpos] < r[rpos]:
                l = li.next() 
            while r[rpos] < l[lpos]:
                r = ri.next()        
            if l[lpos] == r[rpos]:
                #inner join 
                if conditions:
                    row = l + r
                    for key, value in conditions.iteritems():
                        if row[key] != value:
                            break
                    else:
                        yield l, r
                else:
                    yield l, r
                l = li.next();
    
    def filter(self, conditions=None, hints=None):
        for left, right in self._filter(conditions):
            yield left+right

    def getJoinType(self):
        return 'ordered merge'

class Union(base.Tupleset):
    '''
    Corresponds to a nodeset containing nodes of different node types
    '''    
    def __init__(self, tuplesets=None,op='',unique=True, columns=None,
                                                    msg='', debug=False):
        tuplesets = tuplesets or []
        self.tuplesets = tuplesets #set of tuplesets
        self.unique = unique
        self.columns = columns
        self.msg = msg
        self.debug = debug
        if debug:
            self._filter = self.filter
            self.filter = self._debugFilter
            self.debug = debug

    def _debugFilter(self, conditions=None, hints=None):
        results = tuple(self._filter(conditions, hints))        
        print >>self.debug, self.__class__.__name__,hex(id(self)), '('+self.msg+')', 'on', \
            self.tuplesets, 'cols', _colrepr(self), 'filter:', repr(conditions), 'results:'
        pprint.pprint(results, self.debug)
        [validateRowShape(self.columns, r) for r in results]
        for row in results:
            yield row

    def filter(self, conditions=None, hints=None):
        if self.unique:
            index = set()
        else:
            index = None
        for tupleset in self.tuplesets:
            for row in tupleset.filter(conditions, hints):
                if index is None:
                    yield row
                    continue
                key = keyfunc(row) #hash(flatten(row, to=tuple))
                if key not in index:
                    index.add(key)
                    yield row

    def __repr__(self):
        return self.__class__.__name__+' '+ hex(id(self))+' with: '+ self.msg

    def explain(self, out, indent=''): 
        print >>out, indent, repr(self), _colrepr(self)
        indent += ' '*4
        for t in self.tuplesets:
            t.explain(out,indent)
