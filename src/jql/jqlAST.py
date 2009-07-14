from jql import *
from rx.utils import flattenSeq, flatten

#############################################################
########################   AST   ############################
#############################################################

#define the AST syntax using Zephyr Abstract Syntax Definition Language.
#(see http://www.cs.princeton.edu/~danwang/Papers/dsl97/dsl97-abstract.html)
#If the AST gets more complicated we could write a code generator using
#http://svn.python.org/view/python/trunk/Parser/asdl.py

syntax = '''
-- Zephyr ASDL's five builtin types are identifier, int, string, object, bool

module RxPathQuery
{
    exp =  boolexp | AnyFunc(exp*) | Query(subquery)

    subquery = Filter(subquery input?, boolexp* subject,
                    boolexp* predicate, boolexp* object) |
               Join(subquery left, subquery right) |
               Project(subquery input, column id)  |
               Union(subquery left, subquery right)

    boolexp = BoolFunc(exp*)
    -- nodesetfunc, eqfunc, orfunc, andfunc
}
'''

def depthfirstsearch(root, descendPredicate = None, visited = None):
    """
    Given a starting vertex, root, do a depth-first search.
    """
    import collections
    to_visit = collections.deque()
    if visited is None:
        visited = set()

    to_visit.append(root) # Start with root
    while len(to_visit) != 0:
        v = to_visit.pop()
        if id(v) not in visited:
            visited.add( id(v) )
            yield v
            if not descendPredicate or descendPredicate(v):                
                to_visit.extend(v.args)


def findfirstdiff(op1, op2):
    import itertools

    l1 = list(op1.depthfirst())
    l2 = list(op2.depthfirst())
    for (i1, i2) in itertools.izip(reversed(l1), reversed(l2)):
        if not (i1 == i2): #XXX i1 != i2 is wrong for Label, why?
            return i1, i2

    if len(l1) > len(l2):
        return (l1[len(l2):], None)
    elif len(l1) < len(l2):
        return (None, l2[len(l1):])
    else:
        return (None, None)

class QueryOp(object):
    '''
    Base class for the AST.
    '''

    _parent = None
    args = ()
    labels = ()
    name = None
    value = None #evaluation results maybe cached here

    def _setparent(self, parent_):
        parents = [id(self)]
        parent = parent_
        while parent:
            if id(parent) in parents:
                raise QueryException('loop!! anc %s:%s self %s:%s parent %s:%s'
 % (type(parent), id(parent), type(self), id(self), type(parent_), id(parent_)))
            parents.append(id(parent))
            parent = parent.parent

        self._parent = parent_
    
    parent = property(lambda self: self._parent, _setparent)

    @classmethod
    def _costMethodName(cls):
        return 'cost'+ cls.__name__

    @classmethod
    def _evalMethodName(cls):
        return 'eval'+ cls.__name__

    def getType(self):
        return ObjectType

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        return self.args == other.args

    def isIndependent(self):
        for a in self.args:
            if not a.isIndependent():
                return False
        return True

    def cost(self, engine, context):
        return getattr(engine, self._costMethodName())(self, context)

    def evaluate(self, engine, context):
        '''
        Given a context with a sourceModel, evaluate either modified
        the context's resultModel or returns a value to be used by a
        parent QueryOp's evaluate()
        '''
        return getattr(engine, self._evalMethodName())(self, context)

    def __repr__(self):
        indent = self.parent and '\n' or ''
        parent = self.parent
        parents = [id(self)]
        while parent:
            indent += '  '
            if id(parent) in parents:
                indent = 'LOOP!['+parent.__class__.__name__+']\n'
                break;
            parents.append(id(parent))
            parent = parent.parent
            
        if self.name is not None:
            name = self.name
            if isinstance(name, tuple): #if qname pair
                name = self.name[1]
            namerepr = ':'+ repr(name)
        else:
            namerepr = ''
        if self.args:
            assert all(a.parent is self for a in self.args), repr(self.__class__) + repr([(a.__class__, a.parent) for a in self.args if a.parent is not self])
            argsrepr = '(' + ','.join([repr(a) for a in self.args]) + ')'
        else:
            argsrepr = ''
        return (indent + self.__class__.__name__ + namerepr
                + (self.labels and repr(self.labels) or '')
                + argsrepr)

    def _siblings(self):
        if not self.parent:
            return []
        return [a for a in self.parent.args if a is not self]
    siblings = property(_siblings)

    def depthfirst(self, descendPredicate=None):
        '''
        yield descendants depth-first (pre-order traversal)
        '''
        for n in depthfirstsearch(self, descendPredicate):
            yield n

    def _bydepth(self,level=0):
        for a in self.args:
            for descend, lvl in a._bydepth(level+1):
                yield descend, lvl
        yield self, level

    def breadthfirst(self, deepestFirst=False, includeLevel=False):
        '''
        yield descendants (and self) by ordered by level
        if deepestFirst = True, yield deepest level first
        if includeLevel = True, yield (node, level) pairs 
        '''
        return [includeLevel and i or i[0] for i in
            sorted(self._bydepth(), key=lambda k:k[1], reverse=deepestFirst)]

    def appendArg(self, arg):
        self.args.append(arg)
        arg.parent = self

class ErrorOp(QueryOp):
    def __init__(self, args, name=''):
        if not isinstance(args, (list, tuple)):
            args = (args,)
        self.args = args
        self.name = "Error " + name

class ResourceSetOp(QueryOp):
    '''
    These operations take one or more tuplesets and return a resource set.
    '''

    def __init__(self, *args, **kw):
        '''

        keywords:
        join: tuple

        '''
        self.args = []
        self.labels = []
        self.name = kw.get('name')
        for a in args:
            self.appendArg(a)

    def _setname(self, name):
        if self.labels:
            assert len(self.labels) == 1
            if name:
                self.labels[0] = (name,0)
            else:
                del self.labels[0]
        elif name:
            self.labels.append( (name,0) )

    #name = property(lambda self: self.labels and self.labels[0][0] or None,
    #             _setname)

    def appendArg(self, op):
        if isinstance(op, (Filter,ResourceSetOp)):
            op = JoinConditionOp(op)
        elif not isinstance(op, JoinConditionOp):
            raise QueryException('bad ast')
        QueryOp.appendArg(self, op)

    def getType(self):
        return Resourceset

class Join(ResourceSetOp):
    '''
    handles "and"
    '''

class Union(ResourceSetOp):
    '''
    handles "or"
    '''

class Except(ResourceSetOp):
    '''
    handles 'not'
    '''

class JoinConditionOp(QueryOp):
    '''
    helper op
    '''
    INNER = 'inner'
    RIGHTOUTER = 'right outer'

    def __init__(self, op, position=SUBJECT, join=INNER):
        self.op = op
        self.args = []
        self.appendArg(op)
        self.setJoinPredicate(position)
        self.join = join
        if isinstance(self.position, int):
            assert isinstance(op, Filter), 'pos %i but not a Filter: %s' % (self.position, type(op))
            label = "#%d" % self.position
            op.addLabel(label, self.position)
            self.position = label

    name = property(lambda self: '%s:%s' % (str(self.position),self.join) )

    def setJoinPredicate(self, position):
        if isinstance(position, QueryOp):
            if isinstance(position, Eq):
                if position.left == Project(SUBJECT):
                    pred = position.right
                elif position.right == Project(SUBJECT):
                    pred = position.left
                else:
                    pred = None
                if pred and isinstance(pred, Project):
                    self.position = pred.name #position or label
                    #self.appendArg(pred)
                    return
            raise QueryException('only equijoin supported for now')
        else:
            self.position = position #index or label
            #self.appendArg(Eq(Project(SUBJECT),Project(self.position)) )

    def getPositionLabel(self):
        if isinstance(self.position, int):
            return ''
        else:
            return self.position

    def resolvePosition(self, throw=True):
        '''
        Return the column index for the join
        To handle joins on labels, this needs to be called after the underlying
        op is evaluated.
        '''
        if not isinstance(self.position, int):
            #print 'resolve', str(self.op), repr(self.op.labels)
            for name, pos in self.op.labels:
                if self.position == name:
                    #print 'found', name, pos
                    return pos

            if throw:                
                raise QueryException('unknown label ' + self.position)
            else:
                return None
        else:
            return self.position


class Filter(QueryOp):
    '''
    Filters rows out of a tupleset based on predicate
    '''

    def __init__(self, *args, **kw):

        self.args = []
        self.labels = []
        for a in args:
            self.appendArg(a)

        self.labels = []
        if 'subjectlabel' in kw:
            self.labels.append( (kw['subjectlabel'], SUBJECT) )
        if 'propertylabel' in kw:
            self.labels.append( (kw['propertylabel'], PROPERTY) )
        if 'objectlabel' in kw:
            self.labels.append( (kw['objectlabel'], OBJECT) )
 
    def getType(self):
        return Tupleset

    def addLabel(self, label, pos):
        for (name, p) in self.labels:
            if name == label:
                if p == pos:
                    return
                else:
                    raise QueryException("label already used " + label)
        self.labels.append( (label, pos) )

class Label(QueryOp):

    def __init__(self, name):
        self.name = name

    def isIndependent(self):
        return False

class Constant(QueryOp):
    '''
    '''

    def __init__(self, value):
        if not isinstance( value, QueryOpTypes):
            #coerce
            if isinstance(value, str):
                value = unicode(value, 'utf8')
            elif isinstance(value, (int, long)):
                value = float(value)
            elif isinstance(value, type(True)):
                value = bool(value)
        self.value = value

    def getType(self):
        if isinstance(self.value, QueryOpTypes):
            return type(self.value)
        else:
            return ObjectType

    def __eq__(self, other):
        return super(Constant,self).__eq__(other) and self.value == other.value

    def __repr__(self):
        return repr(self.value)

class AnyFuncOp(QueryOp):

    def __init__(self, key=(), metadata=None, *args):
        self.name = key
        self.args = []
        for a in args:
            self.appendArg(a)
        self.metadata = metadata or self.defaultMetadata

    def getType(self):
        return self.metadata.type

    def isIndependent(self):
        independent = super(AnyFuncOp, self).isIndependent()
        if independent: #the args are independent
            return self.metadata.isIndependent
        else:
            return False
    
    #def __repr__(self):
    #    if self.name:
    #        name = self.name[1]
    #    else:
    #        raise TypeError('malformed FuncOp, no name')
    #    return name + '(' + ','.join( [repr(a) for a in self.args] ) + ')'

    def cost(self, engine, context):
        return engine.costAnyFuncOp(self, context)

    def evaluate(self, engine, context):
        return engine.evalAnyFuncOp(self, context)

class NumberFuncOp(AnyFuncOp):
    def getType(self):
        return NumberType

class StringFuncOp(AnyFuncOp):
    def getType(self):
        return StringType

class BooleanFuncOp(AnyFuncOp):
    def getType(self):
        return BooleanType

class BooleanOp(QueryOp):

    def __repr__(self):
        #assert all(a.parent is self for a in self.args), repr(self.__class__) + repr([a.parent for a in self.args if a.parent is not self])
        if not self.args:
            return self.name + '()'        
        elif len(self.args) > 1:
            return '(' + self.name.join( [repr(a) for a in self.args] ) + ')'
        else:
            return self.name + '(' +  repr(self.args[0]) + ')'

    def getType(self):
        return BooleanType

class CommunitiveBinaryOp(BooleanOp):

    left = property(lambda self: self.args[0])
    right = property(lambda self: len(self.args) > 1 and self.args[1] or None)

    def __init__(self, left=None, right=None):
        self.args = []
        if left is not None:
            if not isinstance(left, QueryOp):
                left = Constant(left)
            self.appendArg(left)
            if right is not None:
                if not isinstance(right, QueryOp):
                    right = Constant(right)
                self.appendArg(right)

    def __eq__(self, other):
        '''
        Order of args don't matter because op is communitive
        '''
        if type(self) != type(other):
            return False
        if self.left == other.left:
            return self.right == other.right
        elif self.left == other.right:
            return self.right == other.left

class And(CommunitiveBinaryOp):
    name = ' and '

class Or(CommunitiveBinaryOp):
    name = ' or '

class Cmp(CommunitiveBinaryOp):

    def __init__(self, op, *args):
        self.op = op
        return super(Cmp, self).__init__(*args)

    def __repr__(self):
        op = self.op
        assert all(a.parent is self for a in self.args)
        return '(' + op.join( [repr(a) for a in self.args] ) + ')'

class Eq(CommunitiveBinaryOp):
    def __repr__(self):
        assert all(a.parent is self for a in self.args)
        return '(' + ' == '.join( [repr(a) for a in self.args] ) + ')'

class In(BooleanOp):
    '''Like OrOp + EqOp but the first argument is only evaluated once'''
    def __repr__(self):
        rep = repr(self.args[0]) + ' in ('
        return rep + ','.join([repr(a) for a in self.args[1:] ]) + ')'

class IsNull(BooleanOp):
    def __repr__(self):
        return repr(self.args[0]) + ' is null '

class Not(BooleanOp):
    def __repr__(self):
        return 'not(' + ','.join( [repr(a) for a in self.args] ) + ')'

class QueryFuncMetadata(object):
    factoryMap = { StringType: StringFuncOp, NumberType : NumberFuncOp,
      BooleanType : BooleanFuncOp
      }

    def __init__(self, func, type=None, opFactory=None, isIndependent=True,
                                                             costFunc=None):
        self.func = func
        self.type = type or ObjectType
        self.isIndependent = isIndependent
        self.opFactory  = opFactory or self.factoryMap.get(self.type, AnyFuncOp)
        self.costFunc = costFunc

AnyFuncOp.defaultMetadata = QueryFuncMetadata(None)

class QueryFuncs(object):

    SupportedFuncs = {
        (EMPTY_NAMESPACE, 'true') :
          QueryFuncMetadata(lambda *args: True, BooleanType, None, True,
                            lambda *args: 0),
        (EMPTY_NAMESPACE, 'false') :
          QueryFuncMetadata(lambda *args: False, BooleanType, None, True,
                            lambda *args: 0),
    }

    def addFunc(self, name, func, type=None, cost=None):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        if cost is None or callable(cost):
            costfunc = cost
        else:
            costfunc = lambda *args: cost
        self.SupportedFuncs[name] = QueryFuncMetadata(func, type, costFunc=costfunc)

    def getOp(self, name, *args):
        if isinstance(name, (unicode, str)):
            name = (EMPTY_NAMESPACE, name)
        funcMetadata = self.SupportedFuncs[name]
        return funcMetadata.opFactory(name,funcMetadata,*args)

qF = QueryFuncs() #todo: SupportedFuncs should be per query engine and schema handler
qF.addFunc('add', lambda a, b: float(a)+float(b), NumberType)
qF.addFunc('sub', lambda a, b: float(a)-float(b), NumberType)
qF.addFunc('mul', lambda a, b: float(a)*float(b), NumberType)
qF.addFunc('div', lambda a, b: float(a)/float(b), NumberType)
qF.addFunc('mod', lambda a, b: float(a)%float(b), NumberType)
qF.addFunc('negate', lambda a: -float(a), NumberType)
#XXX not so lame isref
qF.addFunc('isref', lambda a: a and True or False, BooleanType)


class Project(QueryOp):  
    
    def __init__(self, fields, var=None):
        self.varref = var 
        if not isinstance(fields, (list,tuple)):
            if str(fields).lower() == 'id':
                fields = SUBJECT
            self.fields = [ fields ]
        else:
            self.fields = fields

    name = property(lambda self: self.fields[-1]) #name or '*'

    def isPosition(self):
        return isinstance(self.name, int)

    def _mutateOpToThis(self, label):
        '''
        yes, a terrible hack
        '''
        assert isinstance(label, QueryOp)
        label.__class__ = Project        
        label.fields = self.fields
        label.varref = self.varref

    def isIndependent(self):
        return False

    def __eq__(self, other):
        return (super(Project,self).__eq__(other)
            and self.fields == other.fields and self.varref == other.varref)

    def __repr__(self):
        varref = ''
        fields = ''
        if self.varref:
            varref = '('+self.varref + ')'
        if len(self.fields) > 1:
            fields = str(self.fields)
        return super(Project,self).__repr__()+varref+fields

class PropShape(object):
    omit = 'omit' #when MAYBE()
    usenull= 'usenull'
    uselist = 'uselist' #when [] specified
    nolist = 'nolist'

class ConstructProp(QueryOp):
    def __init__(self, name, value, ifEmpty=PropShape.usenull,
                ifSingle=PropShape.nolist,nameIsFilter=False):
        self.name = name #if name is None (and needed) derive from value (i.e. Project)
        self.appendArg(value)
        assert ifEmpty in (PropShape.omit, PropShape.usenull, PropShape.uselist)
        self.ifEmpty = ifEmpty
        assert ifSingle in (PropShape.nolist, PropShape.uselist)
        self.ifSingle = ifSingle
        self.nameIsFilter = nameIsFilter

    def appendArg(self, value):
        self.value = value #only one, replaces current if set
        value.parent = self

    def __eq__(self, other):
        return (super(ConstructProp,self).__eq__(other)
         and self.ifEmpty == other.ifEmpty and self.ifSingle == self.ifSingle)
         #and self.nameIsFilter == other.nameIsFilter)

    args = property(lambda self: (self.value,))

class ConstructSubject(QueryOp):
    def __init__(self, name='id', value=None):
        self.name = name        
        if value: #could be a string
            if not isinstance(value, QueryOp):
                value = Label(value)
            value.parent = self
        self.value = value

    def getLabel(self):
        if self.value:
            return self.value.name
        else:
            return ''

    args = property(lambda self: self.value and (self.value,) or ())

class Construct(QueryOp):
    '''
    '''
    dictShape= dict
    listShape= list
    offset = None
    limit = None
    id = None
    
    def __init__(self, props, shape=dictShape):
        self.args = []
        for p in props:
            self.appendArg(p)
            if isinstance(p, ConstructSubject):
                self.id = p
        if not self.id:
            self.id = ConstructSubject()
            self.appendArg(self.id)
        self.shape = shape

    def __eq__(self, other):
        return (super(Construct,self).__eq__(other)
            and self.shape == other.shape and self.id == self.id)

class Select(QueryOp):
    offset = None
    limit = None
    where = None

    def __init__(self, construct, where=None):
        self.appendArg(construct)
        if where:
            self.appendArg(where)

    def appendArg(self, op):
        if (isinstance(op, ResourceSetOp)): 
            self.where = op #only one, replaces current if set
        elif (isinstance(op, Construct)):
            self.construct = op #only one, replaces current if set
        else:
            raise QueryException('bad ast: Select doesnt take %s' % type(op))
        op.parent = self

    args = property(lambda self: [a for a in [self.construct, self.where] if a])

    def replaceArg(self, child, with_):
        if isinstance(child, Join):
            self.where = None
            return
        raise QueryException('invalid operation')
