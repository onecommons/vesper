#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from vesper.query import *
from vesper.pjson import ParseContext
from vesper.utils import flattenSeq, flatten,debugp,debugp

#############################################################
########################   AST   ############################
#############################################################
def depthfirstsearch(root, descendPredicate = None):
    """
    Given a starting vertex, root, do a depth-first search.
    """
    import collections
    to_visit = collections.deque()    
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

def validateTree(*roots):
    for root in roots:
        if root.parent:
            assert root in root.parent.args, root
        root._validateArgs()
        for a in root.args:
            validateTree(a)

def flattenOp(args, opType):
    """
    use with associative ops: (a op b) op c := a op b op c
    """
    if isinstance(args, QueryOp):
        args = (args,)
    for a in args:
        if isinstance(a, opType):
            for i in flattenOp(a.args, opType):
                yield i
        else:
            yield a
            
class QueryOp(object):
    '''
    Base class for the AST.
    '''

    _parent = None
    args = ()
    labels = ()
    name = None
    value = None #evaluation results maybe cached here
    maybe = False
    functions = None
    fromConstruct = False
    saveValue = False
    
    def _setparent(self, parent_):
        parents = [id(self)]
        parent = parent_
        while parent:
            if id(parent) in parents:
                raise QueryException('loop!! anc %s:%s self %s:%s parent %s:%s'
 % (type(parent), id(parent), type(self), id(self), type(parent_), id(parent_)))
            parents.append(id(parent))
            parent = parent.parent
        if self._parent:
            self._parent.removeArg(self)
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
        if self.labels != other.labels:
            return False        
        return self.args == other.args

    def isIndependent(self, exclude=None):
        for a in self.args:
            if exclude and exclude(a):
                continue
            if not a.isIndependent(exclude):
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

    def getReprIndent(self):
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

        return indent

    def _getExtraReprArgs(self, indent):
        args = ''
        if self.name is not None:
            args = repr(self.name)
        if self.maybe:
            if args: args += ','
            args += 'maybe=True'
        return args

    def __repr__(self):
        if self.args:
            self._validateArgs()

        indent = self.getReprIndent() 

        extra = self._getExtraReprArgs(indent)
        if extra and self.args:
            extra= ', '+extra
        return (indent + self.__class__.__name__ + '(' + ','.join([repr(a) for a in self.args])
                + extra + ')')

    def _validateArgs(self):
        if not all(a.parent is self for a in self.args):
            print self.__class__, self.name, id(self)
            print 'has child(ren) with wrong parent:'
            debugp([(a.__class__, a.name, id(a)) for a in self.args if a.parent is not self])
            print 'wrong parent(s):'
            debugp([(a.parent.__class__, getattr(a,'name', None), id(a.parent)) for a in self.args if a.parent is not self])
        assert all(a.parent is self for a in self.args), 'op has children with wrong parent'

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

    def isDescendentOf(self, testOp):
        ancestor = self
        while ancestor:
            if ancestor is testOp:
                return True
            ancestor = ancestor.parent
        return False

    def appendArg(self, arg):
        assert isinstance(arg, QueryOp)
        self.args.append(arg)
        arg.parent = self

    def replaceArg(self, child, with_):
        if not isinstance(self.args, list):
            raise QueryException('invalid operation replaceArg for %s' % type(self))
        for i, a in enumerate(self.args):
            if a is child:                
                self.args[i] = with_                
                if with_.parent and with_.parent is not self:
                    with_.parent.removeArg(with_)
                with_.parent = self
                child._parent = None
                return
        raise QueryException('invalid operation for %s' % type(self))

    def removeArg(self, child):
        try:
            #can't use list.remove() because there might be duplicates
            found = False
            for i, a in enumerate(self.args):
                if a is child:
                    self.args.pop(i)
                    found = True
            if found:
                child._parent = None
            else:
                raise QueryException(
                'removeArg failed on %r: could not find child %c'
                % (type(self), type(child)))
        except:
            print 'self', type(self), self
            raise QueryException('invalid operation: removeArg on %r' % type(self))

    def getLabel(self, label):
        for (name, p) in self.labels:
            if name == label:
                return p
        return None

    def _resolveNameMap(self, parseContext):
        pass #no op by default
        
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
        self.maybe = kw.get('maybe', False)
        for a in args:
            self.appendArg(a)

    #def _setname(self, name):
    #    if self.labels:
    #        assert len(self.labels) == 1
    #        if name:
    #            self.labels[0] = (name,0)
    #        else:
    #            del self.labels[0]
    #    elif name:
    #        self.labels.append( (name,0) )
    #
    #name = property(lambda self: self.labels and self.labels[0][0] or None,
    #             _setname)

    def appendArg(self, op):
        if isinstance(op, (Filter,ResourceSetOp)):
            op = JoinConditionOp(op)
        elif not isinstance(op, JoinConditionOp):
            raise QueryException('bad ast')
        return QueryOp.appendArg(self, op)

    def getType(self):
        return Resourceset

    def addLabel(self, label, position=SUBJECT):
        for child in self.breadthfirst():
            if isinstance(child, Filter):
                child.addLabel(label, position)
                return True
        return False
        #raise QueryException('unable to add label ' + label + ' to empty join: %s' % self)

    def _getExtraReprArgs(self, indent):
        args = ''
        if self.name is not None:
            args += 'name='+repr(self.name)
        if self.maybe:
            args += 'maybe=True'
        return args

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
    describe how the child filter participates in the parent join
    '''
    #join types:
    INNER = 'i'
    LEFTOUTER = 'l'
    RIGHTOUTER = 'r'
    #FULLOUTER = 'f'
    ANTI = 'a'
    SEMI = 's'
    CROSS = 'x'

    def __init__(self, op, position=SUBJECT, join=INNER, leftPosition=SUBJECT):
        self.op = op
        op.parent = self
        self.join = join
        self.setJoinPredicate(position)
        if isinstance(self.position, int):
            #assert isinstance(op, Filter), 'pos %i but not a Filter: %s' % (self.position, type(op))
            label = "#%d" % self.position
            op.addLabel(label, self.position)
            self.position = label
        self.leftPosition = leftPosition

    name = property(lambda self: '%s:%s:%s' % (isinstance(self.position, QueryOp)
      and self.position.name or str(self.position),self.join,self.leftPosition) )
    args = property(lambda self: isinstance(self.position, QueryOp)
                            and (self.op, self.position) or (self.op,))
    
    def setJoinPredicate(self, position):
        if isinstance(position, QueryOp):
            if isinstance(position, Eq):
                def getPosition(op, other):
                    if op == Project(SUBJECT):
                        if isinstance(other, Project):
                           return other.name #will be a position or label
                    elif isinstance(op, Label):
                        if isinstance(other, Project):
                            if other.isPosition():
                                for name, pos in position.parent.labels:
                                    if other.name == pos:
                                        return name
                            else:
                                return other.name
                    return None

                pos = getPosition(position.left, position.right)
                if pos is None:
                    pos = getPosition(position.right, position.left)
                if pos is not None:
                    self.position = pos
                    #self.appendArg(pred)
                    return
            if self.join == self.CROSS:
                assert isinstance(position, Filter)
                self.position = position
                position.parent = self
            else:
                raise QueryException('only equijoin supported for now')
        else:
            self.position = position #index or label
            #self.appendArg(Eq(Project(SUBJECT),Project(self.position)) )

    def _resolveNameMap(self, parseContext):
        if isinstance(self.position, (str, unicode)):
            self.position = parseContext.parseProp(self.position)
        
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

    def removeArg(self, child):
        if self.op is child:            
            if self.parent:
                #dont want to allow "dangling" join conditions so remove this op
                self.parent.removeArg(self)
            else:
                self.op = None
            
    def replaceArg(self, child, with_):
        if child is with_:
            return
        if self.op is child:
            self.op = with_
            if with_.parent and with_.parent is not self:
                with_.parent.removeArg(with_)
            with_.parent = self
            child._parent = None
            return
        print 'op', self.op, 'replace',child, 'with', with_
        raise QueryException('invalid operation for %s' % type(self))

    def _getExtraReprArgs(self, indent):
        import re
        if isinstance(self.position, QueryOp):
            return indent + '  ' + repr(self.join)+','+repr(self.leftPosition)

        match = re.match(r'#(\d)', self.position) #hack!
        if match:
            args = match.group(1) #retrieve number
        else:
            args = repr(self.position)
        return indent + '  ' + args +','+repr(self.join)+','+repr(self.leftPosition)

    #Note: __eq__ not needed since extra attributes are encoded in name property 

class Filter(QueryOp):
    '''
    Filters rows out of a tupleset based on predicate
    '''

    #true if it references more than one project (set during analysis)
    complexPredicates = False

    def __init__(self, *args, **kw):

        self.args = []
        self.labels = []
        for a in args:
            self.appendArg(a)

        self.labels = []
        if 'subjectlabel' in kw:
            subjectlabel = kw['subjectlabel']
            if not isinstance(subjectlabel, list):
                subjectlabel = (subjectlabel, )
            for name in subjectlabel:
                self.labels.append( (name, SUBJECT) )
        if 'propertylabel' in kw:
            propertylabel = kw['propertylabel']
            if not isinstance(propertylabel, list):
                propertylabel = (propertylabel, )
            for name in propertylabel:
                self.labels.append( (name, PROPERTY) )
        if 'objectlabel' in kw:
            objectlabels = kw['objectlabel']
            if not isinstance(objectlabels, list):
                objectlabels = (objectlabels, )
            for objectlabel in objectlabels:
                self.labels.append( (objectlabel, OBJECT) )
                self.labels.append( (objectlabel+':type', OBJTYPE_POS) )
                self.labels.append( (objectlabel+':pos', LIST_POS) )

        if 'saveValuelabel' in kw:
            saveValuelabel = kw['saveValuelabel']
            if not isinstance(saveValuelabel, list):
                saveValuelabel = (saveValuelabel, )
            for name in saveValuelabel:                
                self.labels.append( (name, LIST_POS+1) )

        if 'complexPredicates' in kw:
            self.complexPredicates = kw['complexPredicates']
 
    def getType(self):
        return Tupleset

    def addLabel(self, label, pos=SUBJECT):
        for (name, p) in self.labels:
            if name == label:
                if p == pos:
                    return
                else:
                    raise QueryException("label '%s' already used on "
                                        "different position: %s" % (label, p))
        self.labels.append( (label, pos) )
        if pos == OBJECT:
            if isinstance(label, tuple):
                prefix, name = label
                self.labels.append( ( (prefix, name +':type'), OBJTYPE_POS) )
                self.labels.append( ( (prefix, name +':pos'), LIST_POS) )
            else:
                self.labels.append( (label+':type', OBJTYPE_POS) )
                self.labels.append( (label+':pos', LIST_POS) )

    def removeLabel(self, label, pos):
        self.labels.remove( (label,pos) )
        if pos == OBJECT:
            self.labels.remove( (label+':type', OBJTYPE_POS) )
            self.labels.remove( (label+':pos', LIST_POS) )

    def labelFromPosition(self, pos):
        for (name, p) in self.labels:
            if p == pos:
                return name
        return None

    def _resolveNameMap(self, parseContext):
        def resolve(name):
            return parseContext.parseProp(name)

        self.labels = [ (resolve(name), p) for (name, p) in self.labels]

    def _getExtraReprArgs(self, indent):
        assert not self.maybe
        args = ''
        kws = {}
        for (name, pos) in self.labels:
            if pos <= 2:
                kwname = ['subjectlabel', 'propertylabel', 'objectlabel'][pos]
                kws.setdefault(kwname, []).append(name)

        if self.complexPredicates:
            kws['complexPredicates'] = self.complexPredicates
        if kws:
            args = indent+'  '+ (', ').join( [kw+'='+repr(vals)
                                              for kw, vals in kws.items()] )
        return args

class Label(QueryOp):

    def __init__(self, name, maybe=False):
        self.name = name
        self.maybe = maybe

    def isIndependent(self, exclude=None):
        return False

class BindVar(QueryOp):
    def __init__(self, name, maybe=False):
        self.name = name
        self.maybe = maybe

class Constant(QueryOp):
    '''
    '''

    def __init__(self, value, datatype = None, maybe=False):
        if not isinstance( value, QueryOpTypes):
            #coerce
            if isinstance(value, str):
                value = unicode(value, 'utf8')
            elif isinstance(value, (int, long)):
                value = float(value)
            elif isinstance(value, type(True)):
                value = bool(value)
        self.value = value
        self.datatype = datatype #not used
        self.maybe = maybe

    def getType(self):
        if isinstance(self.value, QueryOpTypes):
            return type(self.value)
        else:
            return ObjectType

    def _resolveNameMap(self, parseContext):
        if isinstance(self.value, ResourceUri):
            self.value = ResourceUri( parseContext.parseId(self.value.uri) )
        #XXX: elif parseContext.datatypemap ?
        if self.datatype:   #XXX pjson should expand datatype names as well
            self.datatype = parseContext.parseProp(self.datatype)

    def __eq__(self, other):
        return super(Constant,self).__eq__(other) and self.value == other.value

    def __repr__(self):
        indent = self.getReprIndent()
        args = repr(self.value)
        if self.datatype:
            args += ','+repr(self.datatype)
        if self.maybe:
            args += ',maybe=True'
        return indent + self.__class__.__name__ + "("+args+")"

    @classmethod
    def _costMethodName(cls):
        return 'cost'+ Constant.__name__

    @classmethod
    def _evalMethodName(cls):
        return 'eval'+ Constant.__name__

class PropString(Constant):
    def _resolveNameMap(self, parseContext):
        self.value = parseContext.parseProp(self.value)

class AnyFuncOp(QueryOp):

    def __init__(self, key=(), metadata=None, *args, **kw):
        self.name = key
        self.args = []
        for a in args:
            self.appendArg(a)
        self.maybe = kw.get('maybe', False)
        self.metadata = metadata or self.defaultMetadata

    def getType(self):
        return self.metadata.type

    def isIndependent(self, exclude=None):
        independent = super(AnyFuncOp, self).isIndependent(exclude)
        if independent: #the args are independent
            return self.metadata.isIndependent
        else:
            return False
    
    def isAggregate(self):
        return self.metadata.isAggregate

    def cost(self, engine, context):
        return engine.costAnyFuncOp(self, context)

    def evaluate(self, engine, context):
        return engine.evalAnyFuncOp(self, context)

    def execFunc(self, context, *args, **kwargs):
        if self.metadata.needsContext:            
            return self.metadata.func(context, *args, **kwargs)
        else:
            return self.metadata.func(*args, **kwargs)

    def __repr__(self):
        self._validateArgs()
        if self.args:
            argsRepr = ','+ ','.join([repr(a) for a in self.args])
        else:
            argsRepr = ''
        if self.maybe:
            argsRepr += ',maybe=True'
        indent = self.getReprIndent()
        return indent+ 'QueryOp.functions.getOp('+str(self.name) + argsRepr + ')'

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
    def __init__(self, *args, **kw):
        self.args = []
        for a in args:
            self.appendArg(a)
        self.maybe = kw.get('maybe', False)

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
        Order of args do not matter because op is communitive
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
        self._validateArgs()
        indent = self.getReprIndent()
        return indent+'Cmp('+repr(self.op)+','+','.join([repr(a) for a in self.args])+')'

class Eq(CommunitiveBinaryOp):
    def __init__(self, left=None, right=None):
        return super(Eq, self).__init__(left, right)

class In(BooleanOp):
    '''Like OrOp + EqOp but the first argument is only evaluated once'''

class Not(BooleanOp):
    pass
    
class QueryFuncMetadata(object):
    factoryMap = { StringType: StringFuncOp, NumberType : NumberFuncOp,
      BooleanType : BooleanFuncOp
      }

    def __init__(self, func, type=None, opFactory=None, isIndependent=True,
            costFunc=None, needsContext=False, lazy=False, checkForNulls=0,
            isAggregate=False, initialValue=None, finalFunc=None):
        self.func = func
        self.type = type or ObjectType
        self.isIndependent = isIndependent
        self.opFactory  = opFactory or self.factoryMap.get(self.type, AnyFuncOp)
        self.costFunc = costFunc
        self.needsContext = needsContext or lazy
        self.lazy = lazy
        self.isAggregate = isAggregate
        self.initialValue = initialValue
        self.finalFunc = finalFunc
        self.checkForNulls = checkForNulls

AnyFuncOp.defaultMetadata = QueryFuncMetadata(None)

class Project(QueryOp):      

    def __init__(self, fields, var=None, constructRefs = None, maybe=False):
        self.varref = var 
        if not isinstance(fields, list):
            self.fields = [ fields ]
        else:
            self.fields = fields
        self.constructRefs = constructRefs #expand references to objects
        self.maybe = maybe

    name = property(lambda self: self.fields[-1]) #name or '*'
    
    def isPosition(self):
        return isinstance(self.name, int)

    def _resolveNameMap(self, nsmap):
        def resolve(name):
            if isinstance(name, (str, unicode)):
                return nsmap.parseProp(name)
            return name

        self.fields = [resolve(name) for name in self.fields]

    def isIndependent(self, exclude=None):
        return False

    def __eq__(self, other):
        return (super(Project,self).__eq__(other)
            and self.fields == other.fields and self.varref == other.varref)

    def __repr__(self):
        indent = self.getReprIndent()
        return (indent+"Project(" + repr(self.fields) + ','+repr(self.varref)+
                ','+repr(self.constructRefs)+','+ repr(self.maybe) +')' )

class PropShape(object):
    omit = 'omit'
    usenull= 'usenull'
    uselist = 'uselist' #when [] specified
    nolist = 'nolist'

class ConstructProp(QueryOp):
    nameFunc = None
    hasAggFunc = False
    
    def __init__(self, name, value, ifEmpty=PropShape.usenull,
                ifSingle=PropShape.nolist,nameIsFilter=False, nameFunc=False):
        self.name = name #if name is None (and needed) derive from value (i.e. Project)
        if nameFunc:
            self.nameFunc = nameFunc
            nameFunc.parent = self
        
        self.appendArg(value)
        
        assert ifEmpty in (PropShape.omit, PropShape.usenull, PropShape.uselist)
        self.ifEmpty = ifEmpty
        assert ifSingle in (PropShape.nolist, PropShape.uselist)
        self.ifSingle = ifSingle
        self.nameIsFilter = nameIsFilter
        self.projects = []

    def appendArg(self, value):
        if isinstance(value, Project):
            #hack: if this is a standalone project expand object references
            #but not for id
            if value.name != 0 and value.constructRefs is None:
                value.constructRefs = True
        self.value = value #only one, replaces current if set
        value.parent = self

    def removeArg(self, child):
        if self.value is child:
            self.value = None
            child._parent = None
        else:
            raise QueryException('removeArg failed: not a child')

    def replaceArg(self, child, with_):
        if self.value is child:
            child._parent = None
            self.appendArg(with_)
            return
        raise QueryException('replaceArg failed: not a child')

    def _resolveNameMap(self, parseContext):
        return self.nameFunc

    def __eq__(self, other):
        return (super(ConstructProp,self).__eq__(other)
         and self.ifEmpty == other.ifEmpty and self.ifSingle == self.ifSingle)
         #and self.nameIsFilter == other.nameIsFilter)

    args = property(lambda self: tuple([a for a in [self.value, self.nameFunc] if a]))

    def __repr__(self):
        indent = self.getReprIndent()
        return (indent + "ConstructProp(" + ','.join([repr(x) for x in
            [self.name, self.value,self.ifEmpty,self.ifSingle, self.nameIsFilter,
            self.nameFunc] ]) +')' )

class ConstructSubject(QueryOp):
    def __init__(self, name='id', value=None):
        self.name = name        
        if value: #could be a string
            if not isinstance(value, QueryOp):
                value = Label(value)
            value.parent = self
        self.value = value

    def appendArg(self, op):
        if isinstance(op, Label):
            self.value = op #only one, replaces current if set
        else:
            raise QueryException('bad ast: ConstructSubject doesnt take %s' % type(op))
        op.parent = self

    def getLabel(self):
        if self.value:
            return self.value.name
        else:
            return ''

    args = property(lambda self: self.value and (self.value,) or ())

    def __repr__(self):
        indent = self.getReprIndent()
        return (indent+"ConstructSubject(" + repr(self.name)+','+ repr(self.value) +')' )


class Construct(QueryOp):
    '''
    '''
    dictShape= dict
    listShape= list
    valueShape = object
    offset = None
    limit = None
    id = None
    hasAggFunc = False
    
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

    def appendArg(self, op):
        assert isinstance(op, QueryOp), op
        if not isinstance(op, (ConstructSubject, ConstructProp)):
            op = ConstructProp(None, op)
        super(Construct, self).appendArg(op)
        
    def __eq__(self, other):
        return (super(Construct,self).__eq__(other)
            and self.shape == other.shape and self.id == self.id)

    def __repr__(self):
        indent = self.getReprIndent()
        extra = ''
        if self.shape == self.listShape:
            extra = ',list'
        elif self.shape == self.valueShape:
            extra = ',object'
        return indent+"Construct(" + repr(self.args)+ extra +')'
        
class GroupBy(QueryOp):
    aggregateShape = None
    
    def __init__(self, arg, **options):
        assert isinstance(arg, (Project, Label))
        self.args = []
        self.appendArg(arg)

    name = property(lambda self: self.args[0].name)

class OrderBy(QueryOp):    
    def __init__(self, *args):
        self.args = []
        for arg in args:
            if not isinstance(arg, SortExp):
                arg = SortExp(arg)
            self.appendArg(arg)

class SortExp(QueryOp):    
    def __init__(self, expression, desc=False):
        self.exp = expression
        expression.parent = self
        self.asc = not desc

    args = property(lambda self: (self.exp,))

    def _getExtraReprArgs(self, indent):
        return repr(not self.asc)

class Select(QueryOp):
    where = None
    groupby = None
    orderby=None
    skipEmbeddedBNodes = False    

    def __init__(self, construct, where=None, groupby=None, limit=None, 
        offset=None, depth=None, namemap=None, orderby=None, mergeall=False):
        self.appendArg(construct)
        if where:
            self.appendArg(where)
        if groupby:
            self.appendArg(groupby)
        if orderby:
            self.appendArg(orderby)
        self.offset = offset
        self.limit = limit
        self.depth = depth
        self.namemap = namemap
        self.mergeall = mergeall

    def appendArg(self, op):
        if (isinstance(op, ResourceSetOp)): 
            self.where = op #only one, replaces current if set
        elif (isinstance(op, Construct)):
            self.construct = op #only one, replaces current if set
        elif (isinstance(op, GroupBy)):
            self.groupby = op #only one, replaces current if set
        elif (isinstance(op, OrderBy)):
            self.orderby = op #only one, replaces current if set
        else:
            raise QueryException('bad ast: Select doesnt take %s' % type(op))
        op.parent = self

    args = property(lambda self: tuple([a for a in [self.construct, self.where,
                                             self.groupby, self.orderby] if a]))

    def removeArg(self, child):
        if self.where is child:
            self.where = None
            child._parent = None
        else:
            raise QueryException('removeArg failed: not a child')

    def __repr__(self):
        indent = self.getReprIndent()
        args = repr(self.construct)
        for name in ('where', 'groupby', 'limit', 'offset', 'depth',
            'namemap', 'orderby', 'mergeall'):
            v = getattr(self, name)
            if v is not None:
                args += ',\n' + indent + name + '=' + repr(v)

        return indent+"Select(" + args + ')'
