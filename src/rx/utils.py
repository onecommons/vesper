"""
    General purpose utilities

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import os.path
import os, sys, types, re, copy
from stat import *
from time import *
from types import *
from binascii import unhexlify, b2a_base64
try:
    from hashlib import sha1
except ImportError:
    import sha
    sha1 = sha.new
    
class NotSetType(object):
    '''use when None is a valid value'''
    
NotSet = NotSetType()

_flattenTypes = (list,tuple, GeneratorType, type({}.iteritems()),
    type({}.itervalues()), type({}.iterkeys()))

def flattenSeq(seq, depth=0xFFFF, flattenTypes=None):
    '''
    >>> list(flattenSeq([ [1,2], 3, [4,5]]))
    [1, 2, 3, 4, 5]
    >>> list(flattenSeq([ [1,2], 3, [4,5]], 0 ))
    [[1, 2], 3, [4, 5]]
    >>>
    >>> list(flattenSeq([ [1,2], 3, [4,5]], 1 ))
    [1, 2, 3, 4, 5]
    >>>
    >>> list(flattenSeq([ [1,2], 3, [4,[5] ]], 1 ))
    [1, 2, 3, 4, [5]]
'''
    if flattenTypes is None:
        flattenTypes = _flattenTypes
    if not isinstance(seq, flattenTypes):
        yield seq
    else:
        for a in seq:
            if depth > 0:
                for i in flattenSeq(a, depth-1, flattenTypes):
                    yield i
            else:
                yield a

def flatten(seq, to=list, depth=0xFFFF, flattenTypes=None, keepSeq=False):
    '''
>>> flatten(1)
1
>>> flatten([1])
1
>>> flatten([1,2])
[1, 2]
>>> flatten([1], keepSeq=1)
[1]
>>> flatten(1, keepSeq=1)
[1]
>>> type(flatten([]))
<type 'NoneType'>
'''    
    if not keepSeq and not isinstance(seq, flattenTypes or _flattenTypes):
        return seq
    flattened = to(flattenSeq(seq, depth, flattenTypes))    
    if keepSeq:
        return flattened
    else:
        size = len(flattened)
        if not size:
            return None    
        elif size == 1:
            return flattened[0]
        else:
            return flattened

def bisect_left(a, x, cmp=cmp, lo=0, hi=None):
    """
    Like bisect.bisect_left except it takes a comparision function.
    
    Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, i points just
    before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2        
        if cmp(a[mid],x) < 0: lo = mid+1
        else: hi = mid
    return lo

import threading

class object_with_threadlocals(object):    
    '''
    Creates an attribute whose value will be local to the current
    thread.
    Deleting an attribute will delete it for all threads.

    usage:
        class HasThreadLocals(object_with_threadlocals):
            def __init__(self, bar):
                #set values that will initialize across every thread
                self.initThreadLocals(tl1 = 1, tl2 = bar)
    '''

    def __init__(self, **kw):
        return self.initThreadLocals(**kw)
        
    def initThreadLocals(self, **kw):    
        self._locals = threading.local()
        for propname, initValue in kw.items():
            defaultValueAttrName = '__' + propname + '_initValue'
            setattr(self, defaultValueAttrName, initValue)
            prop = getattr(self, propname, None)
            if not isinstance(prop, object_with_threadlocals._threadlocalattribute):
                self._createThreadLocalProp(propname, defaultValueAttrName)

    def _createThreadLocalProp(self, propname, defaultValueAttrName):
        def get(self):
            try:
                return getattr(self._locals, propname)
            except AttributeError:
                value = getattr(self, defaultValueAttrName)
                setattr(self._locals, propname, value)
                return value

        def set(self, value):
            setattr(self._locals, propname, value)
            
        prop = self._threadlocalattribute(propname, get, set, doc='thread local property for ' + propname)
        setattr(self.__class__, propname, prop)                 

    class _threadlocalattribute(property):
        def __init__(self, propname, *args, **kw):
            self.name = propname
            return property.__init__(self, *args, **kw)

def debugp(*args, **kw):
    import pprint
    if len(args) == 1:
        args = args[0]
    pprint.pprint(args, kw.get('stream'))
    
def htmlQuote(data):
    return data.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def pprintdiff(a, b):
    import difflib
    from pprint import pformat
    d = difflib.SequenceMatcher(None, a, b)
    return '\n'.join([("%7s a[%d:%d] (%s) b[%d:%d] (%s)" %
       (tag, i1, i2, pformat(a[i1:i2]), j1, j2, pformat(b[j1:j2])) )
     for tag, i1, i2, j1, j2 in d.get_opcodes() if tag != 'equal'])

def diff(new, old, cutoffOffset = -100, sep = '\n'):
    '''
    returns a list of changes needed to transform the first string to the second unless the length
    of the list of changes is greater the length of the old content itself plus 
    the cutoffOffset, in which case None is returned.
    '''
    maxlen = len(old) + cutoffOffset
    old = old.split(sep) 
    new = new.split(sep)     
    import difflib
    cruncher = difflib.SequenceMatcher(None, new, old)
    return opcodes2Patch(new, old, cruncher.get_opcodes(), maxlen)

##def merge3(base, first, second):
##    #compare first set changes with second set of changes
##    #if any of the ranges overlap its a conflict
##    #for each change

##    
##    old = old.split(sep) 
##    new = new.split(sep)     
##    import difflib
##    cruncher = difflib.SequenceMatcher(None, new, old)
##    changeset1 = cruncher1.get_opcodes()
##    ranges1 = [(alo, ahi) for tag, alo, ahi, blo, bhi in changeset1
##       if tag != 'equals']
##    ranges1.sort()
##
##    changeset2 = cruncher2.get_opcodes()
##    ranges2 = [(alo, ahi) for tag, alo, ahi, blo, bhi in changeset2
##       if tag != 'equals']
##    ranges2.sort()
##    range2 = iter(range2)
##    for lo, hi in range1: pass
##
##def merge3ToPatch(new1, old, opcodes1, new2, opcodes2):
##    '''
##    Converts a list of opcodes as returned by difflib.SequenceMatcher.get_opcodes()
##    into a list that can be applied to the first sequence using patchList() or patch(),
##    allowing the second list to be discarded.
##    '''
##    changes = []
##    patchlen = 0
##    offset = 0    
##    for tag, alo, ahi, blo, bhi in opcodes1:#to turn a into b
##        clo, chi = opcodes2.next()
##        if clo < alo:
##            if chi < ahi: #overlapping change: conflict
##                #for each new version, find the content that covers the overlapping ranges
##                lo = min(alo1, alo2)
##                hi = max(ahi1, ahi2)
##                #...keep looking left and right make the sure the outer overlap doesn't overlap with another change
##                changes.append( ( 'c', alo1+offset, ahi2+offset, old1[blo:bhi] ))
##            else:
##                updatePatch(changes, old, offset, patchlen, tag2, alo2, ahi2, blo2, bhi2)
##        else:
##            if clo < ahi: #overlapping change: conflict
##                'c'
##            else:
##                updatePatch(changes, old, offset, patchlen, tag1, alo1, ahi1, blo1, bhi1)
##    return changes
                
def updatePatch(changes, old, offset, patchlen, tag, alo, ahi, blo, bhi, maxlen=0):
    if tag == 'replace':        
        #g = self._fancy_replace(a, alo, ahi, b, blo, bhi)            
        changes.append( ( 'r', alo+offset, ahi+offset, old[blo:bhi] ))
        offset += (bhi - blo) - (ahi - alo)
        if maxlen:
            patchlen = reduce(lambda x, y: x + len(y), old[blo:bhi], patchlen)
    elif tag == 'delete':            
        changes.append( ( 'd', alo+offset, ahi+offset) )
        offset -= ahi - alo
    elif tag == 'insert':            
        changes.append( ( 'i', alo+offset, old[blo:bhi] ))
        offset += bhi - blo
        if maxlen:
            patchlen = reduce(lambda x, y: x + len(y), old[blo:bhi], patchlen)
    if patchlen > maxlen:
        return None #don't bother
    return offset, patchlen
            
def opcodes2Patch(new, old, opcodes, maxlen = 0):
    '''
    Converts a list of opcodes as returned by difflib.SequenceMatcher.get_opcodes()
    into a list that can be applied to the first sequence using patchList() or patch(),
    allowing the second list to be discarded.
    '''
    changes = []
    patchlen = 0
    offset = 0    
    for tag, alo, ahi, blo, bhi in opcodes:#to turn a into bn
        retVal = updatePatch(changes, old, offset, patchlen, tag, alo, ahi, blo, bhi, maxlen)
        if retVal is None:
            return None #don't bother
        else:
            offset, patchlen = retVal
    return changes

def patch(base, patch, sep = '\n'):
    base = base.split(sep)
    for op in patch:
        if op[0] == 'r':
            base[op[1]:op[2]] = op[3]
        elif op[0] == 'd':
            del base[ op[1]:op[2]]
        elif op[0] == 'i':
            base.insert(op[1], sep.join(op[2]) )
        elif op[0] == 'c':
            #todo: 'c' not yet implemented
            base.insert(op[1], '<<<<<')
            base.insert(op[1], sep.join(op[2]) )
            base.insert(op[1], '=====')
            base.insert(op[1], sep.join(op[2]) )
            base.insert(op[1], '>>>>>')            
    return sep.join(base)

def patchList(base, patch):
    for op in patch:
        if op[0] == 'r':
            base[op[1]:op[2]] = op[3]
        elif op[0] == 'd':
            del base[ op[1]:op[2]]
        elif op[0] == 'i':
            base[op[1]:op[1]] = op[2]    

def removeDupsFromSortedList(aList):       
    def removeDups(x, y):
        if not x or x[-1] != y:
            x.append(y)
        return x
    return reduce(removeDups, aList, [])

def diffSortedList(oldList, newList, cmp=cmp):
    '''
    Returns a list of instructions for turning the first list 
    into the second assuming they both sorted lists of comparable objects
    
    The instructions will be equivalent to the list returned by
    difflib.SequenceMatcher.get_opcodes()

    An optional comparision function can be specified.    
    '''
    opcodes = []
    nstart = nstop = ostart = ostop = 0

    try:
        last = 'i'            
        old = oldList[ostop]            
        
        last = 'd'
        new = newList[nstop]     
        while 1:
            while cmp(old,new) < 0:
                last = 'i'
                ostop += 1                            
                old = oldList[ostop]                
            if ostop > ostart:
                #delete the items less than new
                op = [ 'delete', ostart, ostop, None, None]
                opcodes.append( op )
                ostart = ostop

            assert cmp(old, new) >= 0
            if cmp(old, new) == 0:                
                last = 'i='
                ostart = ostop = ostop+1            
                old = oldList[ostop]
                
                last = 'd'
                nstart = nstop = nstop+1
                new = newList[nstop]     
      
            while cmp(old, new) > 0:
                last = 'd'
                nstop += 1 
                new = newList[nstop]                                
            if nstop > nstart:
                #add
                op = [ 'insert', ostart, ostop, nstart, nstop]
                opcodes.append( op )
                nstart = nstop

            assert cmp(old, new) <= 0
            
            if cmp(old, new) == 0:
                last = 'i='
                ostart = ostop = ostop+1            
                old = oldList[ostop]                
                
                last = 'd'
                nstart = nstop = nstop+1
                new = newList[nstop]                     
    except IndexError:
        #we're done
        if last[0] == 'i':
            if last[-1] == '=':
                try:
                    nstart = nstop = nstop+1
                    new = newList[nstop]
                except IndexError:
                    return opcodes #at end of both lists so we're done
            
            if ostop > ostart:
                #delete the items less than new
                op = [ 'delete', ostart, ostop, None, None]
                opcodes.append( op )
            if len(newList) > nstop:
                op = [ 'insert', ostop, ostop, nstop, len(newList)]
                opcodes.append( op )                
        else:
            if nstop > nstart:
                #add
                op = [ 'insert', ostart, ostop, nstart, nstop]
                opcodes.append( op )                            
            op = [ 'delete', ostop, len(oldList), None, None]
            opcodes.append( op )

    return opcodes

def walkDir(path, fileFunc, *funcArgs, **kw):
    path = os.path.normpath(path).replace(os.sep, '/')
    assert S_ISDIR( os.stat(path)[ST_MODE] )

    def _walkDir(path, recurse, funcArgs, kw):
        '''recursively descend the directory rooted at dir
        '''
        for f in os.listdir(path):
            pathname = '%s/%s' % (path, f) #note: as of 2.2 listdir() doesn't return unicode                        
            mode = os.stat(pathname)[ST_MODE]
            if S_ISDIR(mode):
                # It's a directory, recurse into it
                if recurse:
                    recurse -= 1
                    if not dirFunc:
                        _walkDir(pathname, recurse, funcArgs, kw)
                    else:
                        dirFunc(pathname, lambda *args, **kw:
                                _walkDir(pathname, recurse, args, kw), *funcArgs, **kw)   
            elif S_ISREG(mode):
                if fileFunc:
                    fileFunc(pathname, f, *funcArgs, **kw)
            else:
                # Unknown file type, raise an exception
                raise 'unexpected file type: %s' % pathname #todo?

    if kw.has_key('recurse'):
        recurse = kw['recurse']
        assert recurse >= 0
        del kw['recurse']
    else:
        recurse = 0xFFFFFF
    dirFunc = kw.get('dirFunc')
    if not dirFunc:
        _walkDir(path, recurse, funcArgs, kw)
    else:
        del kw['dirFunc']
        return dirFunc(path, lambda *args, **kw: _walkDir(path, recurse, args, kw), *funcArgs, **kw)

class Hasher:
    def __init__(self):                         
        self.sha = sha1()
    def write(self, line):
        #print line
        self.sha.update(line.strip().encode('utf8'))

def shaDigest(filepath):
    BUF = 8192
    sha = sha1()
    shaFile = file(filepath, 'rb', BUF)
    for line in iter(lambda: shaFile.read(BUF), ""):
        sha.update(line)
    shaFile.close()
    return b2a_base64(sha.digest())[:-1]
    
def shaDigestString(line):
    sha = sha1()
    sha.update(line)
    return b2a_base64(sha.digest())[:-1]

class Bitset(object):
    '''
>>> bs = Bitset()
>>> bs[3] = 1
>>> [i for i in bs]
[False, False, False, True]
    '''
    
    def __init__(self):
        self.bits = 0
        self._size = 0
                
    def __setitem__(self, i, v):
        if v:
            self.bits |= (1<<i)
        else:
            self.bits &= ~(1<<i)

        if i+1 > self._size:
            self._size = i+1
            
    def __getitem__(self, i):
        return bool(self.bits & (1<<i))

    def __nonzero__(self):
        return bool(self.bits)
        
    def __len__(self):
        return self._size

    def __iter__(self):
        for i in xrange(self._size):
            yield self[i]

    def append(self, on):
         self.bits <<= 1
         if on:
             self.bits |= 1

class Singleton(type):
    '''from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/102187
    usage:
    class C: __metaclass__=Singleton
    '''
    def __init__(cls,name,bases,dic):
        super(Singleton,cls).__init__(name,bases,dic)
        cls.instance=None
    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance=super(Singleton,cls).__call__(*args,**kw)
        return cls.instance

class MonkeyPatcher(type):
    '''    
    This metaclass provides a convenient way to patch an existing class instead of defining a subclass.
    This is useful when you need to fix bugs or add critical functionality to a library without
    modifying its source code. It also can be use to write aspect-oriented programming style code where
    methods for a class are defined in separate modules.
    
    usage:
    given a class named NeedsPatching that needs the method 'buggy' patched.
    'unused' never needs to be instantiated, the patching occurs as soon as the class statement is executed.
    
    class unused(NeedsPatching):
        __metaclass__ = MonkeyPatcher

        def buggy(self):           
           self.buggy_old_() 
           self.newFunc()
           
        def newFunc(self):
            pass    
    '''
    
    def __init__(self,name,bases,dic):
        assert len(bases) == 1
        self.base = bases[0]
        for name, value in dic.items():
            if name in ['__metaclass__', '__module__']:
                continue
            try:
                oldValue = getattr(self.base,name)
                hasOldValue = True
            except:
                hasOldValue = False
            setattr(self.base, name, value)
            if hasOldValue:                
                setattr(self.base, name+'_old_', oldValue)

    def __call__(self,*args,**kw):
        '''instantiate the base object'''        
        return self.base.__metaclass__.__call__(*args,**kw)

class NestedException(Exception):
    def __init__(self, msg = None,useNested = False):
        if not msg is None:
            self.msg = msg
        self.nested_exc_info = sys.exc_info()
        self.useNested = useNested
        if useNested and self.nested_exc_info[0]:
            if self.nested_exc_info[1]:
                args = getattr(self.nested_exc_info[1], 'args', ())
            else: #nested_exc_info[1] is None, a string must have been raised
                args = self.nested_exc_info[0]
        else:
            args = msg
        Exception.__init__(self, args)
            
class DynaException(Exception):
    def __init__(self, msg = None):
        if not msg is None:
            self.msg = msg        
        Exception.__init__(self, msg)
    
class DynaExceptionFactory(object):
    '''
    Defines an Exception class
    usage:
    _defexception = DynaExceptionFactory(__name__)
    _defexception('not found error') #defines exception NotFoundError
    ...
    raise NotFoundError()
    '''    
    def __init__(self, module, base = DynaException):
        self.module = sys.modules[module] #we assume the module has already been loaded
        #self.module = __import__(module) #doesn't work for package -- see the docs for __import__ 
        self.base = base
                        
    def __call__(self, name, msg = None):
        classname = ''.join([word[0].upper()+word[1:] for word in name.split()]) #can't use title(), it makes other characters lower
        dynaexception = getattr(self.module, classname, None)
        if dynaexception is None:
            #create a new class derived from the base Exception type
            msg = msg or name            
            dynaexception = type(self.base)(classname, (self.base,), { 'msg': msg })
            #print 'setting', classname, 'on', self.module, 'with', dynaexception
            #import traceback; print traceback.print_stack(file=sys.stderr)
            setattr(self.module, classname, dynaexception)
        return dynaexception

class attrdict(dict):
    '''
`attrdict` is a `dict` subclass that lets you access keys as using attribute notation.
`dict` attributes and methods are accessed as normal and so mask any keys 
in the dictionary with the same name (of course, those keys can need to be accessed 
through the standard `dict` interface).

For example: 
>>> d = attrdict(foo=1, update=2, copy=3)
>>> d.foo
1
>>> d.copy()
{'copy': 3, 'update': 2, 'foo': 1}
>>> d.update({'update':4})
>>> d['update']
4
>>> d.update #doctest: +ELLIPSIS
<built-in method update of attrdict object at ...>
>>> d[10] = '10'
>>> len(d)
4
>>> del d[10]
>>> len(d)
3
>>> d.newattribute = 'here'
>>> d.newattribute
'here'
>>> d['newattribute']
'here'
>>> d.missingattribute
Traceback (most recent call last):
    ...
AttributeError: missingattribute not found
    '''

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name + ' not found')

    def __setattr__(self, name, value):
        self[name] = value

class defaultattrdict(attrdict):
    '''
`defaultattrdict` is a `dict` subclass that lets you access keys as using attribute notation.
`dict` attributes and methods are accessed as normal and so mask any keys 
in the dictionary with the same name (of course, those keys can need to be accessed 
through the standard `dict` interface).
If attribute is not available the None is returned. This default value can be changed by 
setting defaultattrdict.UNDEFINED. Note that this can only be set at the class 
level, not per instance.

For example: 
>>> d = defaultattrdict(foo=1, update=2, copy=3)
>>> d.foo
1
>>> d.copy()
{'copy': 3, 'update': 2, 'foo': 1}
>>> d.update({'update':4})
>>> d['update']
4
>>> d[10] = '10'
>>> len(d)
4
>>> del d[10]
>>> len(d)
3
>>> type(d.not_there)
<type 'NoneType'>
>>> d.not_there = 'here'
>>> d.not_there
'here'
>>> d['not_there']
'here'
>>> defaultattrdict.UNDEFINED = 0
>>> d.unassigned
0
    '''
    UNDEFINED = None
    
    def __getitem__(self, name):
        return dict.get(self, name, defaultattrdict.UNDEFINED)

class LameAttrDict(dict):
    '''
A `dict` subclass that lets you access keys as using attribute notation.
To access built-in dict attributes and methods prefix the name with '__'.

WARNING: this probably won't work well with code expecting an dict because
calling normal `dict` methods won't work. 

For example: 
>>> d = LameAttrDict(foo=1, update=2, copy=3)
>>> d.foo
1
>>> d.copy
3
>>> d.__copy()
{'copy': 3, 'update': 2, 'foo': 1}
>>> d.__update({'update':4})
>>> d.update
4
>>> d['update']
4
>>> list(d.__iterkeys()) == d.__keys() == list(d)
True
>>> d[10] = '10'
>>> len(d)
4
>>> del d[10]
>>> len(d)
3
    '''

    def __getattribute__(self, name):     
        if name.startswith('__'):
            try:
                return dict.__getattribute__(self, name)                
            except AttributeError:
                try:
                    return dict.__getattribute__(self, name[2:])
                except AttributeError:
                    pass         
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name + ' not found')

class enumlist(list):
    '''
`enumlist` is a list subclass that overrides the built-in iterator
to return (item, index, self) instead of item.

>>> el = enumlist(('a', 'b'))
>>> list(el)
[('a', 0, ['a', 'b']), ('b', 1, ['a', 'b'])]
>>> item = list(el)[1]
>>> item.value
'b'
>>> item.index
1
>>> item.parent
['a', 'b']
    '''

    class item(tuple):
        __slots__ = ()
        
        value = property(lambda self: self[0])
        index = property(lambda self: self[1])
        parent = property(lambda self: self[2])
        
    def __iter__(self):
        for i, v in enumerate(list.__iter__(self)):
            yield enumlist.item((v, i, self))
