#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    General purpose utilities
"""
import os.path
import os, sys, threading
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

def package_import(name):
    '''
    Get a reference to the module object
    Workaround an inconvenient behavior with __import__ on multilevel imports
    '''
    # Can't find the ref in the python docs anymore, but these discuss the issue:
    # http://stackoverflow.com/questions/211100/pythons-import-doesnt-work-as-expected
    # http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class    
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def getTransitiveClosure(aMap):
    def close(done, super, subs):
        done[super] = set(subs)
        for sub in subs:
            if not sub in done:
                close(done, sub, aMap[sub])
            done[super].update(done[sub])

    closure = {}
    for key, value in aMap.items():
        close(closure, key, value)
    return dict([(x, list(y)) for x, y in closure.items()])
    
class ObjectWithThreadLocals(object):
    '''
    Creates an attribute whose value will be local to the current
    thread.
    Deleting an attribute will delete it for all threads.

    usage:
        class HasThreadLocals(ObjectWithThreadLocals):
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
            if not isinstance(prop, ObjectWithThreadLocals._threadlocalattribute):
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
>>> 'newattribute' in d
False
>>> hasattr(d, 'newattribute')
False
>>> d.newattribute = 'here'
>>> 'newattribute' in d
True
>>> hasattr(d, 'newattribute')
True
>>> d.newattribute
'here'
>>> d['newattribute']
'here'
>>> d.missingattribute
Traceback (most recent call last):
    ...
AttributeError: missingattribute not found
>>> del d['newattribute']
>>> 'newattribute' in d
False
>>> hasattr(d, 'newattribute')
False
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

Note that because the built-in function `hasattr()` calls `getattr()` it will always return True
even if the attribute isn't defined. Use `in` operator or the `has_key` method 
to test for key existence.

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
>>> defaultattrdict.UNDEFINED = None
>>> 'newattribute' in d
False
>>> hasattr(d, 'newattribute')
True
>>> d.newattribute = 'here'
>>> 'newattribute' in d
True
>>> d.newattribute
'here'
>>> d['newattribute']
'here'
>>> del d['newattribute']
>>> 'newattribute' in d
False
>>> hasattr(d, 'newattribute')
True
    '''
    UNDEFINED = None
    
    def __getitem__(self, name):
        return dict.get(self, name, defaultattrdict.UNDEFINED)

def _defaultproxyattrdict_getitem(realdict, name):
    val = realdict.get(name, defaultattrdict.UNDEFINED) 
    def mapproxy(obj, item, val):
        if type(val) is dict: #dict but not a subclass of dict
            val = defaultproxyattrdict(val)
            obj[item] = val
        elif isinstance(val, list):
            for i, item in enumerate(val):
                mapproxy(val, i, item)
        return val
    return mapproxy(realdict, name, val)
    
class defaultproxyattrdict(defaultattrdict):
    '''
Like defaultattrdict but hold a reference to the given dictionary instead of make a copy of it. 
An updates updates the given dictionary. Also lazily (as the keys are accessed) creates 
defaultproxyattrdicts for any dictionaries that appears as values in the dictionary. 
If the value is a list, when that list is accessed, it will have any dict 
replaced with defaultproxyattrdicts.

>>> d = defaultproxyattrdict(dict(foo={'a':1}, update=2, copy=3))
>>> d.foo
{'a': 1}
>>> d.foo.a
1
>>> d
{'copy': 3, 'update': 2, 'foo': {'a': 1}}
>>> type(d.foo.not_there)
<type 'NoneType'>
>>> d.copy()
{'copy': 3, 'update': 2, 'foo': {'a': 1}}
>>> d.update({'update':4})
>>> d['update']
4
>>> d[10] = '10'
>>> d[10]
'10'
>>> len(d)
4
>>> del d[10]
>>> len(d)
3
>>> type(d.not_there)
<type 'NoneType'>
>>> 'not_there' in d
False
>>> d.not_there = 'here'
>>> d.not_there
'here'
>>> d['not_there']
'here'
>>> 'not_there' in d
True
>>> d2 = defaultproxyattrdict(dict(alist=[{1:2}]))
>>> type(d2.alist[0])
<class 'vesper.utils._utils.defaultproxyattrdict'>
>>> del d['not_there']
>>> 'not_there' in d
False
>>> d.has_key('not_there')
False
    '''

    def __init__(self, d):
        dict.__setattr__(self, '_dict', d)

    def __getattribute__(self, name):
        if name in ['_dict', '__init__', '__setattr__', '__getitem__']:
            return dict.__getattribute__(self, name)
        
        _dict = dict.__getattribute__(self, '_dict')
        try:
            return getattr(_dict, name)
        except AttributeError:
            return _defaultproxyattrdict_getitem(_dict, name)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return repr(self._dict)
        
    def __setattr__(self, name, value):
        self._dict[name] = value

    def __getitem__(self, name):
        return _defaultproxyattrdict_getitem(self._dict, name)

    def __setitem__(self, name, value):
        self._dict[name] = value

    def __delitem__(self, name):
        del self._dict[name]

    def __contains__(self, item):
        return item in self._dict

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
