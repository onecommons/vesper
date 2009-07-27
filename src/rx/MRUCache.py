'''
An MRU cache implemented as a circular list. Items are not explicitly
added to the cache, instead requests are made and if a requested key
isn't in the cache, the value is calculated and added to the cache.

Heavily modified by Adam Souzis, based on this version:
Copyright (c) 2002 Bengt Richter 2001-10-05. All rights reserved.
Use per Python Software Foundation (PSF) license.
'''

from rx import utils
import weakref, repr as Repr

try:
    from hashlib import md5 # python 2.5 or greater
except ImportError:
    from md5 import new as md5

_defexception = utils.DynaExceptionFactory(__name__)
_defexception('not cacheable') #define NotCacheable

class UseNode(object):
    """For linked list kept in most-recent .. least-recent *use* order"""
    
    __slots__ = ['value','hkey','older','newer', 'sideEffects', 'size',
                 'strongref', '__weakref__']

    def __init__(self, value, hkey, older=None, newer=None):
        self.value = value  # as returned by user valueCalc function
        self.hkey = hkey    # defaults to arg tuple for valueCalc, or else
                            # result of user hashCalc function called with those args
        self.older = older  # link to node not as recently used as current
        self.newer = newer  # Note that list is circular: mru.newer is lru
                            # and lru.older is mru, which is the reference point.
        self.sideEffects = None
                            
    def __repr__(self):
        return str(self.size) + ','+ str(self.hkey) + ',' + str(self.value)

class InvalidationKey(tuple):
    ''' Sub-class of tuple used for marking that a part of a key
    should be added to a validation cache. The exclude argument will exclude
    this tuple from the cache key. '''

    exclude = False

    def __new__(cls, seq, exclude=False):
        it = tuple.__new__(cls, seq)
        if exclude:
            it.exclude = exclude
        return it
                            
class MRUCache:
    """
    Produces cache object with given capacity for MRU/LRU list.
    Uses user-supplied valueCalc function when it can't find value in cache.
    Uses optional user-supplied hashCalc function to make key for finding
    cached values or uses valueCalc arg tuple as key.
    """
    debug = False
    
    def __init__(self,
        capacity,       # max number of simultaneous cache MRU values kept
        valueCalc=None,      # user function to calculate actual value from args
        hashCalc=None,  # normally takes same args as valueCalc if present
        #valueCalc might have some sideEffects that we need to reproduce when we retrieve the cache value:
        sideEffectsFunc=None, #execute the sideEffectsFunc when we return retrieve the value from the cache
        sideEffectsCalc=None, #calculate the sideEffects when we calculate the value
        isValueCacheableCalc=None, #calculate if the value should be cached
        capacityCalc = lambda k, v: 1, #calculate the capacity of the value
        maxValueSize=None,
        digestKey=False, #use a (md5) digest of the key instead of the key itself
    ):
        '''
        Takes capacity, a valueCalc function, and an optional hashCalc
        function to make an MRU/LRU cache instance with a getValue method that
        either retrieves cached value or calculates a new one. Either way,
        makes the value MRU.
        '''
        self.capacity = capacity
        self.hashCalc = hashCalc
        self.valueCalc = valueCalc
        self.sideEffectsCalc = sideEffectsCalc
        self.sideEffectsFunc = sideEffectsFunc
        self.isValueCacheableCalc = isValueCacheableCalc
        self.capacityCalc = capacityCalc
        self.mru = None
        self.nodeSize = 0
        self.nodeDict = dict()
        self.invalidateDict = weakref.WeakValueDictionary()
        self.maxValueSize = maxValueSize
        self.digestKey = digestKey
        
    def getValue(self, *args, **kw):  # magically hidden whether lookup or calc
        """
        Get value from cache or calcuate a new value using user function.
        Either way, make the new value the most recently used, replacing
        the least recently used if cache is full.
        """    
        return self.getOrCalcValue(self.valueCalc, hashCalc=self.hashCalc,
                                sideEffectsFunc=self.sideEffectsFunc,
                                sideEffectsCalc=self.sideEffectsCalc,
                                isValueCacheableCalc=self.isValueCacheableCalc,
                                *args, **kw)
    
    def getOrCalcValue(self, valueCalc, *args, **kw):
        '''
        Like getValue() except you must specify the valueCalc function and (optionally)
        hashCalc, sideEffectCalc, sideEffectsFunc and isValueCacheableCalc as keyword arguments.
        Use this when valueCalc may vary or when valueCalc shouldn't be part of the cache or the owner of the cache.

        self.valueCalc, self.hashCalc, self.sideEffectsCalc, etc. are all ignored by this function.
        '''
        if 'hashCalc' in kw:
            hashCalc = kw['hashCalc']
            del kw['hashCalc']
        else:
            hashCalc = None

        if 'sideEffectsCalc' in kw:
            sideEffectsCalc = kw['sideEffectsCalc']
            del kw['sideEffectsCalc']
        else:
            sideEffectsCalc = None
            
        if 'sideEffectsFunc' in kw:
            sideEffectsFunc = kw['sideEffectsFunc']
            del kw['sideEffectsFunc']
        else:
            sideEffectsFunc = None

        if 'isValueCacheableCalc' in kw:
            isValueCacheableCalc = kw['isValueCacheableCalc']
            del kw['isValueCacheableCalc']
        else:
            isValueCacheableCalc = None

        if self.capacity == 0: #no cache, so just execute valueCalc
            return valueCalc(*args, **kw)

        invalidateKeys = []
        if hashCalc:
            try: 
                keydigest = hkey = hashCalc(*args, **kw)
            except NotCacheable: #can't calculate a key
                #if self.debug:
                #    import traceback
                #    traceback.print_exc()
                return valueCalc(*args, **kw)
        else:
            keydigest = hkey = args # use tuple of args as default key for first stage LU            
            #warning: kw args will not be part of key

        assert not (isinstance(hkey, InvalidationKey) and hkey.exclude
                    ), 'key can not be an excluded InvalidationKey'
        if self.digestKey:                   
            digester = md5()
            _getKeyDigest(hkey, invalidateKeys, digester)
            keydigest = digester.hexdigest()
        else:
            #we still want to find invalidatation keys
            keydigest = _removeExcludedInvalidationKeys(hkey, invalidateKeys)

        try:
            node = self.nodeDict[keydigest]
            if self.debug: self.debug('found key '+ Repr.repr(hkey)+' value '
                                                      + Repr.repr(node.value))
            assert node.hkey == keydigest
            #if node.invalidate and node.invalidate(node.value, *args, **kw):
            #    self.removeNode(node)
            #    raise KeyError 
            if sideEffectsFunc:
                #print 'found key:\n', hkey, '\n value:\n', node.value
                sideEffectsFunc(node.value, node.sideEffects, *args, **kw)            
            value = node.value
        except KeyError:
            # we can't retrieve value
            # calculate new value
            value = valueCalc(*args, **kw)

            newValueSize = self.capacityCalc(hkey, value)
            if newValueSize > (self.maxValueSize or self.capacity):                
                if self.debug: self.debug(newValueSize + ' bigger than '+
                                          (self.maxValueSize or self.capacity))
                return value #too big to be cached
            #note this check doesn't take into account the current
            #nodeSize so the cache can grow to just less than double the capacity
            
            if isValueCacheableCalc:
                newvalue = isValueCacheableCalc(hkey, value, *args, **kw)
                if newvalue is NotCacheable:
                    if self.debug: self.debug('value is not cachable'+Repr.repr(value))
                    return value #value isn't cacheable
                else:
                    value = newvalue
                        
            if sideEffectsCalc:
                sideEffects = sideEffectsCalc(value, *args, **kw)
            else:
                sideEffects = None

            # update the circular list
            if self.nodeSize + newValueSize <= self.capacity:
                #not yet full, add a new node
                if self.mru is None: #no nodes yet
                    self.mru = UseNode(value, keydigest)
                    self.mru.sideEffects = sideEffects
                    self.mru.size = newValueSize
                    self.mru.older = self.mru.newer = self.mru  # init circular list
                else:                    
                    # put new node between existing lru and mru
                    lru = self.mru.newer # newer than mru circularly goes to lru node
                    node = UseNode(value, keydigest, self.mru, lru)
                    node.sideEffects = sideEffects
                    node.size = newValueSize
                    # update links on both sides
                    self.mru.newer = node     # newer from old mru is new mru
                    lru.older = node    # older than lru poits circularly to mru
                    # make new node the mru
                    self.mru = node                
            else:
                #cache full, replace the lru node
                lru = self.mru.newer; #newer than mru circularly goes to lru node
                # position of lru node is correct for becoming mru so
                # just replace value and hkey #                
                self.nodeSize -= lru.size
                #print lru.hkey
                lru.value = value
                lru.sideEffects = sideEffects
                lru.size = newValueSize
                # delete invalidated key->node mapping
                del self.nodeDict[lru.hkey]
                lru.hkey = keydigest
                self.mru = lru # new lru is next newer from before

            self.nodeSize += newValueSize
            if self.debug:
                self.debug('adding key '+ Repr.repr(keydigest)+' value '+Repr.repr(value))
            self.nodeDict[keydigest] = self.mru      # add new key->node mapping
            
            if invalidateKeys:
                #we can associate invalidation keys with many nodes to enable cache invalidation                
                for ikey in invalidateKeys:
                    newWeakrefDict = weakref.WeakKeyDictionary()
                    weakrefDict = self.invalidateDict.setdefault(ikey, newWeakrefDict)
                    #add the attribute "strongref" because
                    #invalidateDict is a WeakValueDictionary so the
                    #only strong reference to the weakrefDict with the
                    #node its themselves this way, when the last key
                    #is remove from the weak key dictionary, the
                    #dictionary will be garbage collected too
                    weakrefDict[self.mru] = weakrefDict
                    self.mru.strongref = weakrefDict
                    
            return value
            
        # Here we have a valid node. Just update its position in linked lru list
        # we want take node from older <=> node <=> newer
        # and put it in lru <=> node <=> mru and then make new node the mru
        # first cut it out unless it's first or last
        if node is self.mru:            # nothing to do
            return value
        lru = self.mru.newer            # circles from newest to oldest
        if node is lru:
            self.mru = lru              # just backs up the circle one notch
            return value
        # must be between somewhere, so cut it out first
        node.older.newer = node.newer   # older neighbor points to newer neighbor
        node.newer.older = node.older   # newer neighbor points to older neighbor
        # then put it between current lru and mru
        node.older = self.mru           # current mru is now older
        self.mru.newer = node
        node.newer = lru                # newer than new mru circles to lru
        lru.older = node
        self.mru = node                 # new node is new mru
        return value

    def invalidate(self, key):
        currentNodes = self.invalidateDict.get(key)        
        if currentNodes is not None:
            #import pprint; pprint.pprint([x.hkey for x in currentNodes.keys()])
            for node in currentNodes.keys(): #we need a copy since currentNodes may change
                self.removeNode(node)
            try:
                del self.invalidateDict[key]
            except KeyError:
                pass #there's a slim chance it got deleted already by the garbage collector

    def removeNode(self, node):            
        if node.older is node:
            #there's only one node and we're removing it
            assert self.mru is node
            self.mru = None
        else:
            if self.mru is node:
                self.mru = node.older
            node.older.newer = node.newer   # older neighbor points to newer neighbor
            node.newer.older = node.older   # newer neighbor points to older neighbor            
        self.nodeSize -= node.size
        del self.nodeDict[node.hkey]                

    def _countNodes(self):
        '''much slower than len(self.nodeDict) -- exists for diagnostic use
        '''
        prev = self.mru
        if not prev:
            return 0
        count = 1
        while prev:            
            if prev.older is self.mru:
                return count
            prev = prev.older
            count+=1        

    def clear(self):
        """
        Clear out circular list and dictionary of cached nodes.
        Re-init empty with same capacity and user functions
        for posssible continued use.
        """
        this = self.mru
        if this is None:
            #already empty
            assert self.nodeSize == 0
            assert not self.nodeDict
            return
        lru = this.newer
        while 1:
            next = this.older
            this.older = this.newer = None
            del this
            this = next
            if this is lru: break
        this.older = this.newer = None
        del this
        self.nodeDict.clear()
        self.invalidateDict.clear()
        # re-init
        self.mru = None
        self.nodeSize = 0

def _getKeyDigest(keys, invalidateKeys, keyDigest):
    if isinstance(keys, tuple):
        if isinstance(keys, InvalidationKey):
            invalidateKeys.append(keys)
            keyDigest = not keys.exclude and keyDigest
        if keyDigest: keyDigest.update( '(' )
        for key in keys:
            _getKeyDigest(key, invalidateKeys, keyDigest)
            if keyDigest: keyDigest.update( ',' )
        if keyDigest: keyDigest.update( ')' )
    elif keyDigest:
        if isinstance(keys, unicode):
            keyDigest.update( keys.encode('utf8'))
        else:
            keyDigest.update( str(keys) )

def _removeExcludedInvalidationKeys(keys, invalidateKeys):
    #note: will still return the outmost tuple even if it is an excluded invalidationKey
    if isinstance(keys, tuple):
        #return a new keys tuple excluding any InvalidationKeys set to exclude
        #as a side-effect, add any InvalidationKeys found to invalidationKeys
        return tuple([_removeExcludedInvalidationKeys(key, invalidateKeys)
                      for key in keys
                       if not (isinstance(key, InvalidationKey) and
                       (invalidateKeys.append(key) or key.exclude)) ])
    return keys