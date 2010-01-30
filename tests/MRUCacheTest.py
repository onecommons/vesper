"""
    MRUCache unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
from vesper.utils import MRUCache

class MRUCacheTestCase(unittest.TestCase):
    def setUp(self):
        #just return whatever is passed as the value
        self.cache = MRUCache.MRUCache(12, lambda arg: arg, capacityCalc=lambda k,v: len(v))
       
    def tearDown(self):
        self.cache = None

    def test1(self):
        v1 = self.cache.getValue(tuple('a'*5))
        v2 = self.cache.getValue(tuple('a'*5))
        self.failUnless(v2 == tuple('a'*5) and v1 is v2) #it's in the cache
        self.failUnless(self.cache.nodeSize == 5)

        #this is item is bigger than the entire cache and so not be placed in the cache
        v3 = self.cache.getValue(tuple('b'*15) ) 
        v4 = self.cache.getValue(tuple('b'*15) )
        self.failUnless(v3 == tuple('b'*15) and v3 is not v4) #it's not in the cache        
        self.failUnless(self.cache.nodeSize == 5)

        v5 = self.cache.getValue(tuple('c'*7))
        v6 = self.cache.getValue(tuple('c'*7))
        self.failUnless(v6 == tuple('c'*7))
        self.failUnless(v5 is v6) #it's in the cache
        self.failUnless(self.cache.nodeSize == 12)

        #now our cache is now full (over 12 characters!)    
        #so adding another item will push out the first item, but not the second
        v7 = self.cache.getValue(tuple('d'*7))
        v8 = self.cache.getValue(tuple('d'*7))
        self.failUnless(v7 == tuple('d'*7))
        self.failUnless(v7 is v8) #it's in the cache
        self.failUnless(self.cache.nodeSize == 14)

        #the second item is in the cache
        v9 = self.cache.getValue(tuple('c'*7))
        self.failUnless(v9 == v5)        
        self.failUnless(v9 is v5)
                        
        #but the first item is not
        v10 = self.cache.getValue(tuple('a'*5))
        self.failUnless(v10 == v1)
        self.failUnless(v10 is not v1)
        self.failUnless(self.cache.nodeSize == 12)
        
        #the second item is in the cache
        v11 = self.cache.getValue(tuple('c'*7))
        #you're still there, right? (just checking)
        self.failUnless(v11 == v5)        
        self.failUnless(v11 is v5)
        self.failUnless(self.cache.nodeSize == 12)

        self.cache.clear()
        self.failUnless(self.cache.nodeSize == 0)

        #the cache is now empty
        v12 = self.cache.getValue(tuple('c'*7))
        self.failUnless(v12 == v5)        
        self.failUnless(v12 is not v5)
        self.failUnless(self.cache.nodeSize == 7)

    def testDigestKey(self):
        self.cache.digestKey = True
        self.test1()

    def testInvalidation(self):
        #create an invalidation key based on whether the value is even or not
        self.cache.hashCalc = lambda arg: (arg, MRUCache.InvalidationKey( [int(arg)%2], exclude=True) )
        v1 = self.cache.getValue('1')
        v2 = self.cache.getValue('2')
        v3 = self.cache.getValue('3')
        self.failUnless(self.cache.nodeSize == 3)
        #remove odd values from cache

        self.cache.invalidate( (1,) )
        self.failUnless(self.cache.nodeSize == 1)
        v4 = self.cache.getValue('2')        
        self.failUnless(v2 is v4) #should be still in the cache
        self.failUnless( len(self.cache.invalidateDict) == 1)

        self.cache.removeNode( self.cache.mru )
        self.failUnless(self.cache._countNodes() == 0)
        import gc
        gc.collect()
        self.failUnless( len(self.cache.invalidateDict) == 0)

    def testRemove(self):
        v1 = self.cache.getValue('b'*7)
        v1 = self.cache.getValue('c'*5)
        self.failUnless(self.cache.nodeSize == 12)
        self.failUnless(self.cache._countNodes() == 2)

        self.cache.removeNode( self.cache.mru ) #remove the last node ('c')
        self.failUnless(self.cache.nodeSize == 7)
        self.failUnless(self.cache._countNodes() == 1)

    def testClear(self):
        #test clear on empty cache
        self.cache.clear()
        self.failUnless(self.cache.nodeSize == 0)
        self.failUnless(self.cache._countNodes() == 0)
        
        v1 = self.cache.getValue('a'*7)
        self.failUnless(self.cache.nodeSize == 7)

        self.cache.clear()
        self.failUnless(self.cache.nodeSize == 0)

        v1 = self.cache.getValue('b'*7)
        self.failUnless(self.cache.nodeSize == 7)

        v1 = self.cache.getValue('c'*5)
        self.failUnless(self.cache.nodeSize == 12)

        self.cache.clear()
        self.failUnless(self.cache.nodeSize == 0)
        self.failUnless(self.cache._countNodes() == 0)
    
if __name__ == '__main__':
    import sys    
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = MRUCacheTestCase(test)
        tc.setUp()
        getattr(tc, test)() #run test
 
