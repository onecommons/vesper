"""
    utils unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
from rx import utils
from rx.utils import *
    
class utilsTestCase(unittest.TestCase):
    def testSingleton(self):
        class single: __metaclass__=Singleton
        s1 = single()
        s2 = single()
        self.failUnless(s1 is s2)
            
    def testDynException(self):
        _defexception = DynaExceptionFactory(__name__)
        _defexception('test dyn error') #defines exception NotFoundError
        try:
            raise TestDynError()
        except (TestDynError), e:
            self.failUnless(e.msg == "test dyn error")
            
        try:
            raise TestDynError("another msg")
        except (TestDynError), e:
            self.failUnless(e.msg == "another msg")

    def testThreadlocalAttribute(self):
        class HasThreadLocals(ObjectWithThreadLocals):
            def __init__(self, bar):
                #set values that will initialize across every thread
                self.initThreadLocals(tl1 = 1, tl2 = bar)

        test = HasThreadLocals('a')        
        test.tl1 = 2        
        test2 = HasThreadLocals('b')
        
        self.failUnless(test.tl2 == 'a')    
        self.failUnless(test2.tl2 == 'b')        
                
        def threadMain():
            #make sure the initial value are what we expect
            self.failUnless(test.tl1 == 1)
            self.failUnless(test.tl2 == 'a')
            #change them
            test.tl1 = 3
            test.tl2 = 'b'
            #make they're what we just set
            self.failUnless(test.tl1 == 3)
            self.failUnless(test.tl2 == 'b')

        #make sure the initial values are what we expect
        self.failUnless(test.tl1 == 2)
        self.failUnless(test.tl2 == 'a')
        
        thread1 = threading.Thread(target=threadMain)
        thread1.start()
        thread1.join()

        #make sure there the values haven't been changed by the other thread
        self.failUnless(test.tl1 == 2)
        self.failUnless(test.tl2 == 'a')
        
    def testDiffPatch(self):
        orig = "A B C D E"
        new = "A C E D"
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

        orig = "A B B B E"
        new = "A C C C"
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

        orig = ""
        new = "A C C C"
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

        orig = "A B B B E"
        new = ""
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

        orig = ""
        new = ""
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

        orig = "A B B B E"
        new = "A B B B E"
        self.failUnless(new == patch(orig, diff(orig, new, 0, ' '), ' ') )

    def _testSortedDiff(self, old, new):
        #print old, 'to', new
        changes = diffSortedList(old, new)
        #print changes
        patch = opcodes2Patch(old, new, changes)        
        #print patch
        patchList(old, patch)
        #print old
        self.failUnless(new == old)        

    def testSortedDiff(self):
        old = [1, 2, 6]
        new = [0, 2, 4, 9]
        self._testSortedDiff(old,new)

        old = []
        new = [0, 2, 4, 9]
        self._testSortedDiff(old,new)
        
        old = [1, 2, 6]
        new = []
        self._testSortedDiff(old,new)
        
        old = [1, 2, 6]
        new = [0, 2]
        self._testSortedDiff(old,new)
        
        old = [1, 2]
        new = [0, 2, 3]
        self._testSortedDiff(old,new)
        
        old = []
        new = []
        self._testSortedDiff(old,new)

        old = [0, 2, 3]
        new = [0, 2, 3]
        self._testSortedDiff(old,new)

    def testMonkeyPatcher(self):
        class NeedsPatching(object):
            def buggy(self):
                return 1
            
        class unusedname(NeedsPatching):
            __metaclass__ = MonkeyPatcher

            def buggy(self):                          
               return self.newFunc()
               
            def newFunc(self):
                return 2

            def addedFunc(self):
                return self.__class__.__name__

        test = NeedsPatching()

        self.failUnless(test.buggy() == 2)
        self.failUnless(test.buggy_old_() == 1) 
        self.failUnless(test.addedFunc() == 'NeedsPatching')

import doctest

class DocTestTestCase(unittest.TestCase):
    '''This testcast automatically adds doctests to the default TestSuite'''
    
    doctestSuite = doctest.DocTestSuite(utils)

    def run(self, result):
        return self.doctestSuite.run(result)

    def runTest(self):
        '''Just here so this TestCase gets automatically added to the
        default TestSuite'''
        
if __name__ == '__main__':
    import sys
    try:
        test=sys.argv[sys.argv.index("-r")+1]
        tc = utilsTestCase(test)
        getattr(tc, test)() #run test
    except (IndexError, ValueError):
        unittest.main()

