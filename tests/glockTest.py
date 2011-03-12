#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    glock unit tests
"""
import unittest
from vesper.utils import glock
import threading
threading._VERBOSE = 0

class glockTestCase(unittest.TestCase):
    def setUp(self):
        self.lockName = 'test.lock'
       
    def tearDown(self):
        pass

    def test1(self):
        lock = glock.LockFile(self.lockName)
        lock.obtain()
        lock.release()

    def testReentry(self):
        '''test re-entrancy'''
        globalLock = glock.LockFile(self.lockName)
        lock = glock.LockGetter(globalLock)
        lock2 = glock.LockGetter(globalLock)
        lock2.release()
        lock.release()

    def testThreads(self):
        print 'Testing glock.py...' 
        
        # unfortunately can't test inter-process lock here!
        l = glock.LockFile(self.lockName)
        #if not _windows:
        #    assert os.path.exists(lockName)
        l.obtain()
        #print l._lock._RLock__count
        l.obtain() # reentrant lock, must not block
        l.release()
        l.release()

        self.failUnlessRaises(glock.NotOwner, lambda: l.release())

        # Check that <> threads of same process do block:
        import threading, time
        thread = threading.Thread(target=threadMain, args=(self, l,))
        
        print 'main: locking...',
        l.obtain()
        print ' done.'
        thread.start()
        time.sleep(1)
        print '\nmain: unlocking...',
        l.release()
        print ' done.'
        time.sleep(0.1)
        
        print '=> Test of glock.py passed.'
        print 'if the app hangs now, something is wrong!'
        return l

def threadMain(self, lock):
    print 'thread started(%s).' % lock
    self.failUnless(not lock.attempt(), 'should not have gotten the lock')
    print 'thread: locking (should stay blocked for ~ 1 sec)...',
    lock.obtain()
    print 'thread: locking done.'
    print 'thread: unlocking...',
    lock.release()
    print ' done.'
    print 'thread ended.'

if __name__ == '__main__':
    import sys
    try:
        test=sys.argv[sys.argv.index("-r")+1]
        tc = glockTestCase(test)
        getattr(tc, test)() #run test
    except (IndexError, ValueError):
        unittest.main()
 
