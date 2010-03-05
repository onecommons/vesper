#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from __future__ import with_statement
'''
Unit tests for functionality only available in Python 2.5 and later
'''
import unittest
import vesper.app

class Python25TestCase(unittest.TestCase):
    def testTxnContext(self):
      root = vesper.app.createApp().load()
      self.failUnless(not root.txnSvc.isActive())
      with root.inTransaction():
          self.failUnless(root.txnSvc.isActive())
      self.failUnless(not root.txnSvc.isActive())

if __name__ == '__main__':
    import sys    
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = Python25TestCase(test)
        tc.setUp()
        getattr(tc, test)() #run test
