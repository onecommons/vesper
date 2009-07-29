"""
    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
__all__ = ['glockTest', 'raccoonTest', 'MRUCacheTest', 
 'transactionsTest', 'utilsTest', 'RDFDomTest', 'htmlfilterTest',
  'sjsonTest', 'jqlTest', 'basicTyrantTest']

import unittest
_runner = unittest.TextTestRunner()

class TestProgram(unittest.TestProgram):

    def runTests(self):            
        result = _runner.run(self.test)
        #sys.exit(not result.wasSuccessful()) #we don't want to exit!

if __name__ == '__main__':    
    for modname in __all__:
        print 'testing', modname
        TestProgram(modname)

