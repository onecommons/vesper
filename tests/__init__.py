"""
    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
__all__ = ['glockTest', 'testmodpatching','raccoonTest', 'MRUCacheTest', 
 'transactionsTest', 'utilsTest', 'RDFDomTest', ] #XXXX

import unittest
class TestProgram(unittest.TestProgram):
    def runTests(self):
        if self.testRunner is None:
            self.testRunner = unittest.TextTestRunner(verbosity=self.verbosity)
        result = self.testRunner.run(self.test)                
        #sys.exit(not result.wasSuccessful()) #we don't want to exit!
    
if __name__ == '__main__':    
    for modname in __all__:
        print 'testing', modname
        TestProgram(modname)

