"""
    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
__all__ = ['glockTest', 'raccoonTest', 'MRUCacheTest', 
 'transactionsTest', 'utilsTest', 'RDFDomTest', 'htmlfilterTest',
  'sjsonTest', 'jqlTest', 'modelTest', 'FileModelTest', 'BdbModelTest']
import sys
if sys.version_info[:2] >= (2,5):
    __all__.append('python25Test')

try:
    import multiprocessing
    import stomp
    import morbid
    import twisted.internet    
except ImportError:
    pass
else:
    __all__.append('replicationTest')
    
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
    import docTest
    print 'running docTests...'
    docTest.runner.run(docTest.suite)
