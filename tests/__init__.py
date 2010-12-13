#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import sys, unittest, docTest

__all__ = ['glockTest', 'appTest', 'MRUCacheTest', 
 'transactionsTest', 'utilsTest', 'RDFDomTest', 'htmlfilterTest',
  'pjsonTest', 'jqlTest', 'jsonqlDocTest', 'jsonqlTutorialTest', 'modelTest', 'FileModelTest']

if sys.version_info[:2] >= (2,5):
    __all__.append('python25Test')

try:
    import vesper.data.store.bdb
except ImportError:
    print "skipping Bdb tests"
else:
    __all__.append('BdbModelTest')
    
try:
    import multiprocessing
    import stomp
    import morbid
    import twisted.internet    
except ImportError:
    print "skipping replication tests"
else:
    __all__.append('replicationTest')
    
try:
    import pytyrant
except ImportError:
    print "skipping tokyo tyrant tests"
else:
    __all__.append("basicTyrantTest")
    
if __name__ == '__main__':
    suites = unittest.TestLoader().loadTestsFromNames(__all__)
    suites.addTests(docTest.suite)
    result = unittest.TextTestRunner().run(suites)
    if result.wasSuccessful():
        exit_code = 0
    else:
        exit_code = 1
    sys.exit(exit_code)
