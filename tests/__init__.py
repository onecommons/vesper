"""
    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import sys, unittest

__all__ = ['glockTest', 'raccoonTest', 'MRUCacheTest', 
 'transactionsTest', 'utilsTest', 'RDFDomTest', 'htmlfilterTest',
  'pjsonTest', 'jqlTest', 'modelTest', 'FileModelTest', 'BdbModelTest']

if sys.version_info[:2] >= (2,5):
    __all__.append('python25Test')

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
    unittest.TextTestRunner().run(suites)
