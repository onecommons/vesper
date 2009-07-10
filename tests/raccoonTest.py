"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import raccoon
from rx import utils, logging
import unittest, glob, os, os.path

class RaccoonTestCase(unittest.TestCase):
    def setUp(self):
        logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
        logging.root.setLevel(logging.INFO)
        logging.basicConfig()
        
    def testMinimalApp(self):
        root = raccoon.RequestProcessor(a='testMinimalApp.py',model_uri = 'test:')
        result = root.runActions('http-request', dict(_name='foo'))
        #print type(result), result
        response = "<html><body>page content.</body></html>"
        self.failUnless(response == result)
        
        #XXX test for InputSource
        #result = raccoon.InputSource.DefaultFactory.fromUri(
        #    'site:///foo', resolver=root.resolver).read()    
        #print type(result), repr(result), result
        #self.failUnless(response == result)
        
        result = root.runActions('http-request', dict(_name='jj'))
        #print type(result), result
        self.failUnless( '<html><body>not found!</body></html>' == result)

    def testErrorHandling(self):
        root = raccoon.RequestProcessor(a='testErrorHandling-config.py',model_uri = 'test:')
        result = root.runActions('test-error-request', dict(_name='foo'))
        
        response = "404 not found"
        self.failUnless(response == result)

if __name__ == '__main__':
    import sys    
    #import os, os.path
    #os.chdir(os.path.basename(sys.modules[__name__ ].__file__))
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = RaccoonTestCase(test)
        tc.setUp()
        getattr(tc, test)() #run test
