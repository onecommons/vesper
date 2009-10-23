"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import raccoon
from rx import utils, logging
import unittest, glob, os, os.path

expectedChangedSet = {'origin': '0A', 'timestamp': 0, 
 'baserevision': '0', 'revision': '0A00001',
 'statements': 
 [('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#baseRevision', u'0', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#hasRevision', u'0A00001', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#createdOn', u'0', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#includes', 'context:add:context:txn:test:;0A00001;;', 'R', 'context:txn:test:;0A00001'), ('context:add:context:txn:test:;0A00001;;', u'http://rx4rdf.sf.net/ns/archive#applies-to', '', 'R', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', u'http://rx4rdf.sf.net/ns/archive#TransactionContext', 'R', 'context:txn:test:;0A00001'), ('a_resource', 'comment', 'page content.', 'L', 'context:add:context:txn:test:;0A00001;;'), ('a_resource', 'label', 'foo', 'R', 'context:add:context:txn:test:;0A00001;;')] }

class RaccoonTestCase(unittest.TestCase):
    def setUp(self):
        logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
        logging.root.setLevel(logging.INFO)
        logging.basicConfig()
        
    def testMinimalApp(self):
        root = raccoon.RequestProcessor(a='testMinimalApp.py',model_uri = 'test:')
        result = root.runActions('http-request', dict(_name='foo'))
        #print 'result', type(result), result
        response = "<html><body>page content.</body></html>"
        self.assertEquals(response, result)
        
        #XXX test for InputSource
        #result = raccoon.InputSource.DefaultFactory.fromUri(
        #    'site:///foo', resolver=root.resolver).read()    
        #print type(result), repr(result), result
        self.assertEquals(response, result)
        
        result = root.runActions('http-request', dict(_name='jj'))
        #print type(result), result
        self.assertEquals( '<html><body>not found!</body></html>', result)

    def testSequencerApp(self):
        root = raccoon.RequestProcessor(a='testSequencerApp.py',model_uri = 'test:')
        result = root.runActions('http-request', dict(_name='foo'))
        self.assertEquals("<html><body>page content.</body></html>", result)

        root = raccoon.HTTPRequestProcessor(a='testSequencerApp.py',model_uri = 'test:')
        kw = dict(_name='no such page', _responseHeaders={}, _environ={})
        result = root.handleHTTPRequest(kw)
        self.assertEquals(kw['_responseHeaders']['_status'],"404 Not Found")

        kw = dict(_name='/static/testfile.txt', 
            _responseHeaders=dict(_status="200 OK"), _environ={})
        result = root.handleHTTPRequest(kw)
        self.assertEquals(result.read().strip(), "test file")

    def testErrorHandling(self):
        root = raccoon.HTTPRequestProcessor(a='testErrorHandling-config.py',model_uri = 'test:')
        result = root.handleHTTPRequest(dict(_name='foo', 
                    _responseHeaders=dict(_status="200 OK"), _environ={}))
        
        response = "404 not found"
        self.assertEquals(response, result)

    def testUpdatesApp(self):
        root = raccoon.RequestProcessor(a='testUpdatesApp.py',model_uri = 'test:')
        root.domStore.model.createTxnTimestamp = lambda *args: 0
        self.notifyChangeset = None
        def testNotifyChangeset(changeset):
            self.notifyChangeset = changeset            
            self.assertEquals(changeset, expectedChangedSet)
        root.domStore.model.notifyChangeset = testNotifyChangeset
        
        result = root.runActions('http-request', dict(_name='foo'))
        response = "<html><body>page content.</body></html>"
        self.assertEquals(response, result)
        
        self.assertEquals(root.updateResults['_added'], [{'comment': u'page content.', 'id': 'a_resource', 'label': 'foo'}])
        self.assertEquals(root.updateResults['_addedStatements'], [('a_resource', 'comment', 'page content.', 'L', ''), ('a_resource', 'label', 'foo', 'R', '')])
        self.assertEquals(root.updateResults['_removedStatements'], [])
        self.assertEquals(root.updateResults['_removed'], [])        

        root.updateResults = {}
        result = root.runActions('http-request', dict(_name='jj'))        
        self.assertEquals('<html><body>not found!</body></html>', result)

        self.failUnless('_addedStatements' not in root.updateResults)
        self.failUnless('_added' not in root.updateResults)
        self.failUnless('_removedStatements' not in root.updateResults)
        self.failUnless('_removed' not in root.updateResults)
        self.failUnless(self.notifyChangeset)
        
        root2= raccoon.RequestProcessor(a='testUpdatesApp.py',model_uri = 'test:', appVars={'branchId':'0B'})
        try:
            root2.txnSvc.begin()
            self.failUnless( root2.domStore.merge(self.notifyChangeset) )
        except:
            root2.txnSvc.abort()
            raise
        else:
            root2.txnSvc.commit()
        
        self.assertEquals(root2.domStore.query("{*}")['results'], 
            [{'comment': 'page content.', 'id': 'a_resource', 'label': 'foo'}])
    
    def testMerge(self):        
        store1 = raccoon.createStore(saveHistory=True)
        root1 = store1.requestProcessor
        self.assertEquals(store1.model.currentVersion, '0')        
        root1.txnSvc.begin()
        store1.add([{
          'base': [ {'foo': 1}, {'foo': 2}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00001')
        
        root1.txnSvc.begin()
        store1.add([{
          'base': [{'foo': 3}, {'foo': 4}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00002')
                
        
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
