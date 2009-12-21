"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import raccoon
from rx import utils, logging, transactions, RxPath
import unittest, glob, os, os.path, traceback

expectedChangeSet = {'origin': '0A', 'timestamp': 0, 
 'baserevision': '0', 'revision': '0A00001',
 'statements': 
 [('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#baseRevision', u'0', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#hasRevision', u'0A00001', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#createdOn', u'0', 'L', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://rx4rdf.sf.net/ns/archive#includes', 'context:add:context:txn:test:;0A00001;;', 'R', 'context:txn:test:;0A00001'), ('context:add:context:txn:test:;0A00001;;', u'http://rx4rdf.sf.net/ns/archive#applies-to', '', 'R', 'context:txn:test:;0A00001'), ('context:txn:test:;0A00001', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', u'http://rx4rdf.sf.net/ns/archive#TransactionContext', 'R', 'context:txn:test:;0A00001'), ('a_resource', 'comment', 'page content.', 'L', 'context:add:context:txn:test:;0A00001;;'), ('a_resource', 'label', 'foo', 'R', 'context:add:context:txn:test:;0A00001;;')] }

LogLevel = logging.CRITICAL+10

class RaccoonTestCase(unittest.TestCase):
    
    def setUp(self):        
        class TestLogHandler(logging.Handler):
            def __init__(self, testCase, level=logging.NOTSET):
                self.records = []
                return logging.Handler.__init__(self, level)
            
            def emit(self, record):
                self.records.append(record)
                
        logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s:%(message)s")
        #using the level parameter in basicConfig isn't enough doesn't reset the log config
        #but Logger.setLevel overrides handlers need to set handler
        logging.root.handlers[0].setLevel(LogLevel) 
        
        self.logHandler = TestLogHandler(self,logging.DEBUG)        
        logging.root.addHandler( self.logHandler ) 
    
    def tearDown(self):
        logging.root.removeHandler( self.logHandler )
    
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

    def _testErrorHandling(self, appVars=None):
        root = raccoon.HTTPRequestProcessor(a='testErrorHandling-config.py',
            model_uri = 'test:', appVars=appVars)
        #make sure the error handler is run and sets the status code to 404 instead of the default 200
        kw = dict(_name='foo', 
                    _responseHeaders=dict(_status="200 OK"), _environ={})
        result = root.handleHTTPRequest(kw)
        
        self.assertEquals(kw['_responseHeaders']['_status'], '%d %s' % (404, root.statusMessages[404]))
        response = "404 not found"
        self.assertEquals(response, result)
        
        kw = dict(_name='errorInCommit', 
                    _responseHeaders=dict(_status="200 OK"), _environ={})
        result = root.handleHTTPRequest(kw)
        self.assertEquals(kw['_responseHeaders']['_status'], '%d %s' % (503, root.statusMessages[503]))
        response = 'badCommit: True'
        self.assertEquals(response, result)

    def testErrorHandling(self):
        self._testErrorHandling()
        #even through this test generates an error during commit, our 2phase commit logic
        #lets us recover and there should only be warning messages but no critical ones
        self.failUnless('CRITICAL' not in [r.levelname for r in self.logHandler.records])
        self.failUnless('WARNING' in [r.levelname for r in self.logHandler.records])
        
        #use a model that doesn't support 2phase commit (i.e. doesn't support updateAdvisory) 
        self.failUnless(not RxPath.TransactionMemModel.updateAdvisory)        
        self._testErrorHandling(appVars=dict(modelFactory=RxPath.TransactionMemModel))   
        #now we will have a critical error
        self.failUnless('CRITICAL' in [r.levelname for r in self.logHandler.records])     
        
    def testUpdatesApp(self):
        root = raccoon.RequestProcessor(a='testUpdatesApp.py',model_uri = 'test:')
        #set timestamp to 0 so tests are reproducible:
        root.domStore.model.createTxnTimestamp = lambda *args: 0
        
        #set a notifyChangeset hook to test if its called properly
        self.notifyChangeset = None
        def testNotifyChangeset(changeset):
            self.notifyChangeset = changeset
            def x(d):
                return ['%s=%s' % (k,v) for k,v in sorted(d.items()) 
                                                    if k != 'statements']
            diff = utils.pprintdiff(x(changeset), x(expectedChangeSet))                    
            self.assertEquals(changeset, expectedChangeSet, diff)
        root.domStore.model.notifyChangeset = testNotifyChangeset
        
        self.failUnless(root.loadModelHookCalled)
        self.failUnless(not root.domStore.model._currentTxn)        
        self.assertEquals(root.domStore.model.currentVersion, '0')
        
        #the "foo" request handler defined in testUpdatesApp.py adds content to the store:
        result = root.runActions('http-request', dict(_name='foo'))
        response = "<html><body>page content.</body></html>"        
        self.assertEquals(response, result)        
        self.assertEquals(root.domStore.model.currentVersion, '0A00001')
        
        self.assertEquals(root.updateResults['_added']['data'], 
            [{'comment': u'page content.', 'id': 'a_resource', 'label': 'foo'}])
        self.assertEquals(root.updateResults['_addedStatements'], 
            [('a_resource', 'comment', 'page content.', 'L', ''), 
            ('a_resource', 'label', 'foo', 'L', '')])
        self.assertEquals(root.updateResults['_removedStatements'], [])
        self.assertEquals(root.updateResults['_removed']['data'], [])        
        self.failUnless(self.notifyChangeset)
        
        #this request doesn't update the store so updateResults should be empty
        root.updateResults = {}
        result = root.runActions('http-request', dict(_name='jj'))                
        self.assertEquals('<html><body>not found!</body></html>', result)
        self.assertEquals(root.domStore.model.currentVersion, '0A00001')
        
        self.failUnless('_addedStatements' not in root.updateResults, root.updateResults)
        self.failUnless('_added' not in root.updateResults)
        self.failUnless('_removedStatements' not in root.updateResults)
        self.failUnless('_removed' not in root.updateResults)
        
        try:
            #merging own changeset should throw an error:
            root.domStore.merge(self.notifyChangeset)
        except RuntimeError, e:
            self.assertEquals(str(e), 'merge received changeset from itself: 0A')
        else:
            self.fail('should have raised an error')

        root2= raccoon.RequestProcessor(a='testUpdatesApp.py',model_uri = 'test:', appVars={'branchId':'0B'})
        self.failUnless( root2.domStore.merge(self.notifyChangeset) )
        
        #XXX merging same changeset again should be an no-op
        #self.failUnless( root2.domStore.merge(self.notifyChangeset) )
        
        self.assertEquals(root2.domStore.query("{*}").results, 
            [{'comment': 'page content.', 'id':  'a_resource', 'label': 'foo'}])
    
    def testMerge(self):
        store1 = raccoon.createStore(saveHistory='split', branchId='B',BASE_MODEL_URI = 'test:')
        self.assertEquals(store1.model.currentVersion, '0')        
        store1.add([{ 'id' : '1',
          'base': [ {'foo': 1}, {'foo': 2}]
        }
        ])
        self.assertEquals(store1.model.currentVersion, '0B00001')
        
        store1.update([{ 'id' : '1',
          'base': [{'foo': 3}, {'foo': 4}]
        }
        ])
        self.assertEquals(store1.model.currentVersion, '0B00002')

        self.assertEquals(store1.query("{*}", debug=1).results, 
            [{'base': [{'foo': 3}, {'foo': 4}], 'id': '1'}])
        
        #merge a different branch changeset based on the origin (empty) revision
        #this causes a simple merge 
        store1.merge(expectedChangeSet)
        self.assertEquals(store1.model.currentVersion, '0A00001,0B00003')
        self.assertEquals(store1.query("{*}").results, 
            [{'comment': 'page content.', 'id': 'a_resource', 'label': 'foo'}, 
                {'base': [{'foo': 3}, {'foo': 4}], 'id': '1'}])

    def testMergeConflict(self):        
        store1 = raccoon.createStore(saveHistory='split', branchId='B',BASE_MODEL_URI = 'test:')
        store1.add([{ 'id' : 'a_resource',
           'newprop' : 'change an existing resource'
        }])
        self.assertEquals(store1.model.currentVersion, '0B00001')
        
        #XXX
        #expectedChangeSet modifies the same resource causing a merge conflict
        #store1.merge(expectedChangeSet)
        #self.assertEquals(store1.model.currentVersion, '0A00001,0B00002')

    def testMergeInTransaction(self):
        #same as testMerge but in a transaction (mostly to test domstore
        #methods inside a transaction)
        store1 = raccoon.createStore(saveHistory='split', branchId='B',BASE_MODEL_URI = 'test:')
        root1 = store1.requestProcessor
        self.assertEquals(store1.model.currentVersion, '0')        
        root1.txnSvc.begin()
        store1.add([{ 'id' : '1',
          'base': [ {'foo': 1}, {'foo': 2}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0B00001')

        root1.txnSvc.begin()
        store1.update([{ 'id' : '1',
          'base': [{'foo': 3}, {'foo': 4}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0B00002')

        self.assertEquals(store1.query("{*}", debug=1).results, 
            [{'base': [{'foo': 3}, {'foo': 4}], 'id': '1'}])

        root1.txnSvc.begin()
        store1.merge(expectedChangeSet)
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00001,0B00003')

        self.assertEquals(store1.query("{*}").results, 
            [{'comment': 'page content.', 'id': 'a_resource', 'label': 'foo'}, 
                {'base': [{'foo': 3}, {'foo': 4}], 'id': '1'}])

    class TxnTestState(object):
        def __init__(self, store):
            self.store = store
            self.testParticipantCommitCalled = False
            self.assertError = None
            self.participant = transactions.TransactionParticipant()

    def _run2PhaseTxnTest(self, testState, commitFunc, testFunc):
        #if one participant fails, make sure the other participants' changes are rollbacked        
        store = testState.store
        root = store.requestProcessor
        testState.participant.voteForCommit = commitFunc
                
        store.update([{ 'id' : '1',
          'prop' : 1
        }
        ])   
        initialStmts = store.model.getStatements()        

        try:
            root.executeTransaction(testFunc)
        except Exception, e:
            if e.message != "voteForCommit failed":
                raise
            self.assertEqual(e.message,"voteForCommit failed")
        else:
            self.failUnless(False, 'didnt see expected exception')
        
        if testState.assertError:
            type, value, tb = testState.assertError
            raise type, value, tb
        
        self.failUnless(testState.testParticipantCommitCalled)
        #this transaction failed so model statements should not have changed
        self.assertEqual(initialStmts, store.model.getStatements())
        
    def _test2PhaseTxn(self, saveHistory):
        '''
        Test updates with multiple transaction participants
        '''
        from rx import DomStore
        store = raccoon.createStore(saveHistory=saveHistory, BASE_MODEL_URI = 'test:')        
        root = store.requestProcessor        
        if saveHistory == 'split':
            graphParticipants = 2            
        elif saveHistory == 'combined':
            graphParticipants = 1
        else:
            graphParticipants = 0
        
        def voteForCommit(txnService):
            testState.testParticipantCommitCalled = True
            try:
                #there are 3 participants: this, domstore, and the model
                #the one participant is the model
                self.assertEquals(len(store._txnparticipants), 
                            1 + graphParticipants, store._txnparticipants)
                self.assertEquals(len(root.txnSvc.state.participants), 
                        3 + graphParticipants, root.txnSvc.state.participants)
                
                if saveHistory == 'split':
                    stmts = store._txnparticipants[-2].undo 
                elif saveHistory == 'combined':
                    stmts = [s for s in store._txnparticipants[-1].undo 
                            if s[0] is RxPath.Removed and not s[1][4] or len(s) == 1 and not s[0][4] ]
                else:
                    stmts = store._txnparticipants[-1].undo
                
                self.assertEqual(stmts,  [
                    (RxPath.Removed, ('1', 'prop', u'1', 
                    'http://www.w3.org/2001/XMLSchema#integer', '')), 
                    (('1', 'prop', u'2', 'http://www.w3.org/2001/XMLSchema#integer', ''),), 
                    (('2', 'prop', 'test', 'R', ''),)
                ])
                
                if before:
                    #this commit is called before so the store shouldn't have been committed yet
                    self.failUnless(isinstance(store._txnparticipants[-1], 
                                                DomStore.TwoPhaseTxnModelAdapter))
                    self.failUnless( not store._txnparticipants[-1].committed )
                else:
                    self.failUnless(store._txnparticipants[-1].committed )
            except:
                #need to capture these so they dont get swallow by txn exception handler
                testState.assertError =  sys.exc_info()
            raise RuntimeError('voteForCommit failed')
        
        def testFail():
            if before:
                #join before domStore
                #this way participant's vote will fail before the model committed            
                testState.participant.join(root.txnSvc)
            #generate adds and removes
            store.update([{ 'id' : '1', 'prop' : 2}, 
            { 'id' : '2', 'prop' : '@test'}
            ])                        
            if not before:
                #join after domStore
                #this way participant's vote will fail after the model committed
                testState.participant.join(root.txnSvc)            
        
        testState = self.TxnTestState(store)
        before = True
        self._run2PhaseTxnTest(testState, voteForCommit, testFail)
        testState = self.TxnTestState(store)
        before = False
        self._run2PhaseTxnTest(testState, voteForCommit, testFail)
        if saveHistory: 
            self.assertEqual(store.model.currentVersion, '0A00001')
        else:
            self.failUnless(isinstance(root.domStore.model, DomStore.ModelWrapper))
        
    def test2PhaseTxn(self):
        '''
        Test updates with multiple transaction participants
        '''
        self._test2PhaseTxn(False)
        
    def test2PhaseGraphManagerTxn(self):
        '''
        Test revision history updates with multiple transaction participants
        '''        
        self._test2PhaseTxn('split')
        self._test2PhaseTxn('combined')

    def testCreateApp(self):
        #this is minimal logconfig that python's logger seems to accept:
        app = raccoon.createApp(static_path=['static'], 
          logconfig = '''
          [loggers]
          keys=root

          [handlers]
          keys=hand01

          [formatters]
          keys=form01

          [logger_root]
          level=DEBUG
          handlers=hand01

          [handler_hand01]
          class=StreamHandler
          level=NOTSET
          formatter=form01
          args=(sys.stdout,)

          [formatter_form01]
          format=%(asctime)s %(levelname)s %(name)s %(message)s
          datefmt=%d %b %H:%M:%S
          '''
        )
        root = app.load()
        self.failUnless(root)
        
if __name__ == '__main__':
    import sys    
    try:
        logarg=sys.argv[sys.argv.index("-l")+1]
        loglevel = getattr(logging, logarg.upper(), None)
        if not loglevel:
            loglevel = int(logarg)
        LogLevel = loglevel
    except (IndexError, ValueError):
        pass

    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = RaccoonTestCase(test)
        tc.setUp()
        getattr(tc, test)() #run test
        tc.tearDown()
