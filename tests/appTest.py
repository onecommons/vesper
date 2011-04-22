#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    request processor unit tests
"""
import vesper.app, vesper.web
from vesper import utils
from vesper.data import transactions, base
from vesper.data.store.basic import TransactionMemStore

import logging
import unittest, glob, os, os.path, traceback
 
def makeChangeset(branchid, rev, baserevision='0', resname='a_resource'):
    return {'origin': branchid, 'timestamp': 0,
        'baserevision': baserevision, 'revision': rev,
        'statements': 
 [base.Statement(*s) for s in [('context:txn:test:;'+rev, u'http://rx4rdf.sf.net/ns/archive#baseRevision', u'0', 'L', 'context:txn:test:;'+rev), 
 ('context:txn:test:;'+rev, u'http://rx4rdf.sf.net/ns/archive#hasRevision', rev, 'L', 'context:txn:test:;'+rev), 
 ('context:txn:test:;'+rev, u'http://rx4rdf.sf.net/ns/archive#createdOn', u'0', 'L', 'context:txn:test:;'+rev), 
 ('context:txn:test:;'+rev, u'http://rx4rdf.sf.net/ns/archive#includes', 'context:add:context:txn:test:;'+rev+';;', 'R', 'context:txn:test:;'+rev), 
 ('context:add:context:txn:test:;0A00001;;', u'http://rx4rdf.sf.net/ns/archive#applies-to', '', 'R', 'context:txn:test:;'+rev), 
 ('context:txn:test:;'+rev, u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', u'http://rx4rdf.sf.net/ns/archive#TransactionContext', 'R', 'context:txn:test:;'+rev),
 (resname, 'comment', 'page content.', 'L', 'context:add:context:txn:test:;'+rev+';;'), 
 (resname, 'label', 'foo', 'L', 'context:add:context:txn:test:;'+rev+';;')]] }

LogLevel = logging.CRITICAL+10

class VoteForCommitException(Exception):
    pass
    
# python 2.4 explodes on unregistering loghandlers which it registers through
# atexit, so purposefully break atexit
import sys
if sys.version_info[:2] < (2,5):
    import atexit
    sys.exitfunc = lambda: 0

class AppTestCase(unittest.TestCase):
    
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
        app = vesper.app.createApp(baseapp='testMinimalApp.py', model_uri='test:')
        app.load()
        root = app._server
        result = root.runActions('http-request', dict(_name='foo'))
        response = "<html><body>page content.</body></html>"
        self.assertEquals(response, result)
        
        #XXX test for InputSource
        #result = app.InputSource.DefaultFactory.fromUri(
        #    'site:///foo', resolver=root.resolver).read()    
        #print type(result), repr(result), result
        self.assertEquals(response, result)
        
        result = root.runActions('http-request', dict(_name='jj'))
        #print type(result), result
        self.assertEquals( '<html><body>not found!</body></html>', result)

    def testSequencerApp(self):
        app = vesper.app.createApp(baseapp='testSequencerApp.py', model_uri='test:')
        app.load()
        root = app._server
        result = root.runActions('http-request', dict(_name='foo'))
        self.assertEquals("<html><body>page content.</body></html>", result)

        app = vesper.app.createApp(baseapp='testSequencerApp.py', model_uri='test:')
        app.load()
        root = app._server
        # XXX createApp doesn't specify whether to create a RequestProcessor or HTTPRequestProcessor
        # root = web.HTTPRequestProcessor(a='testSequencerApp.py',model_uri = 'test:')
        kw = dict(_name='no such page', _responseHeaders={}, _environ={})
        result = root.handleHTTPRequest(kw)
        self.assertEquals(kw['_responseHeaders']['_status'],"404 Not Found")

        kw = dict(_name='/static/testfile.txt', 
            _responseHeaders=dict(_status="200 OK"), _environ={})
        result = root.handleHTTPRequest(kw)
        self.assertEquals(result.read().strip(), "test file")
        

    def _testErrorHandling(self, appVars=None):
        app = vesper.app.createApp(baseapp='testErrorHandling-config.py', model_uri='test:', **(appVars or {}))
        app.load()
        root = app._server
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
        self.failUnless(not TransactionMemStore.updateAdvisory)
        self._testErrorHandling(appVars=dict(model_factory=TransactionMemStore))
        #now we will have a critical error
        self.failUnless('CRITICAL' in [r.levelname for r in self.logHandler.records])
        
    def testUpdatesApp(self):
        # root = app.RequestProcessor(a='testUpdatesApp.py',model_uri = 'test:')
        app = vesper.app.createApp(baseapp='testUpdatesApp.py', model_uri='test:')        
        app.load()        
        root = app._server
        dataStore = root.defaultStore
        #print 'model', dataStore.model.managedModel.model
        #set timestamp to 0 so tests are reproducible:
        dataStore.model.createTxnTimestamp = lambda *args: 0
        expectedChangeSet = makeChangeset('0A', '0A00001')
        
        #set a notifyChangeset hook to test if its called properly
        self.notifyChangeset = None
        def testNotifyChangeset(changeset):
            self.notifyChangeset = changeset
            def x(d):
                return ['%s=%s' % (k,v) for k,v in sorted(d.items()) 
                                                    if k != 'statements']
            diff = utils.pprintdiff(x(changeset), x(expectedChangeSet))                    
            self.assertEquals(changeset, expectedChangeSet, diff)
        dataStore.model.notifyChangeset = testNotifyChangeset
        
        self.failUnless(root.loadModelHookCalled)
        self.failUnless(not dataStore.model._currentTxn)        
        self.assertEquals(dataStore.model.currentVersion, '0')
        
        #the "foo" request handler defined in testUpdatesApp.py adds content to the store:
        result = root.runActions('http-request', dict(_name='foo'))
        response = "<html><body>page content.</body></html>"
        self.assertEquals(response, result)        
        self.assertEquals(dataStore.model.currentVersion, '0A00001')
        
        self.assertEquals(root.updateResults['_added']['data'], 
            [{'comment': u'page content.', 'id': '@a_resource', 'label': 'foo'}])
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
        self.assertEquals(dataStore.model.currentVersion, '0A00001')
        
        self.failUnless('_addedStatements' not in root.updateResults, root.updateResults)
        self.failUnless('_added' not in root.updateResults)
        self.failUnless('_removedStatements' not in root.updateResults)
        self.failUnless('_removed' not in root.updateResults)
        
        try:
            #merging our own changeset in again should throw an error:
            dataStore.merge(self.notifyChangeset)
        except RuntimeError, e:
            self.assertEquals(str(e), 'merge received changeset from itself: 0A')
        else:
            self.fail('should have raised an error')
        
        app2 = vesper.app.createApp(baseapp='testUpdatesApp.py', model_uri='test:', trunk_id='0A', branch_id='0B')
        app2.load()
        root2 = app2._server
        self.failUnless( root2.defaultStore.merge(self.notifyChangeset) )
                
        self.assertEquals(root2.defaultStore.query("{*}"), 
            [{'comment': 'page content.', 'id':  '@a_resource', 'label': 'foo'}])

    def testRemoves(self):
        store = vesper.app.createStore({
        "id": "hello", 
        "tags": [
          { "id": "tag1" }, 
          { "id": "tag2"}
        ]})
        #print 'store', store.query('{*}')        
        #utils.debugp( 'testremove', store.model.by_s)                
        #print 'hello', store.model.by_s.get('hello')
        
        #a non-null value will only remove that particular value
        store.remove({"id":"hello","tags":"@tag2"})
        #utils.debugp('hello', store.model.by_s.get('hello'))
        from vesper import pjson
        self.assertEquals(pjson.tojson(store.model.getStatements())['data'],
            [{"id": "@hello", 
               "tags": ["@tag1"]
            }])            
        self.assertEquals(store.query('{*}'), [{'id': '@hello', 'tags': ['@tag1']}])
        
        #null value will remove the property and any associated values
        store.remove({"id":"hello","tags":None})
        self.assertEquals(pjson.tojson(store.model.getStatements())['data'],
            [{"id": "@hello" }])
        self.assertEquals(store.query('{*}'), [{'id': '@hello'}])
        
        #strings are treated as resource ids and the entire object is removed
        store.remove(["@hello"])
        self.assertEquals(pjson.tojson(store.model.getStatements())['data'], [])
        self.assertEquals(store.query('{*}'), [])        

    def testUpdate(self):
        store = vesper.app.createStore({
        "id": "hello", 
        "tags": [
          { "id": "tag1" }, 
          { "id": "tag2"}
        ]})
        #print 'store', store.query('{*}')        
        #utils.debugp( 'testremove', store.model.by_s)                
        #print 'hello', store.model.by_s.get('hello')
        store.update({"id":"hello","tags":"@tag1"})
        #utils.debugp('hello', store.model.by_s.get('hello'))
        from vesper import pjson
        self.assertEquals(pjson.tojson(store.model.getStatements())['data'],
            [{"id": "@hello", 
               "tags": ["@tag1"]
            }])            
        self.assertEquals(store.query('{*}'), [{'id': '@hello', 'tags': ['@tag1']}])

    def _testUpdateEmbedded(self, op):
        updates = utils.attrdict()        
        @vesper.app.Action
        def recordUpdates(kw, retval):
            updates.update(kw)
        
        store = vesper.app.createStore({
        "id": "hello", 
        "embedded": {
          'foo' : 'not first-class',
          'prop2' : 'a'
        }
         }, storage_template_options=dict(generateBnode='counter'),
         actions = { 'after-commit' : [recordUpdates]}         
        )
        self.assertEquals(store.model.getStatements(),
            [('_:j:e:object:hello:1', 'foo', 'not first-class', 'L', ''),
             ('_:j:e:object:hello:1', 'prop2', 'a', 'L', ''), 
            ('hello', 'embedded', '_:j:e:object:hello:1', 'R', '')])
         
        getattr(store, op)({"id":"hello",
        "embedded": {
          'id' : '_:j:e:object:hello:2', #here just to make sure a different bnode name is used
          'foo' : 'modified',
           'prop2' : 'a'
        }
        })
        
        #XXX this statement shouldn't be duplicated
        self.assertEquals(updates._removedStatements, [
            ('_:j:e:object:hello:1', 'foo', 'not first-class', 'L', ''), 
            ('_:j:e:object:hello:1', 'foo', 'not first-class', 'L', '')])            
        self.assertEquals(updates._addedStatements, 
                  [('_:j:e:object:hello:1', 'foo', 'modified', 'L', '')])
        self.assertEquals(store.model.getStatements(),
            [('_:j:e:object:hello:1', 'foo', 'modified', 'L', ''), 
             ('_:j:e:object:hello:1', 'prop2', 'a', 'L', ''), 
            ('hello', 'embedded', '_:j:e:object:hello:1', 'R', '')]
        )

    def testUpdateEmbedded(self):
        self._testUpdateEmbedded('update')

    def testReplaceEmbedded(self):
        #calling replace instead of update results in the same set of changes
        #in our example but goes through a different code path
        self._testUpdateEmbedded('replace')
                
    def testUpdateEmbeddedList(self):
        updates = utils.attrdict()        
        @vesper.app.Action
        def recordUpdates(kw, retval):
            updates.update(kw)

        store = vesper.app.createStore({
        "id": "hello", 
        "embedded": [{'foo' : 'a', 'prop2':'a'}, {'foo' : 'b'}]
         }, storage_template_options=dict(generateBnode='counter'),
         actions = { 'after-commit' : [recordUpdates]}         
        )

        store.update({"id":"hello",
          "embedded": [{'id' : '_:j:e:object:hello:3', 'foo' : 'c', 'prop2':'a'}, {'id' : '_:j:e:object:hello:4', 'foo' : 'd'}]        
        })

        #XXX these statements shouldn't be duplicated
        removed = [('_:j:e:object:hello:1', 'foo', 'a', 'L', ''), 
        ('_:j:e:object:hello:2', 'foo', 'b', 'L', ''), 
        ('_:j:e:object:hello:1', 'foo', 'a', 'L', ''), 
        ('_:j:e:object:hello:2', 'foo', 'b', 'L', '')] 
 
        #XXX shouldn't include the redundant proplist statements
        added = [('_:j:e:object:hello:1', 'foo', 'c', 'L', ''), ('_:j:e:object:hello:2', 'foo', 'd', 'L', ''), 
('_:j:proplist:hello;embedded', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq', 'R', ''), 
('_:j:proplist:hello;embedded', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'pjson:schema#propseqtype', 'R', ''), 
('_:j:proplist:hello;embedded', 'pjson:schema#propseqprop', 'embedded', 'R', ''), 
('_:j:proplist:hello;embedded', 'pjson:schema#propseqsubject', 'hello', 'R', '')]
        
        self.assertEquals(updates._removedStatements, removed)        
        self.assertEquals(updates._addedStatements, added)

    def testReplaceEmbeddedList(self):
        updates = utils.attrdict()        
        @vesper.app.Action
        def recordUpdates(kw, retval):
            updates.update(kw)

        store = vesper.app.createStore({
        "id": "hello", 
        "embedded": [{'foo' : 'a'}, {'foo' : 'b'}]
         }, storage_template_options=dict(generateBnode='counter'),
         actions = { 'after-commit' : [recordUpdates]}         
        )

        store.replace({"id":"hello",
          "embedded": [{'id' : '_:j:e:object:hello:3', 'foo' : 'c'}, {'id' : '_:j:e:object:hello:4', 'foo' : 'd'}]        
        })

        removed = set(updates._removedStatements) - set(updates._addedStatements)
        expectedRemoved = set([('_:j:e:object:hello:2', 'foo', 'b', 'L', ''),
                                ('_:j:e:object:hello:1', 'foo', 'a', 'L', '')])
        self.assertEquals(removed, expectedRemoved)        
        
        added = set(updates._addedStatements) - set(updates._removedStatements)        
        expectedAdded = set([('_:j:e:object:hello:2', 'foo', 'd', 'L', ''), 
                            ('_:j:e:object:hello:1', 'foo', 'c', 'L', '')])
        self.assertEquals(added, expectedAdded)

    def testMerge(self):
        store1 = vesper.app.createStore(save_history='split', branch_id='A',
          model_uri = 'test:', 
          #XXX merging needs to check for bnode equivalency
          #it doesn't right now so set 'counter' so bnode names are deterministic
          storage_template_options=dict(generateBnode='counter')
          )
        self.assertEquals(store1.model.currentVersion, '0')
                
        store1.add([{ 'id' : '1', 'prop' : 0,
          'base': [ {'foo': 1}, {'foo': 2}]
        }
        ])
        self.assertEquals(store1.model.currentVersion, '0A00001')
        
        store1.update([{ 'id' : '1', 'prop' : 1,
          'base': [{'foo': 3}, {'foo': 4}]
        }
        ])
        self.assertEquals(store1.model.currentVersion, '0A00002')
        import sys
        self.assertEquals(store1.query("{* namemap={refpattern:''}}", debug=0), 
            [{'base': [{'foo': 3}, {'foo': 4}],  'prop' : 1, 'id': '1'}])
        
        try:
            #merging a changeset from a different trunk should throw an error:
            store1.merge( makeChangeset('0B', '0B00001') )
        except RuntimeError, e: 
            self.assertEquals(str(e), 
            'can not add a changeset that does not share trunk branch: 0A00002, 0B00001')
            #print 'stmts', store1.model.revisionModel.getStatements()
        else:
            self.fail('should have raised an error')
        self.assertEquals(list(store1.model.getRevisions()), [u'0A00001', u'0A00002'])
         
        #merge a different branch changeset whose base revision is the current tip 
        #is this case, the changeset will be added but no merge changeset is created
        store1.merge(makeChangeset('0B', '0A00002,0B00001', '0A00002'))
        
        self.assertEquals(store1.model.currentVersion, '0A00002,0B00001')
        self.assertEquals(store1.model.getCurrentContextUri(), 'context:txn:test:;0A00002,0B00001')        
        self.assertEquals(store1.query("{* namemap={refpattern:''}}"), 
            [{'base': [{'foo': 3}, {'foo': 4}], 'prop' : 1, 'id': '1'},
            {'comment': 'page content.', 'id': 'a_resource', 'label': 'foo'}
            ])
        
        contexts = store1.model.getRevisionContextsForResource('1')
        self.assertEquals(contexts, ['context:txn:test:;0A00001', 'context:txn:test:;0A00002'])        
        #diff the latest revision with the first revision for resource "1":
        self.assertEquals(
            store1.model.getStatementsForResourceVisibleAtRevision('1', 99)
            - store1.model.getStatementsForResourceVisibleAtRevision('1', 0),
            set([('1', 'prop', u'1', 'http://www.w3.org/2001/XMLSchema#integer',
                    'context:add:context:txn:test:;0A00002;;')])
        )

        #a_resource was modified in B1, '1' in A1 and B2
        self.assertEquals(['1', 'a_resource'], store1.model.isModifiedAfter(
            'context:txn:test:;0A00001', ['a_resource', '1'], False))

        self.assertEquals([], store1.model.isModifiedAfter(
            store1.model.getCurrentContextUri(), ['a_resource', '1'], False))
        
        self.assertEquals([], store1.model.isModifiedAfter(
            store1.model.getCurrentContextUri(), ['a_resource', '1'], True))
        
        from vesper.data.base.graph import MergeableGraphManager
        self.assertEquals(-1, MergeableGraphManager.comparecontextversion(
                'context:txn:test:;0A00001,0B00003', 'context:txn:test:;0B00002'))

        #merge a different branch changeset whose base revision is the current tip 
        #in this case, the changeset will be added but no merge changeset is created
        self.assertTrue( store1.merge(makeChangeset('0C', '0A00002,0C00001', '0A00002', 'unrelated_resource')) )
        self.assertEquals(store1.query("{* where (id='unrelated_resource') namemap={refpattern:''} }"), 
            [{'comment': 'page content.', 'id': 'unrelated_resource', 'label': 'foo'}])
                
    def testMergeConflict(self):        
        store1 = vesper.app.createStore(save_history='split', branch_id='B',model_uri = 'test:')
        store1.add([{ 'id' : 'a_resource',
           'newprop' : 'change an existing resource'
        }])
        self.assertEquals(store1.model.currentVersion, '0B00001')
        
        #XXX
        #expectedChangeSet modifies the same resource causing a merge conflict
        #store1.merge(expectedChangeSet)
        #self.assertEquals(store1.model.currentVersion, '0A00001,0B00002')

    def testMergeInTransaction(self):
        #same as testMerge but in a transaction (mostly to test datastore
        #methods inside a transaction)
        store1 = vesper.app.createStore(save_history='split', branch_id='A',
         model_uri = 'test:', storage_template_options=dict(generateBnode='counter'))
        root1 = store1.requestProcessor
        self.assertEquals(store1.model.currentVersion, '0')        
        root1.txnSvc.begin()
        store1.add([{ 'id' : '1',
          'base': [ {'foo': 1}, {'foo': 2}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00001')

        root1.txnSvc.begin()
        store1.update([{ 'id' : '1',
          'base': [{'foo': 3}, {'foo': 4}]
        }
        ])
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00002')

        self.assertEquals(store1.query("{* namemap={refpattern:''}}", debug=1).results, 
            [{'base': [{'foo': 3}, {'foo': 4}], 'id': '1'}])

        root1.txnSvc.begin()
        store1.merge(makeChangeset('0B', '0A00002,0B00001', '0A00002'))
        root1.txnSvc.commit()
        self.assertEquals(store1.model.currentVersion, '0A00002,0B00001')
        self.assertEquals(store1.model.getCurrentContextUri(), 'context:txn:test:;0A00002,0B00001')

        self.assertEquals(store1.query("{* namemap={refpattern:''}}"), 
            [{'base': [{'foo': 3}, {'foo': 4}], 'id': '1'},
             {'comment': 'page content.', 'id': 'a_resource', 'label': 'foo'}
        ])

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
        except VoteForCommitException:
            pass
        else:
            self.failUnless(False, 'didnt see expected exception')
        
        if testState.assertError:
            type, value, tb = testState.assertError
            raise type, value, tb
        
        self.failUnless(testState.testParticipantCommitCalled)
        #this transaction failed so model statements should not have changed
        self.assertEqual(initialStmts, store.model.getStatements())
        
    def _test2PhaseTxn(self, save_history):
        '''
        Test updates with multiple transaction participants
        '''
        from vesper.data import DataStore
        store = vesper.app.createStore(save_history=save_history, model_uri = 'test:')
        root = store.requestProcessor        
        if save_history == 'split':
            graphParticipants = 2            
        elif save_history == 'combined':
            graphParticipants = 1
        else:
            graphParticipants = 0
        
        def voteForCommit(txnService):
            testState.testParticipantCommitCalled = True
            try:
                #there are 3 participants: this, datastore, and the model
                #the one participant is the model
                self.assertEquals(len(store._txnparticipants), 
                            1 + graphParticipants, store._txnparticipants)
                self.assertEquals(len(root.txnSvc.state.participants), 
                        3 + graphParticipants, root.txnSvc.state.participants)
                
                if save_history == 'split':
                    stmts = store._txnparticipants[-2].undo 
                elif save_history == 'combined':
                    stmts = [s for s in store._txnparticipants[-1].undo 
                            if s[0] is base.Removed and not s[1][4] or len(s) == 1 and not s[0][4] ]
                else:
                    stmts = store._txnparticipants[-1].undo
                
                self.assertEqual(stmts,  [
                    (base.Removed, ('1', 'prop', u'1', 
                    'http://www.w3.org/2001/XMLSchema#integer', '')), 
                    (('1', 'prop', u'2', 'http://www.w3.org/2001/XMLSchema#integer', ''),), 
                    (('2', 'prop', 'test', 'R', ''),)
                ])
                
                if before:
                    #this commit is called before so the store shouldn't have been committed yet
                    self.failUnless(isinstance(store._txnparticipants[-1], 
                                                DataStore.TwoPhaseTxnModelAdapter))
                    self.failUnless( not store._txnparticipants[-1].committed )
                else:
                    self.failUnless(store._txnparticipants[-1].committed )
            except:
                #need to capture these so they dont get swallow by txn exception handler
                testState.assertError =  sys.exc_info()
            raise VoteForCommitException()
        
        def testFail():
            if before:
                #join before dataStore
                #this way participant's vote will fail before the model committed            
                testState.participant.join(root.txnSvc)
            #generate adds and removes
            store.update([{ 'id' : '1', 'prop' : 2}, 
            { 'id' : '2', 'prop' : '@test'}
            ])                        
            if not before:
                #join after dataStore
                #this way participant's vote will fail after the model committed
                testState.participant.join(root.txnSvc)            
        
        testState = self.TxnTestState(store)
        before = True
        self._run2PhaseTxnTest(testState, voteForCommit, testFail)
        testState = self.TxnTestState(store)
        before = False
        self._run2PhaseTxnTest(testState, voteForCommit, testFail)
        if save_history: 
            self.assertEqual(store.model.currentVersion, '0A00001')
        else:
            self.failUnless(isinstance(store.model, DataStore.ModelWrapper))
        
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

    def testGraphManagerSerialize(self):
        import os
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning) #suppress tempnam is insecure warning
        tmppath = os.tempnam(None, 'vespertest')+'.json'
        try:
            add = {"id":"1",
                    "tags":[
                        {"id":"bnode:j:e:object:bnode:j:t:object:x28a11e57f6c54979baaeefee93de4f2f1:x28a11e57f6c54979baaeefee93de4f2f2",
                        "label":"new tag"}
                        ]
                  }
            add = {"id":"1",                    
                  "tags" : [ '@t' ]
                  }            
            store = vesper.app.createStore(storage_path=tmppath, storage_url='', 
                model_factory=vesper.data.store.basic.FileStore,model_uri = 'test:', save_history=True)
            store.add(add)
            
            #now reload the file into a new store without save_history turned on,
            #so that the history representation is visible to the app
            #print file(tmppath).read()            
            store2 = vesper.app.createStore(storage_path=tmppath, storage_url='', 
                model_factory=vesper.data.store.basic.FileStore,model_uri = 'test:', save_history=False)
            #XXX jql is not including scope in results
            self.assertEquals(store2.query("{* where (id='1')}"), [{'id': '@1', 'tags': ['@t', '@t']}])            
        finally:
            os.unlink(tmppath)
        
              
    def testCreateApp(self):
        #this is minimal logconfig that python's logger seems to accept:
        app = vesper.app.createApp(static_path=['static'], 
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

    def testMultipleStores(self):
        app = vesper.app.createApp(stores = { 
        'config' : dict(storage_template='''{
            "id" : "c1", "prop" : 1
        }'''),
          'data' : dict(default_store=True, storage_template='''{
          "id" : "d1", "prop" : 1
        }''')
        })
        root = app.load()
        self.failUnless(root and root.defaultStore is root.stores.data)
        
        def transaction():
            root.stores.config.update(dict(id='c2', prop2=1))
            root.stores.data.update(dict(id='d1', prop2=1))
        root.executeTransaction(transaction)
        
        cdata = [{'id': '@c2', 'prop2': 1}, {'id': '@c1', u'prop': 1}]
        ddata = [{'id': '@d1', 'prop2': 1, u'prop': 1}]        
        self.assertEquals(root.stores.config.query("{*}"), cdata)
        self.assertEquals(root.defaultStore.query("{*}"), ddata)
        
        def transaction2():
            root.stores.config.update(dict(id='c2', prop3=1))
            root.stores.data.update(dict(id='d1', prop3=1))
            raise Error()
        try:
            root.executeTransaction(transaction2)
        except:
            pass
        else:
            self.fail('should have raised an error')
        self.assertEquals(root.stores.config.query("{*}"), cdata)
        self.assertEquals(root.defaultStore.query("{*}"), ddata)
        
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
        tc = AppTestCase(test)
        tc.setUp()
        getattr(tc, test)() #run test
        tc.tearDown()
