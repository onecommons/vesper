#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

from vesper.data.base import *
from vesper.data import base
from vesper.data.base import graph
from vesper.data.store.basic import *

graphManagerClass = graph.MergeableGraphManager
#graphManagerClass = graph.NamedGraphManager

def random_name(length):
    return ''.join(random.sample(string.ascii_letters, length))

class BasicModelTestCase(unittest.TestCase):
    "Tests basic features of the tyrant model class"    
    persistentStore = True

    def _getModel(self, model):
        return model

    def getModel(self):
        model = MemStore()
        self.persistentStore = False
        return self._getModel(model)

    def getTransactionModel(self):
        model = TransactionMemStore()
        self.persistentStore = False
        return self._getModel(model)
    
    def testStore(self):
        "basic storage test"
        model = self.getModel()

        # confirm a randomly created subject does not exist
        subj = random_name(12)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())

        # add a new statement and confirm the search succeeds
        s1 = Statement(subj, 'pred', "obj")
        model.addStatement(s1)
        s2 = Statement(subj, 'pred2', "obj2")
        model.addStatement(s2)
        
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s2]))

        if not self.persistentStore:
            return
        
        model.commit()
        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s2]))
        
        model.removeStatement(s2)
        s3 = Statement(subj, 'pred3', "obj3")
        model.addStatement(s3)
        
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))

        model.commit()
        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))

    def testGetStatements(self):
        model = self.getModel()
                        
        stmts = [Statement('s', 'p', 'o', 'en', 'c'),
        Statement('s', 'p', 'o', 'en', 'c1'),
        Statement('s', 'p', 'o', 'en-1', 'c1'),
        Statement('s', 'p', 'o1', 'en-1', 'c1'),
        Statement('s', 'p1', 'o1', 'en-1', 'c1'),
        Statement('s1', 'p1', 'o1', 'en-1', 'c1')
        ]
        model.addStatements(stmts)
        
        conditions = ['subject', 's', 
            'predicate', 'p', 
            'object', 'o',
            'objecttype', 'en',
            'context', 'c']
        pairs = zip(*[iter(conditions)]*2)
        
        #each additional condition eliminates one of the matches
        beginMatches = 5
        while pairs:
            matches = beginMatches
            kw = {}
            for k, v in pairs:
                #first match each condition individually
                r1 = model.getStatements(**{k : v})
                self.assertEqual(len(r1), matches)             
                self.assertEqual(set(r1), set(stmts[:matches]))            
                
                kw[k] = v
                r2 = model.getStatements(**kw)
                self.assertEqual(len(r2), matches)             
                self.assertEqual(set(r2), set(stmts[:matches]))
                matches -= 1
            
            #repeat tests but start matching at next position
            pairs.pop(0)
            beginMatches -= 1
        
        more =  [
        Statement('s', 'p1', 'o2', 'en-1', 'c1'),
        Statement('s', 'p1', 'o1', 'en-1', 'c2'),
        Statement('s2', 'p1', 'o2', 'en-1', 'c2'),
        ]
        model.addStatements(more)
        
        r = model.getStatements(predicate='p1', context='c2')
        self.assertEqual(set(r), set(more[1:]) )
        
        r = model.getStatements(subject='s', predicate='p1')
        self.assertEqual(set(r), set( (more[0], more[1], stmts[-2]) ) )
        
        r = model.getStatements(predicate='p1')
        self.assertEqual(set(r), set( more + stmts[-2:] ) )
        
        r = model.getStatements(predicate='p1', object='o')
        self.assertEqual(r, [])
        
        r = model.getStatements(predicate='p1', object='o2')
        self.assertEqual(set(r), set( (more[0], more[-1]) ) )

    def testRemove(self):
        "basic removal test"
        model = self.getModel()

        # set up the model with one randomly named statement
        subj = random_name(12)
        s1 = Statement(subj, random_name(24), random_name(12))
        model.addStatement(s1)

        # confirm a search for the subject finds it
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1])) # object exists

        # remove the statement and confirm that it's gone
        model.removeStatement(s1)

        r2 = model.getStatements(subject=subj)
        self.assertEqual(set(r2), set()) # object is gone
        
        if self.persistentStore:
            model.commit()
            model = self.getModel()
            self.assertEqual(set(r2), set()) # object is gone

    def testSetBehavior(self):
        "confirm model behaves as a set"
        model = self.getModel()

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")
        s3 = Statement("ocean", "is", "blue")

        # before adding anything db should be empty
        r1 = model.getStatements()
        self.assertEqual(set(r1), set())

        # add a single statement and confirm it is returned
        model.addStatement(s1)

        model.debug = 1
        r2 = model.getStatements()
        model.debug = 0
        self.assertEqual(set(r2), set([s1]))

        # add the same statement again & the set should be unchanged
        model.addStatement(s1)
        
        r3 = model.getStatements()
        self.assertEqual(set(r3), set([s1]))
        
        # add a second statement with the same subject as s1
        model.addStatement(s2)
        
        r4 = model.getStatements()
        self.assertEqual(set(r4), set([s1, s2]))

        # add a third statement with same predicate & object as s1
        model.addStatement(s3)

        r5 = model.getStatements()
        self.assertEqual(set(r5), set([s1,s2,s3]))

    def testQuads(self):
        "test (somewhat confusing) quad behavior"
        model = self.getModel()

        # add 3 identical statements with differing contexts
        statements = [Statement("one", "two", "three", "fake", "100"),
                      Statement("one", "two", "three", "fake", "101"),
                      Statement("one", "two", "three", "fake", "102")]
        model.addStatements(statements)

        # asQuad=True should return all 3 statements
        r1 = model.getStatements(asQuad=True)
        self.assertEqual(set(r1), set(statements))

        # asQuad=False (the default) should only return the oldest
        expected = set()
        expected.add(statements[0])
        r2 = model.getStatements(asQuad=False)
        self.assertEqual(set(r2), expected)
    
    def testHints(self):
        "test limit and offset hints"
        model = self.getModel()
        
        # add 20 statements, subject strings '01' to '20'
        model.addStatements([Statement("%02d" % x, "obj", "pred") for x in range(1,21)])
        
        # test limit (should contain 1 to 5)
        r1 = model.getStatements(hints={'limit':5})
        self.assertEqual(set(r1), set([Statement("%02d" % x, "obj", "pred") for x in range(1,6)]))
        
        # test offset (should contain 11 to 20)
        r2 = model.getStatements(hints={'offset':10})
        self.assertEqual(set(r2), set([Statement("%02d" % x, "obj", "pred") for x in range(11,21)]))
        
        # test limit and offset (should contain 13 & 14)
        r3 = model.getStatements(hints={'limit':2, 'offset':12})
        self.assertEqual(set(r3), set([Statement("%02d" % x, "obj", "pred") for x in range(13,15)]))

    def testTransactionCommitAndRollback(self):
        "test simple commit and rollback on a single model instance"
        model = self.getTransactionModel()

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")

        # confirm that database is initially empty
        r1 = model.getStatements()
        self.assertEqual(set(r1), set())

        # add first statement and commit, confirm it's there
        model.addStatement(s1)
        model.commit()
        r2 = model.getStatements()
        self.assertEqual(set(r2), set([s1]))

        # add second statement and rollback, confirm it's not there
        model.addStatement(s2)
        model.rollback()
        r3 = model.getStatements()
        self.assertEqual(set(r3), set([s1]))

    def testTransactionIsolationCommit(self):
        "test commit transaction isolation across 2 models"
        modelA = self.getTransactionModel()
        modelB = self.getTransactionModel()
        
        #include spaces in value so it looks like a literal, not a resource
        statements = [Statement("one", "equals", " one "),
                      Statement("two", "equals", " two "),
                      Statement("three", "equals", " three ")]

        # confirm models are empty
        r1a = modelA.getStatements()
        r1b = modelB.getStatements()
        self.assertEqual(set(), set(r1a), set(r1b))

        # add statements and confirm A sees them and B doesn't
        modelA.addStatements(statements)
        r2a = modelA.getStatements()
        self.assertEqual(set(r2a), set(statements))
        r2b = modelB.getStatements()
        self.assertEqual(set(r2b), set())

        # commit A and confirm both models see the statements
        modelA.commit()
        r3a = modelA.getStatements()
        r3b = modelB.getStatements()
        self.assertEqual(set(statements), set(r3a), set(r3b))
        
        #reload the data
        if not self.persistentStore:
            return
            
        modelC = self.getTransactionModel()
        r3c = modelC.getStatements()
        self.assertEqual(set(statements), set(r3c))
        
    def testTransactionIsolationRollback(self):
        "test rollback transaction isolation across 2 models"
        modelA = self.getTransactionModel()
        modelB = self.getTransactionModel()

        statements = [Statement("one", "equals", "one"),
                      Statement("two", "equals", "two"),
                      Statement("three", "equals", "three")]

        # confirm models are empty
        r1a = modelA.getStatements()
        r1b = modelB.getStatements()
        self.assertEqual(set(), set(r1a), set(r1b))

        # add statements and confirm A sees them and B doesn't
        modelA.addStatements(statements)
        r2a = modelA.getStatements()
        self.assertEqual(set(r2a), set(statements))
        r2b = modelB.getStatements()
        self.assertEqual(set(r2b), set())

        # rollback A and confirm both models see nothing
        modelA.rollback()
        r3a = modelA.getStatements()
        r3b = modelB.getStatements()
        self.assertEqual(set(), set(r3a), set(r3b))

    def testInsert(self):
        model = self.getModel()
        print 'start insert with %s objects (-b to change)' % BIG 
        start = time.time()
        
        for i in xrange(BIG):
            subj = random_name(12)
            for j in xrange(7):
                model.addStatement(Statement(subj, 'pred'+str(j), 'obj'+str(j)) )
        print 'added %s statements in %s seconds' % (BIG * 7, time.time() - start)
        
        try:
            if self.persistentStore:
                model.commit()
                if hasattr(model, 'close'):
                    print 'closing'
                    sys.stdout.flush()
                    start = time.time()
                    model.close()
                    print 'closed in %s seconds, re-opening' % (time.time() - start)                    
                model = self.getModel()
        except:
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            raise

        print 'getting statements'
        sys.stdout.flush()
        start = time.time()
        stmts = model.getStatements()
        print 'got %s statements in %s seconds' % (BIG * 7, time.time() - start)
        self.assertEqual(len(stmts), BIG * 7)
        
        start = time.time()
        lastSubject = None
        for i, s in enumerate(stmts):
            if i > BIG: 
                break
            if s[0] != lastSubject:
                lastSubject = s[0]
                self.assertEqual(len(model.getStatements(s[0])), 7)
        print 'did %s subject lookups in %s seconds' % (BIG, time.time() - start)

class GraphModelTestCase(BasicModelTestCase):

    def _getModel(self, model):
        modelUri = base.generateBnode()
        return graphManagerClass(model, None, modelUri)

class SplitGraphModelTestCase(BasicModelTestCase):
    
    def _getModel(self, model):
        modelUri = base.generateBnode()
        revmodel = TransactionMemStore()
        return graphManagerClass(model, revmodel, modelUri)
    
BIG = 100 #10000
def main(testCaseClass):
    if '-b' in sys.argv:
        i = sys.argv.index("-b")
        global BIG
        BIG = int(sys.argv[i+1])
        del sys.argv[i:i+2]
    
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        #we need to specify the testCaseClass module to prevent
        #BasicModelTestCase from running
        unittest.main(testCaseClass.__module__)
    else:
        path = test.split('.')
        if len(path) > 1:
            testCaseClass = getattr(__import__(testCaseClass.__module__), path[0])
            test = path[1]
        tc = testCaseClass(test)
            
        testfunc = getattr(tc, test)
        tc.setUp()
        try:
            testfunc() #run test
        finally:
            tc.tearDown()

if __name__ == '__main__':
    main(BasicModelTestCase)
