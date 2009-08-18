"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

from rx.RxPath import *

testHistory = '' # 'single' or 'split' or '' (for no graph manager)

def random_name(length):
    return ''.join(random.sample(string.ascii_letters, length))

class BasicModelTestCase(unittest.TestCase):
    "Tests basic features of the tyrant model class"

    def getModel(self, model):
        if testHistory:
            from rx import RxPath, RxPathGraph
            modelUri = RxPath.generateBnode()
            if testHistory == 'single':
                revmodel = None
            else:
                revmodel = RxPath.TransactionMemModel()
            return RxPathGraph.NamedGraphManager(model, revmodel, modelUri)
        else:
            return model

    def getTyrantModel(self):
        model = MemModel()
        return self.getModel(model)

    def getTransactionTyrantModel(self):
        model = TransactionMemModel()
        return self.getModel(model)
    
    def testStore(self):
        "basic storage test"
        model = self.getTyrantModel()

        # confirm a randomly created subject does not exist
        subj = random_name(12)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())

        # add a new statement and confirm the search succeeds
        s1 = Statement(subj, 'pred', "obj")
        model.addStatement(s1)

        r2 = model.getStatements(subject=subj)
        self.assertEqual(set(r2), set([s1]))

    def testRemove(self):
        "basic removal test"
        model = self.getTyrantModel()

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

    def testSetBehavior(self):
        "confirm model behaves as a set"
        model = self.getTyrantModel()

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
        model = self.getTyrantModel()

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
        model = self.getTyrantModel()
        
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
        model = self.getTransactionTyrantModel()

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
        modelA = self.getTransactionTyrantModel()
        modelB = self.getTransactionTyrantModel()

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

        # commit A and confirm both models see the statements
        modelA.commit()
        r3a = modelA.getStatements()
        r3b = modelB.getStatements()
        self.assertEqual(set(statements), set(r3a), set(r3b))

    def testTransactionIsolationRollback(self):
        "test rollback transaction isolation across 2 models"
        modelA = self.getTransactionTyrantModel()
        modelB = self.getTransactionTyrantModel()

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

    def testBigInsert(self):
        model = self.getTyrantModel()
        print 'start big insert'
        start = time.time()
        for i in xrange(10000):
            subj = random_name(12)
            for j in xrange(7):
                model.addStatement(Statement(subj, 'pred'+str(j), 'obj'+str(j)) )
        print 'added 70,000 statements in', time.time() - start, 'seconds'
        
        try:
            if hasattr(model, 'close'):
                print 'closing'
                sys.stdout.flush()
                model.close()
                print 're-opening'            
                model = self.getTyrantModel()
        except:
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            raise

        print 'getting statements'
        sys.stdout.flush()
        start = time.time()
        stmts = model.getStatements()
        print 'got 70,000 statements in', time.time() - start, 'seconds'
        self.assertEqual(len(stmts), 70000)
        
        start = time.time()
        lastSubject = None
        for i, s in enumerate(stmts):
            if i > 10000: 
                break
            if s[0] != lastSubject:
                lastSubject = s[0]
                self.assertEqual(len(model.getStatements(s[0])), 7)
        print 'did 10,000 subject lookups in', time.time() - start, 'seconds'

def main(testCaseClass):
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = testCaseClass(test)
        tc.setUp()
        testfunc = getattr(tc, test)
        testfunc() #run test
        #tc.tearDown()

if __name__ == '__main__':
    main(BasicModelTestCase)
