"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

import modelTest 
from rx.RxPath import FileModel, TransactionFileModel, IncrementalNTriplesFileModel, IncrementalNTriplesFileModelBase, Statement

class FileModelTestCase(modelTest.BasicModelTestCase):
    
    EXT = 'json' #also supported: rdf, nt, nj, yaml 
    
    persistentStore = True
    
    def getModel(self):
        #print 'opening', self.tmpfilename
        #sys.stdout.flush()
        model = FileModel(self.tmpfilename)
        return model #self._getModel(model)

    def getTransactionModel(self):
        model = FileModel(self.tmpfilename)
        return model #self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.'+self.EXT) 
        
    def tearDown(self):
        #print 'tear down removing', self.tmpdir
        shutil.rmtree(self.tmpdir)

    def testCommitFailure(self):
        "test commit transaction isolation across 2 models"
        modelA = self.getTransactionModel()

        #include spaces in value so it looks like a literal, not a resource
        statements = [Statement("one", "equals", " one "),
                      Statement("two", "equals", " two "),
                      Statement("three", "equals", " three ")]

        # confirm models are empty
        r1a = modelA.getStatements()
        self.assertEqual(set(), set(r1a))

        # add statements and confirm A sees them and B doesn't
        modelA.addStatements(statements)
        r2a = modelA.getStatements()
        self.assertEqual(set(r2a), set(statements))

        # commit A and confirm both models see the statements
        modelA.commit()

        more =  [
        Statement('s', 'p1', 'o2', 'en-1', 'c1'),
        Statement('s', 'p1', 'o1', 'en-1', 'c2'),
        Statement('s2', 'p1', 'o2', 'en-1', 'c2'),
        ]
        modelA.addStatements(more)
        
        try:
            #make commit explode            
            modelA.serializeOptions = dict(badOption=1)
            modelA.commit()
        except:
            self.assertTrue("got expected exception")
        else:
            self.assertFalse('expected exception during commit')

        #reload the data, should be equal to first commit (i.e. file shouldn't have been corrupted)
        modelC = self.getTransactionModel()
        r3c = modelC.getStatements()
        self.assertEqual(set(statements), set(r3c))
    
class TransactionFileModelTestCase(FileModelTestCase):

    def getTransactionModel(self):
        model = TransactionFileModel(self.tmpfilename)
        return model#self._getModel(model)

class IncrementalFileModelTestCase(FileModelTestCase):

    EXT = 'nt' #XXX investigate why EXT = 'json' fails
    
    def getModel(self):
        #print 'opening', self.tmpfilename
        #sys.stdout.flush()
        model = IncrementalNTriplesFileModelBase(self.tmpfilename)
        return model#self._getModel(model)

    def getTransactionModel(self):
        model = IncrementalNTriplesFileModel(self.tmpfilename)
        return model#self._getModel(model)
    
    def testCommitFailure(self):
        pass #this test needs to be disabled for IncrementalNTriplesFileModel

class TransactionIncrementalFileModelTestCase(IncrementalFileModelTestCase):

    def getModel(self):
        model = IncrementalNTriplesFileModel(self.tmpfilename)
        return model#self._getModel(model)

if __name__ == '__main__':
    modelTest.main(FileModelTestCase)
