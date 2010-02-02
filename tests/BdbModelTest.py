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
from vesper.data.store.bdb import BdbStore, TransactionBdbStore

class BdbModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        #print 'opening', self.tmpfilename
        sys.stdout.flush()
        model = BdbStore(self.tmpfilename)
        return self._getModel(model)

    def getTransactionModel(self):
        model = TransactionBdbStore(self.tmpfilename)
        return self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.bdb') 
        
    def tearDown(self):
        #print 'tear down removing', self.tmpdir
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(BdbModelTestCase)
