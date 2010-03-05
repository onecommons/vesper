#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    BDB model unit tests
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
