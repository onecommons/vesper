#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SQLite model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

import modelTest 
from vesper.data.store.sqlite import SqliteStore, TransactionSqliteStore

class SqliteModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = SqliteStore(self.tmpfilename)
        return self._getModel(model)

    def getTransactionModel(self):
        model = TransactionSqliteStore(self.tmpfilename)
        return self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.sqlite') 
        
    def tearDown(self):
        #print 'tear down removing', self.tmpdir
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(SqliteModelTestCase)
