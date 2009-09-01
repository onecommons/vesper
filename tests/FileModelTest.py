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
from rx.RxPathModel import FileModel, TransactionFileModel

class FileModelTestCase(modelTest.BasicModelTestCase):
    
    EXT = 'json' #also supported: rdf, nt, nj, yaml 
    
    def getModel(self):
        #print 'opening', self.tmpfilename
        sys.stdout.flush()
        model = FileModel(self.tmpfilename)
        return self._getModel(model)

    def getTransactionModel(self):
        model = TransactionFileModel(self.tmpfilename)
        return self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.'+self.EXT) 
        
    def tearDown(self):
        print 'tear down removing', self.tmpdir
        #shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(FileModelTestCase)
