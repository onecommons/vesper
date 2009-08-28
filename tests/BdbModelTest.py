"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

from modelTest import * 
from rx.store.RxPathModelBdb import BdbModel, TransactionBdbModel

class BdbModelTestCase(BasicModelTestCase):
    
    def getTyrantModel(self):
        print 'opening', self.tmpfilename
        sys.stdout.flush()
        model = BdbModel(self.tmpfilename)
        return self.getModel(model)

    def getTransactionTyrantModel(self):
        model = TransactionBdbModel(self.tmpfilename)
        return self.getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir='/tmp', prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.bdb') 
        
    def tearDown(self):
        print 'tear down removing', self.tmpdir
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    main(BdbModelTestCase)
