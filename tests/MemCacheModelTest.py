"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os, signal
import string, random, shutil, time

import modelTest
from vesper.data.store.memcache import MemCacheStore, TransactionMemCacheStore

_prefixCounter = time.time()

class MemCacheModelTestCase(modelTest.BasicModelTestCase):   
    
    def getModel(self):    
        model = MemCacheStore(prefix=str(self._prefixCounter))
        return self._getModel(model)

    def getTransactionModel(self):
        model = TransactionMemCacheStore(prefix=str(self._prefixCounter))
        return self._getModel(model)

    def setUp(self):
        global _prefixCounter
        _prefixCounter += 1
        self._prefixCounter = _prefixCounter
        print 'count', self._prefixCounter
        
    def tearDown(self):
        pass

if __name__ == '__main__':
    modelTest.main(MemCacheModelTestCase)

