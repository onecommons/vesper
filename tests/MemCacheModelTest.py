#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    memcache model unit tests
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

