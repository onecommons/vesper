"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os, signal
import string, random, shutil, time

from modelTest import * 
from rx.RxPathModelMemcache import MemCacheModel, TransactionMemCacheModel

_prefixCounter = time.time()

class MemCacheModelTestCase(BasicModelTestCase):    
    
    def getTyrantModel(self):    
        global _prefixCounter
        _prefixCounter += 1
        model = MemCacheModel(prefix=str(_prefixCounter))
        return self.getModel(model)

    def getTransactionTyrantModel(self):
        global _prefixCounter
        _prefixCounter += 1
        model = TransactionMemCacheModel(prefix=str(_prefixCounter))
        return self.getModel(model)

    def setUp(self):
        pass
        
    def tearDown(self):
        pass

if __name__ == '__main__':
    main(MemCacheModelTestCase)

