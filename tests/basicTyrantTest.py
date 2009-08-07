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
from rx.RxPathModelTyrant import TyrantModel, TransactionTyrantModel

def start_tyrant_server():
    "start a local tyrant server, return a dict needed to stop & clean up"
    # tmpdir for the datafile
    tmpdir = tempfile.mkdtemp(dir='/tmp', prefix="rhizometest")
    tmpfile = os.path.join(tmpdir, 'test.tct') # extension makes it a table db

    port = random.randrange(9000,9999)
    cmd = "ttserver -port %d %s" % (port, tmpfile)
    #print cmd
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1) # give ttserver time to start up
    if (proc.poll() > 0): # see if the process started up correctly
        print "error starting tyrant server:"
        print proc.stderr.read()
        return False
    else:
        #print "ttserver started as pid %d on port %d" % (proc.pid, port)
        return {'tmpdir':tmpdir, 'port':port, 'proc':proc, }

def stop_tyrant_server(data):
    proc = data['proc']
    if not proc.poll(): # process still alive
        #print "waiting for tyrant server to die..."
        #proc.terminate() #2.6 only, so use:
        os.kill(proc.pid, signal.SIGTERM)
        proc.wait()
        #print "tyrant server exited"
    shutil.rmtree(data['tmpdir'])

class TyrantModelTestCase(BasicModelTestCase):    

    def getTyrantModel(self):
        port = self.tyrant['port']
        model = TyrantModel('127.0.0.1', port)
        return self.getModel(model)

    def getTransactionTyrantModel(self):
        port = self.tyrant['port']
        model = TransactionTyrantModel('127.0.0.1', port)
        return self.getModel(model)
    
    def setUp(self):
        self.tyrant = start_tyrant_server()

    def tearDown(self):
        stop_tyrant_server(self.tyrant)
        self.tyrant = None

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
    unittest.main()

