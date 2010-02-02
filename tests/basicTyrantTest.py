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
from vesper.data.store.tyrant import TyrantStore, TransactionTyrantStore

def start_tyrant_server():
    "start a local tyrant server, return a dict needed to stop & clean up"
    # tmpdir for the datafile
    tmpdir = tempfile.mkdtemp(prefix="rhizometest")
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

class TyrantModelTestCase(modelTest.BasicModelTestCase):    

    def getModel(self):
        port = self.tyrant['port']
        model = TyrantStore('127.0.0.1', port)
        return self._getModel(model)

    def getTransactionModel(self):
        port = self.tyrant['port']
        model = TransactionTyrantStore('127.0.0.1', port)
        return self._getModel(model)
    
    def setUp(self):
        self.tyrant = start_tyrant_server()

    def tearDown(self):
        stop_tyrant_server(self.tyrant)
        self.tyrant = None

if __name__ == '__main__':
    modelTest.main(TyrantModelTestCase)
