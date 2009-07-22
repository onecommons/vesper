"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import subprocess, tempfile, os
import string, random, shutil, time

from rx.RxPath import *
from rx.RxPathModelTyrant import TyrantModel

def random_name(length):
    return ''.join(random.sample(string.ascii_letters, length))

class BasicTyrantModelTestCase(unittest.TestCase):
    "Tests basic features of the tyrant model class"

    def startTyrantServer(self):
        # tmpdir for the datafile
        self.tmpdir = tempfile.mkdtemp(dir='/tmp', prefix="rhizometest")
        tmpfile = os.path.join(self.tmpdir, 'test.tct') # extension makes it a table db

        self.port = random.randrange(9000,9999)
        cmd = "ttserver -port %d %s" % (self.port, tmpfile)
        #print cmd
        self.proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(1) # give ttserver time to start up
        if (self.proc.poll() > 0): # see if the process started up correctly
            print "error starting tyrant server:"
            print self.proc.stderr.read()
            return False
        else:
            #print "ttserver started as pid %d on port %d" % (self.proc.pid, self.port)
            return True

    def stopTyrantServer(self):
        if not self.proc.poll(): # process still alive
            #print "waiting for tyrant server to die..."
            self.proc.terminate()
            self.proc.wait()
            #print "tyrant server exited"
        self.proc = None
        shutil.rmtree(self.tmpdir)

    def getTyrantModel(self):
        return TyrantModel('127.0.0.1', self.port)

    def setUp(self):
        self.startTyrantServer()

    def tearDown(self):
        self.stopTyrantServer()

    def testStore(self):
        "basic storage test"
        model = self.getTyrantModel()

        name = random_name(12)
        x = model.getStatements(subject=name)
        self.assertEqual(len(x), 0) # object does not exist

        stmt = Statement(name, 'pred', "obj")
        model.addStatement(stmt)

        x = model.getStatements(subject=name)
        self.assertEqual(len(x), 1) # object is there now

    def testRemove(self):
        "basic removal test"
        model = self.getTyrantModel()

        name = random_name(12)
        stmt = Statement(name, random_name(24), random_name(12))
        model.addStatement(stmt)

        x = model.getStatements(subject=name)
        self.assertEqual(len(x), 1) # object exists

        model.removeStatement(stmt)

        x = model.getStatements(subject=name)
        self.assertEqual(len(x), 0) # object is gone

    def testSetBehavior(self):
        "confirm model behaves as a set"
        model = self.getTyrantModel()

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")
        s3 = Statement("ocean", "is", "blue")

        self.assertEqual(len(model.getStatements()), 0)
        model.addStatement(s1)
        self.assertEqual(len(model.getStatements()), 1)
        model.addStatement(s1)
        self.assertEqual(len(model.getStatements()), 1)
        model.addStatement(s2) # new statement with same subject
        self.assertEqual(len(model.getStatements()), 2)
        model.addStatement(s3) # new statement with same predicate & object
        self.assertEqual(len(model.getStatements()), 3)

    def testQuads(self):
        model = self.getTyrantModel()

        model.addStatements([Statement("one", "two", "three", "fake", "100"),
                             Statement("one", "two", "three", "fake", "101"),
                             Statement("one", "two", "three", "fake", "102")])

        self.assertEqual(len(model.getStatements(asQuad=True)), 3)
        self.assertEqual(len(model.getStatements(asQuad=False)), 1)


    """
    def testReturnsStatements(self):
        # XXX todo
        pass

    def testSearch(self):
        # XXX todo
        pass
    """

if __name__ == '__main__':
    unittest.main()

