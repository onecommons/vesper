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

        # confirm a randomly created subject does not exist
        subj = random_name(12)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())

        # add a new statement and confirm the search succeeds
        s1 = Statement(subj, 'pred', "obj")
        model.addStatement(s1)

        r2 = model.getStatements(subject=subj)
        self.assertEqual(set(r2), set([s1]))

    def testRemove(self):
        "basic removal test"
        model = self.getTyrantModel()

        # set up the model with one randomly named statement
        subj = random_name(12)
        s1 = Statement(subj, random_name(24), random_name(12))
        model.addStatement(s1)

        # confirm a search for the subject finds it
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1])) # object exists

        # remove the statement and confirm that it's gone
        model.removeStatement(s1)

        r2 = model.getStatements(subject=subj)
        self.assertEqual(set(r2), set()) # object is gone

    def testSetBehavior(self):
        "confirm model behaves as a set"
        model = self.getTyrantModel()

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")
        s3 = Statement("ocean", "is", "blue")

        # before adding anything db should be empty
        r1 = model.getStatements()
        self.assertEqual(set(r1), set())

        # add a single statement and confirm it is returned
        model.addStatement(s1)

        r2 = model.getStatements()
        self.assertEqual(set(r2), set([s1]))

        # add the same statement again & the set should be unchanged
        model.addStatement(s1)
        
        r3 = model.getStatements()
        self.assertEqual(set(r3), set([s1]))
        
        # add a second statement with the same subject as s1
        model.addStatement(s2)
        
        r4 = model.getStatements()
        self.assertEqual(set(r4), set([s1, s2]))

        # add a third statement with same predicate & object as s1
        model.addStatement(s3)

        r5 = model.getStatements()
        self.assertEqual(set(r5), set([s1,s2,s3]))

    def testQuads(self):
        "test (somewhat confusing) quad behavior"
        model = self.getTyrantModel()

        # add 3 identical statements with differing contexts
        statements = [Statement("one", "two", "three", "fake", "100"),
                      Statement("one", "two", "three", "fake", "101"),
                      Statement("one", "two", "three", "fake", "102")]
        model.addStatements(statements)

        # asQuad=True should return all 3 statements
        r1 = model.getStatements(asQuad=True)
        self.assertEqual(set(r1), set(statements))

        # asQuad=False (the default) should only return the oldest
        expected = set()
        expected.add(statements[0])
        r2 = model.getStatements(asQuad=False)
        self.assertEqual(set(r2), expected)


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

