"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest
import string, random

from rx.RxPath import *
from rx.RxPathModelTyrant import TyrantModel

def random_name(length):
    return ''.join(random.sample(string.ascii_letters, length))

class BasicTyrantModelTestCase(unittest.TestCase):
    "Tests basic features of the tyrant model class"

    def setUp(self):
        self.tyrant = TyrantModel('127.0.0.1')

    def tearDown(self):
        pass

    def testStore(self):
        name = random_name(12)
        x = self.tyrant.getStatements(subject=name)
        self.assertEqual(len(x), 0) # object does not exist

        stmt = Statement(name, 'pred', "obj")
        self.tyrant.addStatement(stmt)

        x = self.tyrant.getStatements(subject=name)
        self.assertEqual(len(x), 1) # object is there now

    def testRemove(self):
        name = random_name(12)
        stmt = Statement(name, random_name(24), random_name(12))
        self.tyrant.addStatement(stmt)

        x = self.tyrant.getStatements(subject=name)
        self.assertEqual(len(x), 1) # object exists

        self.tyrant.removeStatement(stmt)

        x = self.tyrant.getStatements(subject=name)
        self.assertEqual(len(x), 0) # object is gone

    def testReturnsStatements(self):
        # XXX todo
        pass

    def testSearch(self):
        # XXX todo
        pass


if __name__ == '__main__':
    unittest.main()

