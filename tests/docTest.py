import doctest
import unittest
import glob
from vesper import query, sjson, multipartjson
from vesper.query import engine
import vesper.utils

suite = unittest.TestSuite()
for mod in (query, engine, sjson, multipartjson, vesper.utils):
    suite.addTest(doctest.DocTestSuite(mod))

#for path in glob.glob('../doc/source/*.rst'):
#    suite.addTest(doctest.DocFileSuite(path))

runner = unittest.TextTestRunner()

if __name__ == '__main__':
    runner.run(suite)
