import doctest
import unittest
import glob
import jql, jql.engine, multipartjson

suite = unittest.TestSuite()
for mod in (jql,jql.engine, multipartjson):
    suite.addTest(doctest.DocTestSuite(mod))

#for path in glob.glob('../doc/source/*.rst'):
#    suite.addTest(doctest.DocFileSuite(path))

runner = unittest.TextTestRunner()

if __name__ == '__main__':
    runner.run(suite)
