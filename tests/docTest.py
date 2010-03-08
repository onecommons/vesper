import doctest
import unittest
import glob, sys
from vesper import query, pjson, multipartjson
from vesper.query import engine
import vesper.utils

suite = unittest.TestSuite()

modulesWithDoctests = [query, engine, pjson, multipartjson, vesper.utils]
if sys.version_info[:2] < (2,6):
    modulesWithDoctests.append(vesper.backports)

for mod in modulesWithDoctests:
    suite.addTest(doctest.DocTestSuite(mod))

#for path in glob.glob('../doc/source/*.rst'):
#    suite.addTest(doctest.DocFileSuite(path))

runner = unittest.TextTestRunner()

if __name__ == '__main__':
    runner.run(suite)
