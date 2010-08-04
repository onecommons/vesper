#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import doctest
import unittest
import glob, sys
from vesper.query import _query
from vesper.utils import _utils
from vesper import pjson, multipartjson
from vesper.query import engine
import vesper.utils

suite = unittest.TestSuite()

modulesWithDoctests = [_query, engine, pjson, multipartjson, _utils]
if sys.version_info[:2] < (2,6):
    modulesWithDoctests.append(vesper.backports)

for mod in modulesWithDoctests:
    suite.addTest(doctest.DocTestSuite(mod))

#for path in glob.glob('../doc/source/*.rst'):
#    suite.addTest(doctest.DocFileSuite(path))

runner = unittest.TextTestRunner()

if __name__ == '__main__':
    runner.run(suite)
