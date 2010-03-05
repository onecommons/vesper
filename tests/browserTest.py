#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import webbrowser
from vesper.utils import Uri

url = Uri.OsPathToUri('binder_tests.html')
print 'running ', url
webbrowser.open(url)
