import webbrowser
from vesper.utils import Uri

url = Uri.OsPathToUri('binder_tests.html')
print 'running ', url
webbrowser.open(url)
