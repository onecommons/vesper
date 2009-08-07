'''
Chooses find the latest version of the simplejson or the built-in json package

Usage: 
>>> from rx import json
'''
import sys
pyver = sys.version_info[:2]
if pyver < (2,6):
    #system json not available before python 2.6
    from simplejson import *
else:    
    try:
        import simplejson
        simplejsonVersion = tuple(int(i) for i in simplejson.__version__.split('.'))
        import json
        jsonVersion = tuple(int(i) for i in json.__version__.split('.'))
        if simplejsonVersion > jsonVersion:
            from simplejson import *
        else:
            from json import *
    except ImportError:
        from json import *
