__all__ = ['all', 'any', 'partial', 'json']

#for pythons older than 2.5:
try:
    all=all
    any=any
except NameError:
    def all(iterable):
         for element in iterable:
             if not element:
                 return False
         return True

    def any(iterable):
         for element in iterable:
             if element:
                 return True
         return False

try:
    from functools import partial
except ImportError:
    def partial(fn_, *args, **keywords):
            def newfunc(*fargs, **fkeywords):
                newkeywords = keywords.copy()
                newkeywords.update(fkeywords)
                return fn_(*(args + fargs), **newkeywords)
            newfunc.func = fn_
            newfunc.args = args
            newfunc.keywords = keywords
            return newfunc

#if simplejson is installed and more recent then the built-in json package
#import that as json
import sys
pyver = sys.version_info[:2]
if pyver < (2,6):
    #system json not available before python 2.6
    import simplejson as json
else:    
    try:
        import simplejson
        simplejsonVersion = tuple(int(i) for i in simplejson.__version__.split('.'))
        import json
        jsonVersion = tuple(int(i) for i in json.__version__.split('.'))
        if simplejsonVersion > jsonVersion:
            import simplejson as json
            del simplejson
    except ImportError:
        import json
