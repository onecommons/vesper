@Action
def testaction(kw, retval):
    e = Exception('not found')
    e.errorCode = 404
    raise e

@Action    
def errorhandler(kw, retval):    
    if kw['_errorInfo']['errorCode'] == 404:
        kw['_responseHeaders']['_status'] = 404
        return '404 not found'
    else:
        kw['_responseHeaders']['_status'] = 503
        return '503 unhandled error'  

actions = {         
        'http-request' : [  testaction ],
        'http-request-error': [ errorhandler ]
        }

