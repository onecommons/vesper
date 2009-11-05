@Action
def testaction(kw, retval):
    if kw._name != 'foo':
        return retval
    e = Exception('not found')
    e.errorCode = 404
    raise e

@Action
def testInTransactionAction(kw, retval):
    if kw._name != 'errorInCommit':
        return retval
    kw.__server__.domStore.add({id:"test", "test" : 'hello!'})
    def badCommit(*args, **kw):
        #print 'bad commit'
        raise RuntimeError("this error inside commit")
    kw.__server__.domStore.model.commit = badCommit
    return 'success'

@Action    
def errorhandler(kw, retval):    
    if kw['_errorInfo']['errorCode'] == 404:
        kw['_responseHeaders']['_status'] = 404
        return '404 not found'
    else:
        kw['_responseHeaders']['_status'] = 503
        return '503 unhandled error'  

actions = {         
        'http-request' : [  testInTransactionAction, testaction ],
        'http-request-error': [ errorhandler ]
        }

