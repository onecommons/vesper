@Action
def testaction(kw, retval):
    if kw._name != 'foo':
        return retval
    e = Exception('not found')
    e.errorCode = 404
    raise e

originalCommit = None
badCommitCalled = False
@Action
def testInTransactionAction(kw, retval):
    if kw._name != 'errorInCommit':
        return retval
    kw.__server__.dataStore.add({id:"test", "test" : 'hello!'})
    def badCommit(*args, **kw):
        global badCommitCalled
        badCommitCalled = True 
        raise RuntimeError("this error inside commit")
        
    global originalCommit    
    originalCommit = kw.__server__.dataStore.model.commit
    from vesper.data import DataStore
    if isinstance(kw.__server__.dataStore.model, DataStore.ModelWrapper): 
        kw.__server__.dataStore.model.model.commit = badCommit        
    else:
        kw.__server__.dataStore.model.commit = badCommit 
    return 'success'

@Action
def errorhandler(kw, retval):
    #print 'in error handler', kw['_errorInfo']['errorCode'], 'badCommit', badCommitCalled, kw.__server__.dataStore.model
    if originalCommit:
        kw.__server__.dataStore.model.commit = originalCommit        

    if kw['_errorInfo']['errorCode'] == 404:
        kw['_responseHeaders']['_status'] = 404
        return '404 not found'
    else:
        kw['_responseHeaders']['_status'] = 503
        return 'badCommit: %s' % badCommitCalled  

actions = {         
        'http-request' : [  testInTransactionAction, testaction ],
        'http-request-error': [ errorhandler ]
        }

createApp(actions=actions)
