class TestAction(SimpleAction):
    def match(self, kw):
        e = Exception('not found')
        e.errorCode = 404
        raise e
    
    def go(self, kw):
        return 'this function should never be called!'

class ErrorHandlerAction(SimpleAction):
    
    def go(self, kw):
        if kw['_errorInfo']['errorCode'] == 404:
            return '404 not found'
        else:
            return '503 unhandled error'  

actions = { 'test-error-request' : [  TestAction() ],
        'test-error-request-error': [ ErrorHandlerAction() ]
        }

