#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from vesper.app import createApp, Action
from vesper.web.route import Route
import vesper.web.route
from vesper.utils import attrdict
from vesper.app import getCurrentApp
from vesper.backports import json
import mako.runtime
import logging
log = logging.getLogger('datarequest')

#contrary to http://www.makotemplates.org/docs/runtime.html#runtime_context_variables
#for templates not throwing errors is useful
#monkey patch so iteration over undefined properties doesn't raise an exception
def emptygenerator(self):
    if 0: yield None
#def undefinedgenerator(self):
#    yield '<undefined>'
mako.runtime.Undefined.__iter__ = emptygenerator
mako.runtime.Undefined.__str__ = lambda self: ''#'<undefined>'
mako.runtime.Undefined.__add__ = lambda self,other: ''+other
mako.runtime.Undefined.__radd__ = lambda self,other: ''+other

import vesper.utils
vesper.utils.defaultattrdict.UNDEFINED = mako.runtime.UNDEFINED
import vesper.query
vesper.query.QueryContext.defaultShapes = { dict : vesper.utils.defaultattrdict }

@Route('datarequest')
@Route('{store:.*}/datarequest')#, REQUEST_METHOD='POST')
def datarequest(kw, retval): 
    '''
    Accepts a JSON-RPC 2.0 (see http://groups.google.com/group/json-rpc/web/json-rpc-2-0)
    request (including a batch request).
    '''
    from vesper import pjson
    if kw._environ.REQUEST_METHOD != 'POST':
        raise RuntimeError('POST expected')

    def handleRequest(id=None, method=0, params=0, jsonrpc=None):
        requestid = id; action = method; data = params
        response = dict(id=requestid, jsonrpc='2.0')
        if requestid is None or jsonrpc != '2.0':
            response['error'] = dict(code=-32600, message='Invalid Request')
            return response
        
        #don't catch exceptions for write operations because we want 
        #the whole request transaction to be aborted
        #sendJsonRpcError below it will turn the error into json-rpc error response
        if action == 'update':
            addStmts, removeStmts = dataStore.update(data)
            #XXX better return values
            result = dict(added=pjson.tojson(addStmts), removed=pjson.tojson(removeStmts))
        elif action == 'replace':
            addStmts, removeStmts = dataStore.replace(data)
            #XXX better return values
            result = dict(added=pjson.tojson(addStmts), removed=pjson.tojson(removeStmts))
        elif action == 'add':
            addJson = dataStore.add(data)
            result = dict(added=addJson)
        elif action == 'create':
            addJson = dataStore.create(data)
            result = dict(added=addJson)
        elif action == 'query':
            #returns { errors, results }
            if isinstance(data, (str, unicode)):
                result = dataStore.query(data, captureErrors=True) 
            else:
                data['captureErrors'] = True
                result = dataStore.query(**data)
            if result.errors:
                response['error'] = dict(code=0, message='query failed', 
                                                    data = result.errors)
                return response
        elif action == 'remove':
            removeJson = dataStore.remove(data)
            result = dict(removed=removeJson)
        else:
            response['error'] = dict(code=-32601, message='Method not found')
            return response
        
        response['result'] = result
        return response

    if kw.urlvars.store:
        dataStore = kw.__server__.stores.get(kw.urlvars.store)
    else:
        dataStore = kw.__server__.defaultStore

    if not dataStore:
        response = dict(id=None, jsonrpc='2.0', error=dict(code=-32600, 
            message='Invalid Request: store "%s" not found' % kw.urlvars.store))
    else:                                             
        postdata = kw._postContent
        # some json libs return unicode keys, which causes problems with **dict usages
        try:
            requests = json.loads(postdata, object_hook=lambda x: 
                                dict([(str(k),v) for (k,v) in x.items()]))
        except:
            response = dict(id=None, jsonrpc='2.0', error=dict(code=-32700, 
                                                      message='Parse error'))
        else:
            if not isinstance(requests, list):
               requests = [requests] 
            #XXX vesper.app should set a default content-type 
            kw._responseHeaders['Content-Type'] = 'application/json'
            response = [isinstance(x, dict) and handleRequest(**x) or 
    dict(id=None, jsonrpc='2.0', error=dict(code=-32600, message='Invalid Request'))
                                                                for x in requests]
    log.debug('request: \n  %r\n response:\n   %r', requests, response)
    return json.dumps(response, indent=4) 

@Route('static/{file:.+}')
def servefile(kw, retval):
    return vesper.web.route._servefile(kw,retval,kw.urlvars.file)

Route('robots.txt')(vesper.web.route.servefile)
Route('favicon.ico')(vesper.web.route.servefile)

actions = { 'http-request' : vesper.web.route.gensequence,
          }

@Action
def sendJsonRpcError(kw, retVal):
    if 'CONTENT_TYPE' not in kw._environ or not kw._environ.CONTENT_TYPE.startswith('application/json'):
        return retVal
    #kw._responseHeaders._status = 500 or 400?
    kw._responseHeaders['Content-Type'] = 'application/json'
    ei = kw._errorInfo
    import traceback
    errordata = traceback.format_exception(ei.type, ei.value, ei.traceback)
    response = dict(id=None, jsonrpc='2.0', error=dict(code=-32000, 
                                    data=errordata, message='Server Error'))
    return json.dumps(response, indent=4)
    
try:
    import mako

    @Action
    def displayError(kw, retVal):
        kw._responseHeaders._status = 500
        kw._responseHeaders['content-type'] = 'text/html'
        type = kw._errorInfo.type
        value = kw._errorInfo.value
        tb = kw._errorInfo.traceback
        try:
            #work-around bug in mako.exceptions.html_error_template
            raise type, value, tb         
        except:
            return mako.exceptions.html_error_template().render()#traceback=(type,value,tb))
    
    actions['http-request-error'] = [sendJsonRpcError, displayError]
except ImportError:
    actions['http-request-error'] = [sendJsonRpcError]

from vesper.data.store.basic import FileStore
app = createApp(
    static_path=['static'],
    default_page_name = 'index.html',
    actions = actions
)

if __name__ == "__main__":
    app.run()
