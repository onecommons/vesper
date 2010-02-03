from vesper.app import createApp, Action
from vesper.web.route import Route
import vesper.web.route
from vesper.utils import attrdict
from vesper.app import loadApp
from vesper.backports import json
import mako.runtime
#contrary to http://www.makotemplates.org/docs/runtime.html#runtime_context_variables
#for templates not throwing errors is useful
#monkey patch so iteration over undefined properties doesn't raise an exception
def emptygenerator(self):
    if 0: yield
#def undefinedgenerator(self):
#    yield '<undefined>'
mako.runtime.Undefined.__iter__ = emptygenerator
mako.runtime.Undefined.__str__ = lambda self: ''#'<undefined>'

import vesper.utils
vesper.utils.defaultattrdict.UNDEFINED = mako.runtime.UNDEFINED

@Route('datarequest')#, REQUEST_METHOD='POST')
def datarequest(kw, retval): 
    from vesper import sjson
    if kw._environ.REQUEST_METHOD != 'POST':
        raise RuntimeError('POST expected')

    def handleRequest(requestid=0, action=0, data=0):
        if not requestid:
            raise RuntimeError('datarequest missing requestid')
        
        #don't catch exceptions for write operations because we want 
        #the whole request transaction to be aborted
        if action == 'update':
            addStmts, removeStmts = dataStore.update(data)
            addJson = sjson.tojson(addStmts)
            results = dict(results=addJson)
        elif action == 'add':
            addJson = dataStore.add(data)
            results = dict(results=addJson)#sjson.tojson(addStmts))
        elif action == 'query':
            #returns { errors, results }
            if isinstance(data, (str, unicode)):
                results = dataStore.query(data) 
            else:
                results = dataStore.query(data['query'], data.get('bindvars',{})) 
        elif action == 'remove':
            #returns None
            results = dataStore.remove(data)
            #XXX return better result?
            results = dict(results=results)
        else:
            raise RuntimeError('unexpected datarequest action: '+ action)

        results['requestid']=requestid
        results['action']=action
        return results

    dataStore = kw.__server__.dataStore
    postdata = kw._params.requests
    requests = json.loads(postdata)
    #XXX raccoon needs default content-type 
    kw._responseHeaders['Content-Type'] = 'application/json'
    response = dict(responses = [handleRequest(**x) for x in requests])
    #may add an "errors" field in the future instead of just raising exception
    return json.dumps(response)

@Route('static/{file:.+}')
def servefile(kw, retval):
    return vesper.web.route._servefile(kw,retval,kw.urlvars.file)

Route('robots.txt')(vesper.web.route.servefile)
Route('favicon.ico')(vesper.web.route.servefile)

actions = { 'http-request' : vesper.web.route.gensequence,
          }

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
    
    actions['http-request-error'] = [displayError]
except ImportError:
    pass

from vesper.data.store.basic import FileStore
app = createApp(
    static_path=['static'],
    STORAGE_PATH="baseapp-store.json",
    modelFactory=FileStore,
    defaultPageName = 'index.html',
    actions = actions
)

if __name__ == "__main__":
    app.run()
    
