from vesper.app import createApp, Action
from vesper.web.route import Route
import vesper.web.route
from vesper.utils import attrdict
from vesper.app import getCurrentApp
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
    from vesper import pjson
    if kw._environ.REQUEST_METHOD != 'POST':
        raise RuntimeError('POST expected')

    def handleRequest(requestid=0, action=0, data=0):
        if not requestid:
            raise RuntimeError('datarequest missing requestid')
        
        #don't catch exceptions for write operations because we want 
        #the whole request transaction to be aborted
        if action == 'update':
            addStmts, removeStmts = dataStore.update(data)
            addJson = pjson.tojson(addStmts)
            results = dict(results=addJson)
        elif action == 'add':
            addJson = dataStore.add(data)
            results = dict(results=addJson)#pjson.tojson(addStmts))
        elif action == 'query':
            #returns { errors, results }
            if isinstance(data, (str, unicode)):
                results = dataStore.query(data) 
            else:
                results = dataStore.query(**data) 
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
    # some json libs return unicode keys, which causes problems with **dict usages
    requests = json.loads(postdata, object_hook=lambda x: dict([(str(k),v) for (k,v) in x.items()]))
    #XXX raccoon needs default content-type 
    kw._responseHeaders['Content-Type'] = 'application/json'
    response = dict(responses = [handleRequest(**x) for x in requests])
    #may add an "errors" field in the future instead of just raising exception
    return json.dumps(response, indent=4) # XXX

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

def parseCmdLine():
    # XXX
    import os, sys, logging
    from optparse import OptionParser
    app = getCurrentApp()
    
    parser = OptionParser() # XXX usage string here!
    parser.add_option("-l", "--log-config", dest="log_config", help="path to logging configuration file")
    parser.add_option("-s", "--storage", dest="storage", help="storage url")
    parser.add_option("-p", "--port", dest="port", help="server listener port")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False)
    (options, args) = parser.parse_args()
    
    if len(args) > 0: # XXX
        print "loading config file from:", args[0]
        app.updateFromConfigFile(args[0])
    
    CONF={}
    
    if options.log_config:
        logpath = options.log_config
    else:
        serverdir = os.path.dirname(os.path.abspath(__file__))
        logpath = os.path.join(serverdir, "log.conf")
    
    if os.path.exists(logpath) and os.path.isfile(logpath):
        print "loading log config file:" + logpath
        CONF['logconfig'] = logpath
    else:
        if options.verbose:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO
        log = logging.getLogger()
        log.setLevel(loglevel)
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
        datefmt="%d %b %H:%M:%S"    
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(logging.Formatter(format, datefmt))
        log.addHandler(stream)
        
    if options.storage:
        CONF['STORAGE_URL'] = options.storage
    if options.port:
        CONF['PORT'] = int(options.port)
    
    # apply these last since they must override anything from the config file
    if len(CONF) > 0:
        app.update(CONF)
        
    return app # ??

if __name__ == "__main__":
    app.run()
    
