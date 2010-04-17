#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
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
    if 0: yield None
#def undefinedgenerator(self):
#    yield '<undefined>'
mako.runtime.Undefined.__iter__ = emptygenerator
mako.runtime.Undefined.__str__ = lambda self: ''#'<undefined>'

import vesper.utils
vesper.utils.defaultattrdict.UNDEFINED = mako.runtime.UNDEFINED
import vesper.query
vesper.query.QueryContext.defaultShapes = { dict : vesper.utils.defaultattrdict }

@Route('datarequest')#, REQUEST_METHOD='POST')
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
        if action == 'update':
            addStmts, removeStmts = dataStore.update(data)
            result = pjson.tojson(addStmts)
        elif action == 'add':
            addJson = dataStore.add(data)
            result = addJson #pjson.tojson(addStmts))
        elif action == 'query':
            #returns { errors, results }
            if isinstance(data, (str, unicode)):
                result = dataStore.query(data) 
            else:
                result = dataStore.query(**data)
            if result.errors:
                response['error'] = dict(code=0, message='query failed', 
                                                    data = result.errors)
                return response
        elif action == 'remove':
            #returns None
            result = dataStore.remove(data)
            #XXX return better result?
        else:
            response['error'] = dict(code=-32601, message='Method not found')
            return response
        
        response['result'] = result
        return response

    dataStore = kw.__server__.dataStore
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
        #XXX raccoon needs default content-type 
        kw._responseHeaders['Content-Type'] = 'application/json'
        response = [isinstance(x, dict) and handleRequest(**x) or 
dict(id=None, jsonrpc='2.0', error=dict(code=-32600, message='Invalid Request'))
                                                            for x in requests]
    return json.dumps(response, indent=4) 

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
    model_factory=FileStore,
    default_page_name = 'index.html',
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
        # format="%(asctime)s %(levelname)s %(name)s %(message)s"
        # datefmt="%d %b %H:%M:%S"    
        # stream = logging.StreamHandler(sys.stdout)
        # stream.setFormatter(logging.Formatter(format, datefmt))
        # log.addHandler(stream)
        
    if options.storage:
        CONF['storage_url'] = options.storage
    if options.port:
        CONF['port'] = int(options.port)
    
    # apply these last since they must override anything from the config file
    if len(CONF) > 0:
        app.update(CONF)
        
    if 'storage_url' not in app:
        parser.error("storage_url not specified. Either use the -s option or load a config file that sets the value.")
        
    return app # ??

if __name__ == "__main__":
    parseCmdLine()
    app.run()
