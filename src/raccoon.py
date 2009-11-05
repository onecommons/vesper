"""
    Engine and helper classes for Raccoon

    Copyright (c) 2003-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
if __name__ == '__main__':
    import sys,raccoon
    sys.exit(raccoon.main())
    
from rx import utils, glock, RxPath, MRUCache, DomStore, transactions, store
import os, time, sys, base64, mimetypes, types, traceback
import urllib, re

try:
    import cPickle
    pickle = cPickle
except ImportError:
    import pickle
try:
    import cStringIO
    StringIO = cStringIO
except ImportError:
    import StringIO

try:
    from hashlib import md5 # python 2.5 or greater
except ImportError:
    from md5 import new as md5

import logging
DEFAULT_LOGLEVEL = logging.INFO

logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
logging.root.setLevel(DEFAULT_LOGLEVEL)
logging.basicConfig()

log = logging.getLogger("raccoon")
_defexception = utils.DynaExceptionFactory(__name__)

_defexception('CmdArgError')
_defexception('RaccoonError')
_defexception('unusable namespace error')
_defexception('not authorized')

class DoNotHandleException(Exception):
    '''
    RequestProcessor.doActions() will not invoke error handler actions on
    exceptions derived from this class.
    '''

class ActionWrapperException(utils.NestedException):
    def __init__(self):
        return utils.NestedException.__init__(self,useNested=True)

#from rx.ExtFunctions import *
#from rx.UriResolvers import *
#from rx import ContentProcessors

############################################################
##Raccoon defaults
############################################################

DefaultNsMap = { 'owl': 'http://www.w3.org/2002/07/owl#',
           'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#',
           'bnode': RxPath.BNODE_BASE,
        }

############################################################
##Helper classes and functions
############################################################    
class Requestor(object):
    '''
    Requestor is a helper class that allows python code to invoke a
    Raccoon request as if it was function call

    Usage:
    response = __requestor__.requestname(**kw)
    where kw is the optional request parameters

    An AttributeError exception is raised if the server does not
    recognize the request
    '''
    def __init__(self, server, triggerName = None):
        self.server = server
        self.triggerName = triggerName

    #the trailing __ so you can have requests named 'invoke' without conflicting
    def invoke__(self, name, **kw):
        return self.invokeEx__(name, kw)[0]

    def invokeEx__(self, name, kwargs):
        kw = self.server.requestContext[-1].copy()
        kw.update(kwargs)#overrides request context kw

        kw['_name']=name
        if not kw.has_key('_path'):
            kw['_path'] = name
        #print 'invoke', kw
        #defaultTriggerName let's us have different trigger type per thread
        #allowing site:/// urls to rely on the defaultTriggerName
        triggerName = self.triggerName or self.server.defaultRequestTrigger
        result = self.server.runActions(triggerName, kw, newTransaction=False)
        if result is not None: #'cause '' is OK
            return (result, kw)
        else:
            raise AttributeError, name

    def __getattr__(self, name):
        if name in ['__hash__','__nonzero__', '__cmp__', '__del__']:
            #undefined but reserved attribute names
            raise AttributeError("'Requestor' object has no attribute '%s'" %name)
        return lambda **k: self.invoke__(name, **k)
        #else:raise AttributeError, name #we can't do this yet since
        #we may need the parameters to figure out what to invoke (like a multimethod)

def defaultActionCacheKeyPredicateFactory(action, cacheKeyPredicate):
    '''
    Returns a predicate to calculate a key for the action
    based on a given request.
    This function gives an action a chance to
    customize the cacheKeyPredicate for the particulars of the
    action instance. At the very least it should bind the action
    instance with the cacheKeyPredicate to disambiguate keys from
    different actions.
    '''
    actionid = id(action) #do this to avoid memory leaks
    return lambda kw, retVal: (actionid, cacheKeyPredicate(kw, retVal))

def notCacheableKeyPredicate(*args, **kw):
    raise MRUCache.NotCacheable

def defaultActionValueCacheableCalc(hkey, value, kw, retResult):
    if value is retResult:
        #when the result hasn't changed, store NotModified in the cache
        #instead of the result. This way the retVal won't need to be part
        #of the cache key
        return Action.NotModified
    else:
        return value

class Action(object):
    '''
The Action class encapsulates a step in the request processing pipeline.

An Action has two parts, one or more match expressions and an action
function that is invoked if the request metadata matches one of the
match expressions. The action function returns a value which is passed
onto the next Action in the sequence.
    '''
    NotModified = ('notmodified',)

    def __init__(self, action,
            cachePredicate=notCacheableKeyPredicate,
            sideEffectsPredicate=None, sideEffectsFunc=None,
            isValueCacheableCalc=defaultActionValueCacheableCalc,
            cachePredicateFactory=defaultActionCacheKeyPredicateFactory,
            debug=False):
        '''
action must be a function with this signature:    
def action(kw, retVal) where:
kw is the dictionary of metadata associated with the request
retVal was the return value of the last action invoked in the in action sequence or None
'''        
        self.action = action
        self.cacheKeyPredicate = cachePredicateFactory(self, cachePredicate)
        self.cachePredicateFactory = cachePredicateFactory
        self.sideEffectsPredicate = sideEffectsPredicate
        self.sideEffectsFunc = sideEffectsFunc
        self.isValueCacheableCalc = isValueCacheableCalc
        self.debug = debug

    def __call__(self, kw, retVal):
        return self.action(kw, retVal)

class Result(object):
    def __init__(self, retVal):
        self.value = retVal

    @property
    def asBytes(self):
        value = self.value
        if isinstance(value, unicode):
            return value.decode('utf8')
        elif hasattr(value, 'read'):
            self.value = value.read()
        return str(self.value)

    @property
    def asUnicode(self):
        if hasattr(self.value, 'read'):
            self.value = value.read()
        if isinstance(self.value, str):
            return self.value.encode('utf8')
        elif isinstance(self.value, unicode):
            return self.value
        else:
            return unicode(self.value)

def assignVars(self, kw, varlist, default):
    '''
    Helper function for assigning variables from the config file.
    Also used by rhizome.py.
    '''
    import copy
    for name in varlist:
        try:
            defaultValue = copy.copy(default)
        except TypeError:
            #probably ok, can't copy certain non-mutable objects like functions
            defaultValue = default
        value = kw.get(name, defaultValue)
        if default is not None and not isinstance(value, type(default)):
            raise RaccoonError('config variable %s (of type %s)'
                               'must be compatible with type %s'
                               % (name, type(value), type(default)))
        setattr(self, name, value)

############################################################
##Raccoon main class
############################################################
class RequestProcessor(utils.object_with_threadlocals):                
    DEFAULT_CONFIG_PATH = ''#'raccoon-default-config.py'
    lock = None

    requestsRecord = None
    log = log

    defaultGlobalVars = ['_name', '_noErrorHandling',
            '__current-transaction', '__readOnly'
            '__requestor__', '__server__',
            '_prevkw', '__argv__', '_errorInfo'
            ]

    def __init__(self,
                 #correspond to equivalentl command line args:
                 a=None, m=None, p=None, argsForConfig=None,
                 #correspond to equivalently named config settings
                 appBase='/', model_uri=None, appName='',
                 #dictionary of config settings, overrides the config
                 appVars=None):

        self.initThreadLocals(requestContext=None, inErrorHandler=0,
                               previousResolvers=None)

        #variables you want made available to anyone during this request
        self.requestContext = [{}] #stack of dicts
        configpath = a or self.DEFAULT_CONFIG_PATH
        self.source = m
        self.PATH = p or os.environ.get('RACCOONPATH',os.getcwd())
        self.BASE_MODEL_URI = model_uri
        #use first directory on the PATH as the base for relative paths
        #unless this was specifically set it will be the current dir
        self.baseDir = self.PATH.split(os.pathsep)[0]
        self.appBase = appBase or '/'
        self.appName = appName
        self.cmd_usage = DEFAULT_cmd_usage
        self.loadConfig(configpath, argsForConfig, appVars)        
        if self.template_path:
            from mako.lookup import TemplateLookup
            self.template_loader = TemplateLookup(directories=self.template_path, 
                module_directory='mako_modules', output_encoding='utf-8', encoding_errors='replace')
        self.requestDispatcher = Requestor(self)
        #self.resolver = SiteUriResolver(self)
        self.loadModel()
        self.handleCommandLine(argsForConfig or [])

    def handleCommandLine(self, argv):
        '''  the command line is translated into XPath variables
        as follows:

        * arguments beginning with a '-' are treated as a variable
        name with its value being the next argument unless that
        argument also starts with a '-'

        * the entire command line is assigned to the variable '_cmdline'
        '''
        kw = argsToKw(argv, self.cmd_usage)
        kw['_cmdline'] = '"' + '" "'.join(argv) + '"'
        self.runActions('run-cmds', kw)

    def loadConfig(self, path, argsForConfig=None, appVars=None):
        if not path and not appVars:
            #todo: path = self.DEFAULT_CONFIG_PATH (e.g. server-config.py)
            raise CmdArgError('you must specify a config file using -a')
        if path and not os.path.exists(path):
            raise CmdArgError('%s not found' % path)

        if not self.BASE_MODEL_URI:
            import socket
            self.BASE_MODEL_URI= 'http://' + socket.getfqdn() + '/'

        kw = globals().copy() #copy this module's namespace

        if path:
            def includeConfig(path):
                 kw['__configpath__'].append(os.path.abspath(path))
                 execfile(path, globals(), kw)
                 kw['__configpath__'].pop()

            kw['__server__'] = self
            kw['__argv__'] = argsForConfig or []
            kw['__include__'] = includeConfig
            kw['__configpath__'] = [os.path.abspath(path)]
            execfile(path, kw)

        if appVars:
            kw.update(appVars)        
        self.config = utils.defaultattrdict(appVars or {})

        if kw.get('beforeConfigHook'):
            kw['beforeConfigHook'](kw)

        def initConstants(varlist, default):
            return assignVars(self, kw, varlist, default)

        initConstants( [ 'nsMap', 'extFunctions', 'actions',
                         'authorizationDigests',
                         'NOT_CACHEABLE_FUNCTIONS', ], {} )
        initConstants( ['DEFAULT_MIME_TYPE'], '')

        initConstants( ['appBase'], self.appBase)
        assert self.appBase[0] == '/', "appBase must start with a '/'"
        initConstants( ['BASE_MODEL_URI'], self.BASE_MODEL_URI)
        initConstants( ['appName'], self.appName)
        #appName is a unique name for this request processor instance
        if not self.appName:
            self.appName = re.sub(r'\W','_', self.BASE_MODEL_URI)
        self.log = logging.getLogger("raccoon." + self.appName)

        useFileLock = kw.get('useFileLock')
        if useFileLock:
            if isinstance(useFileLock, type):
                self.LockFile = useFileLock
            else:
                self.LockFile = glock.LockFile
        else:
            self.LockFile = glock.NullLockFile #the default

        self.txnSvc = transactions.RaccoonTransactionService(self)
        domStoreFactory = kw.get('domStoreFactory', DomStore.BasicStore)
        self.domStore = domStoreFactory(self, **kw)
        self.domStore.addTrigger = self.txnSvc.addHook
        self.domStore.removeTrigger = self.txnSvc.removeHook
        if 'before-new' in self.actions:
            #newResourceHook is optional since it's expensive
            self.domStore.newResourceTrigger = self.txnSvc.newResourceHook

        self.defaultRequestTrigger = kw.get('DEFAULT_TRIGGER','http-request')
        initConstants( ['globalRequestVars', 'static_path', 'template_path'], [])
        self.globalRequestVars.extend( self.defaultGlobalVars )
        self.defaultPageName = kw.get('defaultPageName', 'index')
        #cache settings:
        initConstants( ['LIVE_ENVIRONMENT', 'SECURE_FILE_ACCESS', 'useEtags'], 1)
        self.defaultExpiresIn = kw.get('defaultExpiresIn', 0)
        initConstants( ['ACTION_CACHE_SIZE'], 1000)
        #disable by default(default cache size used to be 10000000 (~10mb))
        initConstants( ['maxCacheableStream','FILE_CACHE_SIZE'], 0)

        self.PATH = kw.get('PATH', self.PATH)
        
        self.authorizeMetadata = kw.get('authorizeMetadata',
                                        lambda *args: True)
        self.validateExternalRequest = kw.get('validateExternalRequest',
                                        lambda *args: True)
        self.getPrincipleFunc = kw.get('getPrincipleFunc', lambda kw: '')

        self.MODEL_RESOURCE_URI = kw.get('MODEL_RESOURCE_URI',
                                         self.BASE_MODEL_URI)

        self.cmd_usage = DEFAULT_cmd_usage + kw.get('cmd_usage', '')

        self.nsMap.update(DefaultNsMap)
        
        if kw.get('configHook'):
            kw['configHook'](kw)

    def getLock(self):
        '''
        Acquires and returns the lock associated with this RequestProcessor.
        Call release() on the returned lock object to release it.
        '''
        assert self.lock
        return glock.LockGetter(self.lock)

    def loadModel(self):
        if not self.lock:
            lockName = 'r' + str(hash(self.MODEL_RESOURCE_URI)) + '.lock'
            self.lock = self.LockFile(lockName)

        lock = self.getLock()
        try:
            self.actionCache = MRUCache.MRUCache(self.ACTION_CACHE_SIZE,
                                                 digestKey=True)

            self.domStore.loadDom()
        finally:
            lock.release()
        self.runActions('load-model')

###########################################
## request handling engine
###########################################

    def runActions(self, triggerName, kw = None, initVal=None, newTransaction=True):
        '''
        Retrieve the action sequences associated with the triggerName.
        Each Action has a list of RxPath expressions that are evaluated after
        mapping runActions keyword parameters to RxPath variables.  If an
        expression returns a non-empty nodeset the Action is invoked and the
        value it returns is passed to the next invoked Action until the end of
        the sequence, upon which the final return value is return by this function.
        '''
        kw = utils.attrdict(kw or {})
        sequence = self.actions.get(triggerName)
        if sequence:
            errorSequence = self.actions.get(triggerName+'-error')
            return self.doActions(sequence, kw, retVal=initVal,
                errorSequence=errorSequence, newTransaction=newTransaction)

    def _doActionsBare(self, sequence, kw, retVal):
        try:
            if not isinstance(sequence, (list, tuple)):
                sequence = sequence(kw)

            for action in sequence:
                retResult = Result(retVal)
                #try to retrieve action result from cache
                #when an action is not cachable (the default)
                #just calls the action
                newRetVal = self.actionCache.getOrCalcValue(
                    action, kw, retResult,
                    hashCalc=action.cacheKeyPredicate,
                    sideEffectsCalc=action.sideEffectsPredicate,
                    sideEffectsFunc=action.sideEffectsFunc,
                    isValueCacheableCalc=action.isValueCacheableCalc)

                if (newRetVal is not retResult
                        and newRetVal is not Action.NotModified):
                    retVal = newRetVal
        except:
            exc = ActionWrapperException()
            exc.state = retVal
            raise exc
        return retVal

    def _doActionsTxn(self, sequence, kw, retVal):
        func = lambda: self._doActionsBare(sequence, kw, retVal)
        return self.executeTransaction(func, kw, retVal)
        
    def executeTransaction(self, func, kw=None, retVal=None):
        kw = kw or {}
        self.txnSvc.begin()
        self.txnSvc.state.kw = kw
        self.txnSvc.state.retVal = retVal
        try:
            retVal = func()
        except:
            if self.txnSvc.isActive(): #else its already been aborted
                self.txnSvc.abort()
            raise
        else:
            if self.txnSvc.isActive(): #could have already been aborted
                self.txnSvc.addInfo(source=self.getPrincipleFunc(kw))
                self.txnSvc.state.retVal = retVal
                if self.txnSvc.isDirty():
                    if kw.get('__readOnly'):
                        self.log.warning(
                        'a read-only transaction was modified and aborted')
                        self.txnSvc.abort()
                    else:
                        self.txnSvc.commit()
                else: #don't bother committing
                    self.txnSvc.abort()    #need this to clean up the transaction
        return retVal

    if sys.version_info[:2] > (2,4):
        from contextlib import contextmanager

        @contextmanager
        def inTransaction(self, kw=None):
            kw = kw or {}
            self.txnSvc.begin()
            self.txnSvc.state.kw = kw

            try:
                yield self
            except:
                if self.txnSvc.isActive(): #else its already been aborted
                    self.txnSvc.abort()
                raise
            else:
                if self.txnSvc.isActive(): #could have already been aborted
                    self.txnSvc.addInfo(source=self.getPrincipleFunc(kw))
                    if self.txnSvc.isDirty():
                        if kw.get('__readOnly'):
                            self.log.warning(
                            'a read-only transaction was modified and aborted')
                            self.txnSvc.abort()
                        else:
                            self.txnSvc.commit()
                    else: #don't bother committing
                        self.txnSvc.abort()    #need this to clean up the transaction

    def doActions(self, sequence, kw=None, retVal=None,
                  errorSequence=None, newTransaction=False):
        if kw is None: 
            kw = utils.attrdict()

        kw['__requestor__'] = self.requestDispatcher
        kw['__server__'] = self

        try:
            if newTransaction:
                retVal = self._doActionsTxn(sequence, kw, retVal)
            else:
                retVal = self._doActionsBare(sequence, kw, retVal)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            #print newTransaction, self.txnSvc.state.timestamp
            exc_info = sys.exc_info()
            if isinstance(exc_info[1], ActionWrapperException):
                retVal = exc_info[1].state
                exc_info = exc_info[1].nested_exc_info

            if self.inErrorHandler or kw.get('_noErrorHandling'):
                #avoid endless loops
                raise exc_info[1] or exc_info[0], None, exc_info[2]
            else:
                self.inErrorHandler += 1
            try:
                if isinstance(exc_info[1], DoNotHandleException):
                    raise exc_info[1] or exc_info[0], None, exc_info[2]

                if errorSequence and sequence is not errorSequence:
                    import traceback as traceback_module
                    def extractErrorInfo(type, value):
                        #value may be either the nested exception
                        #or the wrapper exception
                        message = str(value)
                        module = '.'.join( str(type).split('.')[:-1] )
                        name = str(type).split('.')[-1].strip("'>")
                        errorCode = getattr(value, 'errorCode', '')
                        return message, module, name, errorCode

                    def getErrorKWs():
                        type, value, traceback = exc_info
                        if (isinstance(value, utils.NestedException)
                                and value.useNested):
                            message, module, name, errorCode=extractErrorInfo(
                                 value.nested_exc_info[0],
                                 value.nested_exc_info[1])
                        else:
                            message, module, name, errorCode=extractErrorInfo(
                                                             type, value)
                        #these should always be the wrapper exception:
                        (fileName, lineNumber, functionName,
                            text) = traceback_module.extract_tb(
                                                    traceback, 1)[0]
                        details = ''.join(
                            traceback_module.format_exception(
                                        type, value, traceback) )
                        return utils.attrdict(locals())

                    kw['_errorInfo'] = getErrorKWs()
                    self.log.warning("invoking error handler on exception:\n"+
                                     kw['_errorInfo']['details'])
                    try:
                        #if we're creating a new transaction,
                        #it has been aborted by now, so start a new one
                        #however if the error was thrown during commit we're in the midst 
                        #of a bad transaction and its not safe to create a new one
                        newTransaction = newTransaction and not self.txnSvc.isActive()
                        return self.callActions(errorSequence, kw, retVal,
                            newTransaction=newTransaction)
                    finally:
                        del kw['_errorInfo']
                else:
                    #traceback.print_exception(*exc_info)
                    raise exc_info[1] or exc_info[0], None, exc_info[2]
            finally:
                self.inErrorHandler -= 1
        return retVal

    def callActions(self, actions, kw, retVal, errorSequence=None, globalVars=None, newTransaction=False):
        '''
        process another set of actions using the current context as input,
        but without modified the current context.
        Particularly useful for template processing.
        '''
        globalVars = self.globalRequestVars + (globalVars or [])

        #merge previous prevkw, overriding vars as necessary
        prevkw = kw.get('_prevkw', {}).copy()
        templatekw = utils.attrdict()
        for k, v in kw.items():
            #initialize the templates variable map copying the
            #core request kws and copy the r est (the application
            #specific kws) to _prevkw this way the template
            #processing doesn't mix with the orginal request but
            #are made available in the 'previous' namespace (think
            #of them as template parameters)
            if k in globalVars:
                templatekw[k] = v
            elif k != '_metadatachanges':
                prevkw[k] = v
        templatekw['_prevkw'] = prevkw
        templatekw['_contents'] = Result(retVal)

        return self.doActions(actions, templatekw,
            errorSequence=errorSequence, newTransaction=newTransaction)

class HTTPRequestProcessor(RequestProcessor):
    '''
    Adds functionality for handling an HTTP request.
    '''

    statusMessages = {
        200 : "OK",
        206 : 'Partial Content',
        301 : "Moved Permanently",
        302 : "Moved Temporarily",
        304 : "Not Modified",
        400 : 'Bad Request',
        401 : 'Unauthorized',
        403 : 'Forbidden',
        404 : 'Not Found',
        426 : 'Upgrade Required',
        500 : 'Internal Server Error',
        501 : 'Not Implemented',
        503 : 'Service Unavailable',
    }

    defaultGlobalVars = RequestProcessor.defaultGlobalVars + ['_environ',
            '_uri',
            '_baseUri',
            '_responseCookies',
            '_requestCookies',
            '_responseHeaders',
    ]

    def __init__(self,*args, **kwargs):
        #add missing mimetypes that IE cares about:
        mimetypes.types_map['.ico']='image/x-icon'
        mimetypes.types_map['.htc']='text/x-component'

        super(HTTPRequestProcessor,self).__init__(*args, **kwargs)

    def handleHTTPRequest(self, kw):
        if self.requestsRecord is not None:
            self.requestsRecord.append(kw)

        #if the request name has an extension try to set
        #a default mimetype before executing the request
        name = kw['_name']
        i=name.rfind('.')
        if i!=-1:
            ext=name[i:]
            contentType=mimetypes.types_map.get(ext)
            if contentType:
                kw['_responseHeaders']['content-type']=contentType

        try:
            rc = {}
            #requestContext is used by all Requestor objects
            #in the current thread
            #rc['_environ']=kw['_environ']
            #rc['_responseHeaders'] = kw['_responseHeaders']
            #rc['_session']=kw['_session']
            self.requestContext.append(rc)

            self.validateExternalRequest(kw)

            result = self.runActions('http-request', kw)
            if result is not None:
                #if mimetype is not set, make another attempt
                if 'content-type' not in kw['_responseHeaders']:
                    contentType = self.guessMimeTypeFromContent(result)
                    if contentType:
                        kw['_responseHeaders']['content-type']=contentType

                status = kw['_responseHeaders']['_status']
                if isinstance(status, (float, int)):
                   msg = self.statusMessages.get(status, '')
                   status  = '%d %s' % (status, msg)
                   kw['_responseHeaders']['_status'] = status

                if not status.startswith('200'):
                    return result #don't set the following headers

                if (self.defaultExpiresIn and
                    'expires' not in kw['_responseHeaders']):
                    if self.defaultExpiresIn == -1:
                        expires = '-1'
                    else:
                        expires = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                        time.gmtime(time.time() + self.defaultExpiresIn))
                    kw['_responseHeaders']['expires'] = expires

                #XXX this etag stuff should be an action
                if self.useEtags:
                    resultHash = kw['_responseHeaders'].get('etag')
                    #if the app already set the etag use that value instead
                    if resultHash is None and isinstance(result, str):
                        resultHash = '"' + md5(result).hexdigest() + '"'
                        kw['_responseHeaders']['etag'] = resultHash
                    etags = kw['_environ'].get('HTTP_IF_NONE_MATCH')
                    if etags and resultHash in [x.strip() for x in etags.split(',')]:
                        kw['_responseHeaders']['_status'] = "304 Not Modified"
                        return ''

                return result
        finally:
            self.requestContext.pop()

        return self.default_not_found(kw)

    def wsgi_app(self, environ, start_response):
        """
        Converts an HTTP request into these kws:

        _environ
        _params
        _uri
        _baseUri
        _name
        _responseheaders
        _responsecookies
        _requestcookies
        """
        import Cookie, wsgiref.util
        _name = environ['PATH_INFO'].strip('/')
        if not _name:
            _name = self.defaultPageName

        _responseCookies = Cookie.SimpleCookie()
        _responseHeaders = utils.attrdict(_status="200 OK") #include response code pseudo-header
        kw = utils.attrdict(_environ=utils.attrdict(environ),
            _uri = wsgiref.util.request_uri(environ),
            _baseUri = wsgiref.util.application_uri(environ),
            _responseCookies = _responseCookies,
            _requestCookies = Cookie.SimpleCookie(environ.get('HTTP_COOKIE', '')),
            _responseHeaders= _responseHeaders,
            _name=_name
        )
        paramsdict = get_http_params(environ)
        kw.update(paramsdict)
        response = self.handleHTTPRequest(kw)

        status = _responseHeaders.pop('_status')
        headerlist = _responseHeaders.items()
        if len(_responseCookies):
            headerlist += [('Set-Cookie', m.OutputString() )
                            for m in _responseCookies.values()]

        start_response(status, headerlist)
        if hasattr(response, 'read'): #its a file not a string
            block_size = 8192
            if 'wsgi.file_wrapper' in environ:
                return environ['wsgi.file_wrapper'](response, block_size)
            else:
                return iter(lambda: response.read(block_size), '')
        else:
            return [response]

    def guessMimeTypeFromContent(self, result):
        #obviously this could be improved,
        #e.g. detect encoding in xml header or html meta tag
        #or handle the BOM mark in front of the <?xml
        #detect binary vs. text, etc.
        if isinstance(result, (str, unicode)):
            test = result[:30].strip().lower()
        else:
            test = ''
        if test.startswith("<html") or result.startswith("<!doctype html"):
            return "text/html"
        elif test.startswith("<?xml") or test[2:].startswith("<?xml"):
            return "text/xml"
        elif self.DEFAULT_MIME_TYPE:
            return self.DEFAULT_MIME_TYPE
        else:
            return None

    def default_not_found(self, kw):
        kw['_responseHeaders']["content-type"]="text/html"
        kw['_responseHeaders']['_status'] = "404 Not Found"
        return '''<html><head><title>Error 404</title>
<meta name="robots" content="noindex" />
</head><body>
<h2>HTTP Error 404</h2>
<p><strong>404 Not Found</strong></p>
<p>The Web server cannot find the file or script you asked for.
Please check the URL to ensure that the path is correct.</p>
<p>Please contact the server's administrator if this problem persists.</p>
</body></html>'''

    def saveRequestHistory(self):
        if self.requestsRecord:
            requestRecordFile = file(self.requestRecordPath, 'wb')
            pickle.dump(self.requestsRecord, requestRecordFile)
            requestRecordFile.close()

    def playbackRequestHistory(self, debugFileName, out=sys.stdout):
        requests = pickle.load(file(debugFileName, 'rU'))
        import repr
        rpr = repr.Repr()
        rpr.maxdict = 20
        rpr.maxlevel = 2
        for i, request in enumerate(requests):
            verb = request['_environ']['REQUEST_METHOD']
            login = request.get('_session',{}).get('login','')
            print>>out, i, verb, request['_name'], 'login:', login
            #print form variables
            print>>out, rpr.repr(dict([(k, v) for k, v in request.items()
                                if isinstance(v, (unicode, str, list))]))

            self.handleHTTPRequest(request)

    def runWsgiServer(self, port=8000, server=None, middleware=None):
        if not server:
            from wsgiref.simple_server import make_server
            server = make_server
        if middleware:
            app = middleware(self.wsgi_app)
        else:
            app = self.wsgi_app
        httpd = server('', port, app)
        try:
            httpd.serve_forever()
        finally:
            self.runActions('shutdown')
            self.saveRequestHistory()

class UploadFile(object):

    def __init__(field):
        self._field = field

    filename = property(lambda self: self._field.filename)
    file = property(lambda self: self._field.file)
    contents = property(lambda self: self._field.value)

def get_http_params(environ):        
    '''build _params (and maybe _postContent)'''
    import cgi

    _params = {}
    _postContent = None
    getparams = utils.defaultattrdict()
    postparams = utils.defaultattrdict()

    if environ.get('QUERY_STRING'):
        forms = cgi.FieldStorage(environ=environ, keep_blank_values=1)
        for key in forms.keys():
            valueList = forms[key]
            if isinstance(valueList, list):# Check if it's a list or not
                getparams[key]= [item.value for item in valueList]
            else:
                getparams[key] = valueList.value

    if environ['REQUEST_METHOD'] == 'POST':
        forms = cgi.FieldStorage(fp=environ.get('wsgi.input'),
                                environ=environ, keep_blank_values=1)
        if forms.list is None:
            assert forms.file is not None
            _postContent = forms.file.read()
        else:
            for key in forms.keys():
                valueList = forms[key]
                if isinstance(valueList, list):# Check if it's a list or not
                    postparams[key]= [item.value for item in valueList]
                else:
                    # In case it's a file being uploaded, we save the filename in a map (user might need it)
                    if not valueList.filename:
                        postparams[key] = valueList.value
                    else:
                        postparams[key] = UploadFile(valueList)

    if getparams and postparams:
        #merge the dicts together
        for k,v in getparams.items():
            if k in postparams:
                if not isinstance(v, list):
                    v = [v]
                pv = postparams[k]
                if isinstance(pv, list):
                    v.extend(pv)
                else:
                    v.append(pv)
            postparams[k] = v
        _params = postparams
    else:
        _params = getparams or postparams

    return dict(_params=_params, _postContent=_postContent)

#################################################
##command line handling
#################################################
def argsToKw(argv, cmd_usage):
    kw = { }

    i = iter(argv)
    try:
        arg = i.next()
        while 1:
            if arg[0] != '-':
                raise CmdArgError('missing arg')
            name = arg.lstrip('-')
            kw[name] = True
            arg = i.next()
            if arg[0] != '-':
                kw[name] = arg
                arg = i.next()
    except StopIteration: pass
    #print 'args', kw
    return kw

def translateCmdArgs(data):
    """
    translate raccoonrunner vars into shell args suitable for RequestProcessor init
    """
    replacements = [("CONFIG_PATH", "a"), ("SOURCE", "m"), ("RACCOON_PATH", "p"),
                    ("APP_BASE", "appBase"), ("APP_NAME", "appName"), ("MODEL_URI", "model_uri")]
    for x in replacements:
      if x[0] in data:
        data[x[1]] = data[x[0]]
        del data[x[0]]
    return data

DEFAULT_cmd_usage = 'python raccoon.py -l [log.config] -r -d [debug.pkl] -x -s server.cfg -p path -m store.nt -a config.py '
cmd_usage = '''
-h this help message
-l [log.config] specify a config file for logging
-r record requests (ctrl-c to stop recording) 
-d [debug.pkl]: debug mode (replay the requests saved in debug.pkl)
-x exit after executing config specific cmd arguments
-p specify the path (overrides RACCOONPATH env. variable)
-m [store.nt] load the RDF model
   (default model supports .rdf, .nt, .mk)
-a config.py run the application specified
'''

def parse_args(argv=sys.argv[1:], out=sys.stdout):
    "parse cmd args and return vars suitable for passing to run"
    vars = {}
    try:
        eatNext = False
        mainArgs, rootArgs, configArgs = [], [], []
        for i in range(len(argv)):
            if argv[i] == '-a':
                rootArgs += argv[i:i+2]
                configArgs += argv[i+2:]
                break
            if argv[i] in ['-d', '-r', '-x', '-s', '-l', '-h', '--help'
                           ] or (eatNext and argv[i][0] != '-'):
                eatNext = argv[i] in ['-d', '-s', '-l']
                mainArgs.append( argv[i] )
            else:
                rootArgs.append( argv[i] )

        if '-l' in mainArgs:
            try:
                logConfig=mainArgs[mainArgs.index("-l")+1]
                if logConfig[0] == '-':
                    raise ValueError
            except (IndexError, ValueError):
                logConfig = 'log.config'
            if not os.path.exists(logConfig):
                raise CmdArgError("%s not found" % logConfig)

            vars['LOG_CONFIG'] = logConfig

        vars.update(argsToKw(rootArgs, DEFAULT_cmd_usage))
        vars['argsForConfig'] = configArgs
        #print 'ma', mainArgs
        if '-h' in mainArgs or '--help' in mainArgs:
            raise CmdArgError('')

        if '-d' in mainArgs:
            try:
                debugFileName=mainArgs[mainArgs.index("-d")+1]
                if debugFileName[0] == '-':
                    raise ValueError
            except (IndexError, ValueError):
                debugFileName = 'debug-wiki.pkl'
            vars['DEBUG_FILENAME'] = debugFileName

        else:
            if '-r' in mainArgs:
                vars['RECORD_REQUESTS'] = True

            #if -x (execute cmdline and exit) we're done
            if '-x' in mainArgs:
                vars['EXEC_CMD_AND_EXIT'] = True

    except (CmdArgError), e:
        print>>out, e
        print>>out, 'usage:'
        print>>out, DEFAULT_cmd_usage +'[config specific options]'
        print>>out, cmd_usage

    return vars

def initLogConfig(logConfig):
    import logging.config as log_config
    if isinstance(logConfig,(str,unicode)) and logConfig.lstrip()[:1] in ';#[':
        #looks like a logging configuration 
        import textwrap
        logConfig = StringIO.StringIO(textwrap.dedent(logConfig))
    log_config.fileConfig(logConfig)
    #any logger already created and not explicitly
    #specified in the log config file is disabled this
    #seems like a bad design -- certainly took me a while
    #to why understand things weren't getting logged so
    #re-enable the loggers
    for logger in logging.Logger.manager.loggerDict.itervalues():
        logger.disabled = 0
    
class AppConfig(utils.attrdict):
    _server = None
    
    def load(self):
        if 'STORAGE_URL' in self:        
            (proto, path) = self['STORAGE_URL'].split('://')

            self['modelFactory'] = store.get_factory(proto)
            self['STORAGE_PATH'] = path

        if self.get('logconfig'):
            initLogConfig(self['logconfig'])

        kw = translateCmdArgs(self)
        root = HTTPRequestProcessor(a=kw.get('a'), appName='root', appVars=kw)
        dict.__setattr__(self, '_server', root)
        return self._server
        
    def run(self, startserver=True, out=sys.stdout):
        root = self._server
        if not root:
            root = self.load()

        if 'DEBUG_FILENAME' in self:
            self.playbackRequestHistory(self['DEBUG_FILENAME'], out)

        if self.get('RECORD_REQUESTS'):
            root.requestsRecord = []
            root.requestRecordPath = 'debug-wiki.pkl'

        if not self.get('EXEC_CMD_AND_EXIT', not startserver):
            port = self.get('PORT', 8000)
            if self.get('firepython_enabled'):
                import firepython.middleware
                middleware = firepython.middleware.FirePythonWSGI
            else:
                middleware = None
            httpserver = self.get('httpserver')
            print>>out, "Starting HTTP on port %d..." % port
            #runs forever:
            root.runWsgiServer(port, httpserver, middleware)

        return root

def createStore(json='', storageURL = 'mem://', idGenerator='counter', **kw):
    root = createApp(
        STORAGE_URL = storageURL,
        STORAGE_TEMPLATE = json,
        storageTemplateOptions = dict(generateBnode=idGenerator),
        **kw    
    ).run(False)
    return root.domStore

_current_config = AppConfig()
_current_configpath = [None]

def _normpath(basedir, path):
    return [os.path.isabs(dir) and dir or os.path.normpath(
                        os.path.join(basedir, dir)) for dir in path]

def importApp(baseapp):
    '''
    Executes the given config file and returns a Python module-like object that contains the global variables defined by it.
    If `createApp()` was called during execution, it have an attribute called `_app_config` set to the app configuration returned by `createApp()`.
    '''
    baseglobals = utils.attrdict()
    #set this global so we can resolve relative paths against the location
    #of the config file they appear in
    _current_configpath.append( os.path.dirname(os.path.abspath(baseapp)) )
    #assuming the baseapp file calls createApp(), it will set _current_config
    execfile(baseapp, baseglobals)
    _current_configpath.pop()
    baseglobals._app_config = _current_config
    return baseglobals

def createApp(baseapp=None, static_path=(), template_path=(), actions=None, **config):
    '''
    Returns an `AppConfig` based on the given config parameters.
    If specified, `baseapp` is either a path to config file or an object returned by `importApp`.
    Otherwise, all other parameters are treated as config variables.
    '''
    global _current_config
    if baseapp:
        if isinstance(baseapp, (str, unicode)):
            baseapp = importApp(baseapp)    
        _current_config = baseapp._app_config
    else:
        _current_config = AppConfig()
    
    _current_config.__toplevel__ = not bool(_current_configpath[-1])
    #config variables that shouldn't be simply overwritten should be specified 
    #as an explicit function args
    _current_config.update(config)
        
    if 'actions' in _current_config:
        if actions:
            _current_config.actions.update(actions)
    else:
        _current_config.actions = actions or {}
    
    basedir = _current_configpath[-1] or os.getcwd()
    
    if isinstance(static_path, (str, unicode)):
        static_path = [static_path]     
    static_path = list(static_path) + _current_config.get('static_path',[])
    _current_config.static_path = _normpath(basedir, static_path)

    if isinstance(template_path, (str, unicode)):
        template_path = [template_path]    
    template_path = list(template_path) + _current_config.get('template_path',[])
    _current_config.template_path = _normpath(basedir, template_path)
    
    return _current_config

#XXX clean up args and implement this as the doc says
def main(argv=sys.argv[1:], out=sys.stdout):
    '''
    usage app-config.py [options]
    Any appconfig variables can be passed as an command line option 
    and will override the config value set in the app.
    For convenience, short alternative are available:

    -l [log.config] LOG_CONFIG specify a config file for logging
    -x EXEC_CMD_AND_EXIT exit after executing config specific cmd arguments
    -m [store.json] SOURCE (connect to/load the store)
    -r RECORD_REQUESTS record requests (ctrl-c to stop recording) 
    -d [debug.pkl] DEBUG_FILENAME debug mode (replay the requests saved in debug.pkl)    
    '''
    # mimics behavior of old main(), not really used anywhere
    vars = parse_args(argv, out)
    createApp(**vars).run(out=out)
    return 0
