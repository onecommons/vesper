"""
    vesper.app
    ==========
    This module defines the framework used by Vesper to bootstrap a running server from 
    a given configuration.
"""
import os, os.path, sys, traceback, re

from vesper import utils
from vesper.utils import glock, MRUCache
from vesper.utils.Uri import UriToOsPath
from vesper.data import base, DataStore, transactions, store
from vesper.data.transaction_processor import TransactionProcessor

try:
    import cStringIO
    StringIO = cStringIO
except ImportError:
    import StringIO

import logging
DEFAULT_LOGLEVEL = logging.INFO

#logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
#logging.root.setLevel(DEFAULT_LOGLEVEL)
#logging.basicConfig()

log = logging.getLogger("app")
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

def assignAttrs(obj, configDict, varlist, default):
    '''
    Helper function for adding attributes to an object
    given a dictionary of configuration properties
    '''
    import copy
    for name in varlist:
        try:
            defaultValue = copy.copy(default)
        except TypeError:
            #probably ok, can't copy certain non-mutable objects like functions
            defaultValue = default
        value = configDict.get(name, defaultValue)
        if default is not None and not isinstance(value, type(default)):
            raise RaccoonError('config variable %s (of type %s)'
                               'must be compatible with type %s'
                               % (name, type(value), type(default)))
        setattr(obj, name, value)

############################################################
##Raccoon main class
############################################################
class RequestProcessor(TransactionProcessor):
    DEFAULT_CONFIG_PATH = ''#'raccoon-default-config.py'

    requestsRecord = None

    defaultGlobalVars = ['_name', '_noErrorHandling',
            '__current-transaction', '__readOnly'
            '__requestor__', '__server__',
            '_prevkw', '__argv__', '_errorInfo'
            ]

    def __init__(self, appVars):
        self.baseDir = os.getcwd() #XXX make configurable
        self.loadConfig(appVars)
                 
        # XXX copy and paste from
        self.initThreadLocals(requestContext=None, inErrorHandler=0, previousResolvers=None)
        self.requestContext = [{}] #stack of dicts
        self.lock = None
        self.log = log
        #######################
                
        if self.template_path:
            from mako.lookup import TemplateLookup
            self.template_loader = TemplateLookup(directories=self.template_path, 
                default_filters=['decode.utf8'], module_directory='mako_modules',
                output_encoding='utf-8', encoding_errors='replace')
        self.requestDispatcher = Requestor(self)
        self.loadModel()
        self.handleCommandLine(self.argsForConfig)

    def handleCommandLine(self, argv):
        '''  the command line is translated into the `_params`
        request variable as follows:

        * arguments beginning with a '-' are treated as a variable
        name with its value being the next argument unless that
        argument also starts with a '-'

        * the entire command line is assigned to the variable 'cmdline'
        '''
        kw = utils.attrdict()
        kw._params = utils.defaultattrdict(argsToKw(argv))        
        #XXX use self.cmd_usage
        kw['cmdline'] = '"' + '" "'.join(argv) + '"'
        self.runActions('run-cmds', kw)

    def loadConfig(self, appVars):
        self.BASE_MODEL_URI = appVars.get('model_uri')
        if not self.BASE_MODEL_URI:
            import socket
            self.BASE_MODEL_URI= 'http://' + socket.getfqdn() + '/'

        self.config = utils.defaultattrdict(appVars)

        if appVars.get('beforeConfigHook'):
            appVars['beforeConfigHook'](appVars)

        def initConstants(varlist, default):
            #add the given list of config properties as attributes
            #on this RequestProcessor
            return assignAttrs(self, appVars, varlist, default)

        initConstants( [ 'actions'], {})
        initConstants( ['DEFAULT_MIME_TYPE'], '')
        initConstants( ['BASE_MODEL_URI'], self.BASE_MODEL_URI)
        initConstants( ['appName'], 'root')
        #appName is a unique name for this request processor instance
        if not self.appName:
            self.appName = re.sub(r'\W','_', self.BASE_MODEL_URI)
        self.log = logging.getLogger("app." + self.appName)

        useFileLock = appVars.get('useFileLock')
        if useFileLock:
            if isinstance(useFileLock, type):
                self.LockFile = useFileLock
            else:
                self.LockFile = glock.LockFile
        else:
            self.LockFile = glock.NullLockFile #the default
        
        self.loadDataStore(appVars)
                
        self.defaultRequestTrigger = appVars.get('DEFAULT_TRIGGER','http-request')
        initConstants( ['globalRequestVars', 'static_path', 'template_path'], [])
        self.globalRequestVars.extend( self.defaultGlobalVars )
        self.defaultPageName = appVars.get('defaultPageName', 'index')
        #cache settings:
        initConstants( ['SECURE_FILE_ACCESS', 'useEtags'], True)
        self.defaultExpiresIn = appVars.get('defaultExpiresIn', 0)
        initConstants( ['ACTION_CACHE_SIZE'], 0)
        self.validateExternalRequest = appVars.get('validateExternalRequest',
                                        lambda *args: True)
        self.getPrincipleFunc = appVars.get('getPrincipleFunc', lambda kw: '')

        self.MODEL_RESOURCE_URI = appVars.get('MODEL_RESOURCE_URI',
                                         self.BASE_MODEL_URI)
        
        self.argsForConfig = appVars.get('argsForConfig', [])
        #XXX self.cmd_usage = DEFAULT_cmd_usage + kw.get('cmd_usage', '')
        
        if appVars.get('configHook'):
            appVars['configHook'](appVars)

    def loadModel(self):
        self.actionCache = MRUCache.MRUCache(self.ACTION_CACHE_SIZE,
                                             digestKey=True)
        super(RequestProcessor, self).loadModel()
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


#################################################
##command line handling
#################################################
def argsToKw(argv):
    '''
    '''
    kw = {}

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
    
    return kw

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
    
    def updateFromConfigFile(self, filepath):
        # XXX check to see if we've already started running
        config = {}
        execfile(filepath, config, config)
        self.update(config)
    
    def load(self):
        if self.get('logconfig'):
            initLogConfig(self['logconfig'])

        if self.get('STORAGE_URL'):
            (proto, path) = re.split(r':(?://)?', self['STORAGE_URL'],1)

            self['modelFactory'] = store.get_factory(proto)
            if proto == 'file':
                path = UriToOsPath(path)            
            self['STORAGE_PATH'] = path
            #XXX if modelFactory is set should override STORAGE_URL            
            log.info("Using %s at %s" % (self['modelFactory'].__name__, self['STORAGE_PATH']))

        from web import HTTPRequestProcessor
        root = HTTPRequestProcessor(appVars=self.copy())
        dict.__setattr__(self, '_server', root)
        global _current_config
        if self is _current_config:
            _current_config = None
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
            middleware =  self.get('wsgi_middleware')
            httpserver = self.get('httpserver')
            print>>out, "Starting HTTP on port %d..." % port
            #runs forever:
            root.runWsgiServer(port, httpserver, middleware)

        return root

def createStore(json='', storageURL = 'mem://', idGenerator='counter', **kw):
    #XXX very confusing that storageURL spelling doesn't match STORAGE_URL 
    root = createApp(
        STORAGE_URL = storageURL,
        STORAGE_TEMPLATE = json,
        storageTemplateOptions = dict(generateBnode=idGenerator),
        **kw    
    ).run(False)
    return root.dataStore

_current_config = AppConfig()
_current_configpath = [None]

def _normpath(basedir, path):
    """
    return an absolute path given a basedir and a path fragment.  If `path` is already absolute
    it will be returned unchanged.
    """
    if os.path.isabs(path):
        return path
    else:
        tmp = os.path.normpath(os.path.join(basedir, path))
        if os.path.isabs(tmp):
            return tmp

def _get_module_path(modulename):
    "for a modulename like 'vesper.web.admin' return a tuple (absolute_module_path, is_directory)"
    import sys, imp
    if modulename == "__main__":
        m = sys.modules[modulename]
        assert hasattr(m, '__file__'), "__main__ module missing __file__ attribute"
        path = _normpath(os.getcwd(), m.__file__)
        return (path, False)
    else:
        parts = modulename.split('.')
        parts.reverse()
        path = None
        while parts:
            part = parts.pop()
            f = None
            try:
                f, path, descr = imp.find_module(part, path and [path] or None)
            finally:
                if f: f.close()
        return (path, descr[-1] == imp.PKG_DIRECTORY)

def _importApp(baseapp):
    '''
    Executes the given app config file. If `createApp()` was 
    called during execution of the config file, the `_current_config`
    global will be set to the app configuration returned by `createApp()`.
    '''
    baseglobals = utils.attrdict(Action=Action, createApp=createApp)
    #assuming the baseapp file calls createApp(), it will set _current_config
    if os.path.exists(baseapp):
        #set this global so we can resolve relative paths against the location
        #of the config file they appear in
        _current_configpath.append( os.path.dirname(os.path.abspath(baseapp)) )
        execfile(baseapp, baseglobals)
    else:
        (path, isdir) = _get_module_path(baseapp)
        # print "_get_module_path for:" + str(baseapp) + " --> path:" + str(path) + " isdir:" + str(isdir)
        assert path
        #set this global so we can resolve relative paths against the location
        #of the config file they appear in
        _current_configpath.append( os.path.abspath(path) )
        __import__(baseapp, baseglobals)
    _current_configpath.pop()

def getCurrentApp():
    return _current_config

def createApp(derivedapp=None, baseapp=None, static_path=(), template_path=(), actions=None, **config):
    '''
    Create a new `AppConfig`.

    :param derivedapp: is the name of the module that is extending the app. (Usually just pass `__name__`)
    :param baseapp: is either a module name or a file path to the Python script that defines an app. 
           This file should have a call to :func:`createApp` in it
    
    :param static_path: list or string prepended to the static resource path of the app.
    :param template_path: list or string prepended to the template resource path of the app.
    :param actions: actions map of the app, will updates the base app's `action` dictionary.
    
    :param config: Any other keyword arguments will override config values set by the base app
    '''
    global _current_config
    
    if derivedapp:
        (derived_path, isdir) = _get_module_path(derivedapp)
        if not isdir:
            derived_path = os.path.dirname(derived_path)
    else:
        derived_path = None
        
    if baseapp:
        assert isinstance(baseapp, (str, unicode))
        #sets _current_config if the baseapp calls createApp
        _importApp(baseapp)
    else:
        _current_config = AppConfig()        
    
    #config variables that shouldn't be simply overwritten should be specified 
    #as an explicit function args
    _current_config.update(config)
    
    if 'actions' in _current_config:
        if actions:
            _current_config.actions.update(actions)
    else:
        _current_config.actions = actions or {}
    
    basedir = _current_configpath[-1] or derived_path
    if basedir is not None:
        if isinstance(static_path, (str, unicode)):
            static_path = [static_path]
        static_path = list(static_path) + _current_config.get('static_path',[])
        _current_config.static_path = [_normpath(basedir, x) for x in static_path]
        # print "static path:" + str(_current_config.static_path)
        assert all([os.path.isdir(x) for x in _current_config.static_path]
                    ), "invalid directory in:" + str(_current_config.static_path)

        if isinstance(template_path, (str, unicode)):
            template_path = [template_path]
        template_path = list(template_path) + _current_config.get('template_path',[])
        _current_config.template_path = [_normpath(basedir, x) for x in template_path]
        # print "template path:" + str(_current_config.template_path)
        assert all([os.path.isdir(x) for x in _current_config.template_path]
                    ), "invalid directory in:" + str(_current_config.template_path)
    
    return _current_config
