#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    vesper.app
    ==========
    This module defines the framework used by Vesper to bootstrap a running server from 
    a given configuration.
"""
import os, os.path, sys, traceback, re
from optparse import OptionParser
import itertools

from vesper import utils
from vesper.utils import MRUCache
from vesper.utils.Uri import UriToOsPath
from vesper.data import base, DataStore, transactions, store
from vesper.data.transaction_processor import TransactionProcessor
from vesper.backports import *
from vesper import VesperError

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
    vesper request as if it was function call

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
            raise VesperError('config variable %s (of type %s)'
                               'must be compatible with type %s'
                               % (name, type(value), type(default)))
        setattr(obj, name, value)

############################################################
## main class
############################################################
class RequestProcessor(TransactionProcessor):
    DEFAULT_CONFIG_PATH = ''

    requestsRecord = None

    defaultGlobalVars = ['_name', '_noErrorHandling',
            '__current-transaction', '__readOnly'
            '__requestor__', '__server__',
            '_prevkw', '__argv__', '_errorInfo'
            ]

    nonMergableConfigDicts = ['nameMap'] 
    
    def __init__(self, appVars):
        super(RequestProcessor, self).__init__()
        self.baseDir = os.getcwd() #XXX make configurable
        self.loadConfig(appVars)                                 
        if self.template_path:
            from mako.lookup import TemplateLookup
            templateArgs = dict(directories=self.template_path, 
                default_filters=['decode.utf8'], 
                module_directory =self.mako_module_dir,
                output_encoding='utf-8', encoding_errors='replace')
            templateArgs.update(self.template_options)
            self.template_loader = TemplateLookup(**templateArgs)
        self.requestDispatcher = Requestor(self)
        self.loadModel()
        self.handleCommandLine(self.argsForConfig)

    @property
    def defaultStore(self):
        return self.stores.get('default')
        
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
        kw['cmdline'] = '"' + '" "'.join(argv) + '"'
        self.runActions('run-cmds', kw)

    def loadConfig(self, appVars):
        self.config = utils.defaultattrdict(appVars)

        if appVars.get('beforeConfigHook'):
            appVars['beforeConfigHook'](appVars)

        def initConstants(varlist, default):
            #add the given list of config properties as attributes
            #on this RequestProcessor
            return assignAttrs(self, appVars, varlist, default)

        initConstants( [ 'actions'], {})
        initConstants( ['default_mime_type'], '')        
        self.initLock(appVars)
        self.txnSvc = transactions.ProcessorTransactionService(self)
        initConstants( [ 'stores', 'storeDefaults'], {})
        addNewResourceHook = self.actions.get('before-new')
        if 'stores' in appVars:
            stores = utils.attrdict()
            for name, storeConfig in appVars['stores'].items():
                stores[name] = self.loadDataStore(storeConfig, 
                                    self.storeDefaults, addNewResourceHook)
                if storeConfig.get('default_store'):
                    stores['default'] = stores[name]
            if stores and 'default' not in stores:
                if len(stores) > 1:
                    raise VesperError('default store not set')
                else:
                    stores['default'] = stores.values()[0]
            self.stores = stores
        else:
            self.stores = utils.attrdict(default=
             self.loadDataStore(appVars,self.storeDefaults,addNewResourceHook))

        #app_name is a unique name for this request processor instance
        initConstants( ['app_name'], 'root')
        self.log = logging.getLogger("app." + self.app_name)
                        
        self.defaultRequestTrigger = appVars.get('default_trigger','http-request')
        initConstants( ['global_request_vars', 'static_path', 'template_path'], [])
        self.mako_module_dir = appVars.get('mako_module_dir', 'mako_modules')
        initConstants( ['template_options'], {})        
        self.global_request_vars.extend( self.defaultGlobalVars )
        self.default_page_name = appVars.get('default_page_name', 'index')
        #cache settings:
        initConstants( ['secure_file_access', 'use_etags'], True)
        self.default_expires_in = appVars.get('default_expires_in', 0)
        initConstants( ['action_cache_size'], 0)
        self.validate_external_request = appVars.get('validate_external_request',
                                        lambda *args: True)
        self.get_principal_func = appVars.get('get_principal_func', lambda kw: '')        
        self.argsForConfig = appVars.get('argsForConfig', [])
        
        if appVars.get('configHook'):
            appVars['configHook'](appVars)

    def loadModel(self):
        self.actionCache = MRUCache.MRUCache(self.action_cache_size,
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
        globalVars = self.global_request_vars + (globalVars or [])

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
            longArg = arg[:2] == '--'
            name = arg.lstrip('-')
            if not longArg and len(name) > 1:
                #e.g. -fname
                kw[name[0]] = name[1:]
                arg = i.next()
            elif longArg and '=' in name:
                name, val = name.split('=', 1)
                kw[name] = val
                arg = i.next()
            else:                
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

    cmd_usage = "%prog [options] [settings]"
    cmd_help = '''Settings:\n--name=VALUE Add [name] to config settings'''
    #XXX add to docs
    parser = OptionParser()
    parser.add_option("-s", "--storage", dest="storage", help="storage path or url")
    parser.add_option("-c", "--config", dest="config", help="path to configuration file")
    parser.add_option("-p", "--port", dest="port", type="int", help="http server listener port")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", 
                            default=False, help="set logging level to DEBUG")
    parser.add_option("-x", "--exit", action="store_true", dest="exit", 
                                help="exit without running HTTP server")
        
    def updateFromConfigFile(self, filepath):
        config = {}
        execfile(filepath, config, config)
        utils.recursiveUpdate(self, config, RequestProcessor.nonMergableConfigDicts)
    
    def isBuiltinCmdOption(self, arg):
        return self.parser.has_option(arg)

    def handleCmdOptions(self, args):
        (options, args) = self.parser.parse_args(args)
        if options.config:    
            self.updateFromConfigFile(options.config)

        if options.verbose:
            self.loglevel = logging.DEBUG

        if options.storage:
            self.storage_url = options.storage
        if options.port:
            self.port = int(options.port)
        if options.exit:
            self.exec_cmd_and_exit = True

    def _parseCmdLine(self, cmdline):
        appargs = []
        mainargs = []
        want = False
        while cmdline:
            arg = cmdline.pop(0)
            if arg.startswith('-'):
                want = self.isBuiltinCmdOption(arg)
                if want:
                    mainargs.append(arg)
                else:
                    appargs.append(arg)
            elif want:
                mainargs.append(arg)
            else:
                appargs.append(arg)

        self.parser.set_usage(self.get('cmd_usage', self.cmd_usage))
        self.parser.epilog = self.get('cmd_help', self.cmd_help)
        
        if mainargs:
            self.handleCmdOptions(mainargs)            
        
        handler = self.get('cmdline_handler', lambda app, appargs: appargs)
        appLeftOver = handler(self, appargs)
        if appLeftOver:
            #if cmd_args not set, set it to appLeftOver
            if 'cmd_args' not in self:
                self.cmd_args = appLeftOver
            #also treat appLeftOver as config settings
            try: 
                moreConfig = argsToKw(appLeftOver)
                self.update( moreConfig )
            except CmdArgError, e:
                print "Error:", e.msg
                self.parser.print_help()
                sys.exit()
        
    def load(self, cmdline=False):
        '''
        `cmdline` is a boolean or a list of command line arguments
        If `cmdline` is True, the system command line is used. 
        If False command line processing is disabled.
        '''
        if isinstance(cmdline, bool):
            if cmdline:
                cmdline = sys.argv[1:]
            else:
                cmdline = []
        else:
            cmdline = cmdline[:]
        
        if cmdline:
            self._parseCmdLine(cmdline)
        
        if self.get('logconfig'):
            initLogConfig(self['logconfig'])
        else:
            log = logging.getLogger()
            log.setLevel(self.get('loglevel', logging.INFO))
            # format="%(asctime)s %(levelname)s %(name)s %(message)s"
            # datefmt="%d %b %H:%M:%S"    
            # stream = logging.StreamHandler(sys.stdout)
            # stream.setFormatter(logging.Formatter(format, datefmt))
            # log.addHandler(stream)

        if self.get('storage_url'):
            try:
                (proto, path) = re.split(r':(?://)?', self['storage_url'],1)
            except ValueError: # split didn't work, assume its file path
                proto = 'file'
                path = self['storage_url']
            
            if 'model_factory' not in self:
                self['model_factory'] = store.get_factory(proto)
            if proto == 'file':
                path = UriToOsPath(path)            
            self['storage_path'] = path

        from web import HTTPRequestProcessor
        root = HTTPRequestProcessor(appVars=self.copy())
        dict.__setattr__(self, '_server', root)
        #configuration complete, clear global configuration state:
        _initConfigState()
        return self._server
        
    def run(self, startserver=True, cmdline=True, out=sys.stdout):
        '''
        `cmdline` is a boolean or a list of command line arguments
        If `cmdline` is True, the system command line is used. 
        If False command line processing is disabled.
        '''
        root = self._server
        if not root:
            root = self.load(cmdline)

        if 'debug_filename' in self:
            self.playbackRequestHistory(self['debug_filename'], out)

        if self.get('record_requests'):
            root.requestsRecord = []
            root.requestRecordPath = 'debug-vesper-requests.pkl'

        if not self.get('exec_cmd_and_exit', not startserver):
            port = self.get('port', 8000)
            middleware =  self.get('wsgi_middleware')
            httpserver = self.get('httpserver')
            log.info("Starting HTTP on port %d..." % port)
            #runs forever:
            root.runWsgiServer(port, httpserver, middleware)

        return root

def createStore(json='', **kw):
    for name, default in [('storage_url', 'mem://'), ('storage_template', json),
                      ('storage_template_options', dict(toplevelBnodes=False))]:
        if name not in kw:
            kw[name] = default
    root = createApp(**kw).run(False, False)
    return root.defaultStore

def _initConfigState():
    global _current_config, _current_configpath
    _current_config = AppConfig()
    _current_configpath = [None]        
_initConfigState()

def _normpath(basedir, path):
    """
    return an absolute path given a basedir and a path fragment.  If `path` is already absolute
    it will be returned unchanged.
    """
    if os.path.isabs(path):
        return path
    else:
        tmp = os.path.normpath(os.path.join(basedir, path))
        #assert os.path.isabs(tmp), 'not abs path %s, from %s + %s' % (tmp,basedir,path) 
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
        basemod = sys.modules.get(baseapp)
        if basemod:
            reload(basemod)
        else:
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
    #as an explicit function argument so they're not overwritten by this line:
    _current_config.update(config)
    
    if 'actions' in _current_config:
        if actions:
            _current_config.actions.update(actions)
    else:
        _current_config.actions = actions or {}
    
    basedir = _current_configpath[-1] or derived_path
    if basedir is not None:
        if not os.path.isdir(basedir):
            basedir = os.path.dirname(basedir)

        def addToPath(path, configvar):
            if isinstance(path, (str, unicode)):
                path = [path]
            path = list(path) + _current_config.get(configvar,[])
            path = [_normpath(basedir, x) for x in path]
            _current_config[configvar] = path
            for p in path:
                if not os.path.isdir(p):
                    raise VesperError('bad config variable "%s": '
                    '%s is not a valid directory' % (configvar, p))

        addToPath(static_path, 'static_path')
        addToPath(template_path, 'template_path')
        #set the 'mako_modules' directory default to be relative to directory of
        #the most derived app to prevent tmp directory spew in current directory
        mako_module_dir = config.get('mako_module_dir', 'mako_modules')
        _current_config.mako_module_dir = _normpath(basedir, mako_module_dir)
        
        #storage_template_path should be relative to the app config 
        #that sets it, not the final (most derived) app
        for configdict in itertools.chain([_current_config, config.get('storeDefaults')], 
                                        (config.get('stores') or {}).values()):
            if not configdict:
                continue
            storage_template_path = configdict.get('storage_template_path')
            if storage_template_path:
                abspath = _normpath(basedir, storage_template_path)
                configdict['storage_template_path'] = abspath

    return _current_config
