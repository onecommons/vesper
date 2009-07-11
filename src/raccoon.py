#! /usr/bin/env python
"""
    Engine and helper classes for Raccoon

    Copyright (c) 2003-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
if __name__ == '__main__':
    #we do this to avoid a lameness of Python where two Raccoon modules
    #can get loaded: <module '__main__' (built-in)>
    #and <module 'rx.Raccoon' from 'g:\_dev\rx4rdf\rx\raccoon.py'>
    #and so two different set of class definitions that won't be equal
    #which means for example an exception thrown by one module won't
    #get caught in code that imported the other module
    import raccoon
    import sys
    raccoon.main(sys.argv[1:])
else:
    from rx import utils, glock, RxPath, MRUCache, DomStore, transactions
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
    import logging

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
        return lambda resultNodeset, kw, contextNode, retVal: (
          actionid, cacheKeyPredicate(resultNodeset, kw, contextNode, retVal))
        
    def notCacheableKeyPredicate(*args, **kw):
        raise MRUCache.NotCacheable

    class ActionQuery(object):
        def evaluate(self, requestProcessor, kw, contextNode):
            return RxPath.XTrue
        
    class Action(object):
        '''
The Action class encapsulates a step in the request processing pipeline.

An Action has two parts, one or more match expressions and an action
function that is invoked if the request metadata matches one of the
match expressions. The action function returns a value which is passed
onto the next Action in the sequence.
        '''
                
        def __init__(self, queries=None, action=None, matchFirst=True,
                forEachNode = False, depthFirst=True, requiresContext=False,
                cachePredicate=notCacheableKeyPredicate,
                sideEffectsPredicate=None, sideEffectsFunc=None,
                isValueCacheableCalc=None,
                cachePredicateFactory=defaultActionCacheKeyPredicateFactory,
                canReceiveStreams=False, debug=False):
            '''Queries is a list of RxPath expressions associated with this action
action must be a function with this signature:    
def action(matchResult, kw, contextNode, retVal) where:
    result is the result of the action's matching RxPath query 
    kw is the dictionary of metadata associated with the request
    contentNode is the context node used when the RxPath expressions were evaluated
    retVal was the return value of the last action invoked in the in action sequence or None

If action is None this action will set the context node to the first node
in the nodeset returned by the matching expression

If matchFirst is True (the default) the requesthandler will stop after
the first matching query. Otherwise all the match expression be
evaluated and the action function call after each match.

If forEachNode is True then the action function will be called for
each node in a matching expression's result nodeset. The action
function's result parameter will be a nodeset contain only that
current node.
'''        
            if queries is None:
                queries = [ActionQuery()]            
            assert [q for q in queries if isinstance(q, ActionQuery)]
            self.queries = queries
            self.action = action
            self.matchFirst = matchFirst 
            self.forEachNode = forEachNode
            self.requiresContext = requiresContext
            self.depthFirst = depthFirst
            self.preVars = []
            self.postVars = []
            # self.requiresRetVal = requiresRetVal not yet implemented
            self.cacheKeyPredicate = cachePredicateFactory(self, cachePredicate)
            self.cachePredicateFactory = cachePredicateFactory
            self.sideEffectsPredicate = sideEffectsPredicate
            self.sideEffectsFunc = sideEffectsFunc
            self.isValueCacheableCalc = isValueCacheableCalc
            self.canReceiveStreams = canReceiveStreams
            self.debug = debug

        def __deepcopy__(self, memo):
            '''deepcopy() fails when trying to copy function pointers,
            so just deepcopy what we need here.'''
            #todo deal with cacheKeyPredicate and cachePredicateFactory
            import copy
            dup = copy.copy(self)
            dup.queries = copy.copy( self.queries )
            dup.preVars = copy.copy( self.preVars )
            dup.postVars = copy.copy( self.postVars )
            return dup
            
        def assign(self, varName, *exps, **kw):
            '''
Add a variable and expression list. Before the Action is run each
expression evaluated, the first one that returns a non-empty value is
assigned to the variable. Otherwise, the result of last expression is
assigned (so you can choose between '', [], and 0). If the
'assignEmpty' keyword argument is set to False the variable will only
be assigned if the result is non-zero (default is True). If the 'post'
keyword argument is set to True the variable will be assigned after
the Action is run (default is False).
            '''
            assert len(exps), 'Action.assign requires at least one expression'
            assignWhenEmpty = kw.get('assignEmpty', True)            

            if kw.get('post'):
                varlist=self.postVars
            else:
                varlist=self.preVars
            for i in xrange(len(varlist)):
                if varName == varlist[i][0]:#replace if names match
                    varlist[i] = (varName,  exps, assignWhenEmpty)
                    break
            else:
                varlist.append( (varName,  exps, assignWhenEmpty) )                    
    
    class SimpleAction(Action, ActionQuery):
        '''
        Override match and go
        '''

        def __init__(self):
            Action.__init__(self, [self], self.runAction)
            
        def match(self, kw):
            return True
            
        def go(self, kw):
            return None
    
        def evaluate(self, requestProcessor, kw, contextNode):
            return self.match(kw)# and RxPath.XTrue or RxPath.XFalse
            
        def runAction(self, result, kw, contextNode, retVal):
            return self.go(kw)    
                                                                     
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
        #DefaultDisabledContentProcessors = []        
                    
        #expCache = MRUCache.MRUCache(0, XPath.Compile, sideEffectsFunc = 
        #    lambda cacheValue,sideEffects,*args: setattr(cacheValue,'fromCache',True))    
                
        requestsRecord = None
        log = log        

        #keys found in the request metadata dictionary that
        #can't be mapped to XPath variables
        COMPLEX_REQUESTVARS = ['__requestor__', '__server__',
                                '_prevkw', '__argv__', '_errorInfo']
        
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
            if not os.path.exists(path):
                raise CmdArgError('%s not found' % path) 

            kw = globals().copy() #copy this modules namespace
            if path:
                if not self.BASE_MODEL_URI:
                    import socket            
                    self.BASE_MODEL_URI= 'http://' + socket.getfqdn() + '/'
                
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
            domStoreFactory = kw.get('domStoreFactory', DomStore.RxPathDomStore)
            self.domStore = domStoreFactory(**kw)                        
            self.domStore.addTrigger = self.txnSvc.addHook
            self.domStore.removeTrigger = self.txnSvc.removeHook
            self.domStore.newResourceTrigger = self.txnSvc.newResourceHook
            
            self.defaultRequestTrigger = kw.get('DEFAULT_TRIGGER','http-request')
            initConstants( ['globalRequestVars'], [])
            self.globalRequestVars.extend( ['_name', '_noErrorHandling',
                '__current-transaction','__store', '_APP_BASE', '__readOnly'] )
            self.defaultPageName = kw.get('defaultPageName', 'index')
            #cache settings:                
            initConstants( ['LIVE_ENVIRONMENT', 'useEtags'], 1)
            self.defaultExpiresIn = kw.get('defaultExpiresIn', 3600)
            initConstants( ['XPATH_CACHE_SIZE','ACTION_CACHE_SIZE'], 1000)
            initConstants( ['XPATH_PARSER_CACHE_SIZE',
                            'STYLESHEET_CACHE_SIZE'], 200)
            #disable by default(default cache size used to be 10000000 (~10mb))
            initConstants( ['maxCacheableStream','FILE_CACHE_SIZE'], 0)
                        
            #todo: these caches are global, only let the root RequestProcessor
            #set these values
            #fileCache.maxFileSize = kw.get('MAX_CACHEABLE_FILE_SIZE', 0)                        
            #self.expCache.capacity = self.XPATH_PARSER_CACHE_SIZE
            #fileCache.capacity = self.FILE_CACHE_SIZE
            #fileCache.hashValue = lambda path: getFileCacheKey(path,
            #                                   fileCache.maxFileSize)
                    
            self.PATH = kw.get('PATH', self.PATH)
            
            #self.disabledContentProcessors = kw.get('disabledContentProcessors',
            #                            self.DefaultDisabledContentProcessors)            
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
                #self.queryCache = MRUCache.MRUCache(self.XPATH_CACHE_SIZE,
                #        lambda compExpr, context: compExpr.evaluate(context),
                #        lambda compExpr, context: getKeyFromXPathExp(compExpr,
                #                        context, self.NOT_CACHEABLE_FUNCTIONS),
                #        processXPathExpSideEffects, calcXPathSideEffects)
                #self.queryCache.debug = self.log.info
                self.actionCache = MRUCache.MRUCache(self.ACTION_CACHE_SIZE,
                                                     digestKey=True)
                
                self.domStore.loadDom(self)
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
            if kw is None: kw = {}            
            sequence = self.actions.get(triggerName)
            if sequence:
                errorSequence = self.actions.get(triggerName+'-error')
                return self.doActions(sequence, kw, retVal=initVal,
                    errorSequence=errorSequence, newTransaction=newTransaction)
                            
        STOP_VALUE = u'2334555393434302' #hack
        
        def __assign(self, actionvars, kw, contextNode, debug=False):
            #context = XPath.Context.Context(None, processorNss = self.nsMap)
            for name, exps, assignEmpty in actionvars:
                assert False, 'TODO!' #XXX
                vars, extFunMap = self.mapToXPathVars(kw)            
                for exp in exps:
                    result = self.evalXPath(exp, vars=vars,
                                extFunctionMap=extFunMap, node=contextNode)                    
                    if result:
                        break
                if debug:
                    print name, exp; print result
                if assignEmpty or result:
                    AssignMetaData(kw, context, name, result, authorize=False)
                    #print name, result, contextNode

        def _doActionsBare(self, sequence, kw, contextNode, retVal):
            result = None
            try:
                for action in sequence:            
                    if action.requiresContext:
                        #if the next action requires a contextnode
                        #and there isn't one, end the sequence
                        if not contextNode: 
                            return retVal

                    self.__assign(action.preVars, kw, contextNode, action.debug)
                                        
                    for xpath in action.queries:
                        result = xpath.evaluate(self, kw, contextNode)
                        if result is self.STOP_VALUE:#for $STOP
                            break
                        if result: #todo: if result != []:
                            #if not equal empty nodeset (empty strings ok)
                            
                            #if no action function is defined
                            #the action resets the contextNode instead
                            if not action.action: 
                                if type(result) == list:
                                    #only set context if result is a nonempty nodeset
                                    contextNode = result[0]
                                    kw['__context'] = [ contextNode ]
                                    self.log.debug('context changed: %s',result)
                                    #why would you want to evaluate
                                    #every query in this case?
                                    assert action.matchFirst 
                                break
                            else:
                                if (not action.canReceiveStreams
                                        and hasattr(retVal, 'read')):
                                    retVal = retVal.read()
                                if action.forEachNode:
                                    assert type(result) == list, (
                                        'result must be a nodeset')
                                    if action.depthFirst:
                                        #we probably want the reverse of
                                        #document order
                                        #(e.g. the deepest first)
                                        #copy the list since it might have
                                        #been cached
                                        result = result[:] 
                                        result.reverse()
                                    for node in result:
                                        if kw.get('_metadatachanges'):
                                            del kw['_metadatachanges']
                                        retVal = self.actionCache.getOrCalcValue(
                action.action, [node], kw, contextNode, retVal,
                hashCalc=action.cacheKeyPredicate,
                sideEffectsCalc=action.sideEffectsPredicate,
                sideEffectsFunc=action.sideEffectsFunc,
                isValueCacheableCalc=action.isValueCacheableCalc)
                                else:
                                    if kw.get('_metadatachanges'):
                                        del kw['_metadatachanges']
                                        
                                    retVal = self.actionCache.getOrCalcValue(
                action.action, result, kw, contextNode, retVal,
                hashCalc=action.cacheKeyPredicate,
                sideEffectsCalc=action.sideEffectsPredicate,
                sideEffectsFunc=action.sideEffectsFunc,
                isValueCacheableCalc=action.isValueCacheableCalc)
                            if action.matchFirst:
                                break

                    self.__assign(action.postVars,kw,contextNode,action.debug)
            except:
                exc = ActionWrapperException()
                exc.state = (result, contextNode, retVal)
                raise exc
            return result, contextNode, retVal

        def _doActionsTxn(self, sequence, kw, contextNode, retVal):            
            self.txnSvc.begin()
            self.txnSvc.state.kw = kw
            txnCtxtResult = self.domStore.getTransactionContext()
            self.txnSvc.state.kw['__current-transaction'] = txnCtxtResult or []

            self.txnSvc.state.contextNode = contextNode
            self.txnSvc.state.retVal = retVal
            try:
                result, contextNode, retVal = self._doActionsBare(
                                 sequence, kw, contextNode, retVal)
            except:
                if self.txnSvc.isActive(): #else its already been aborted
                    self.txnSvc.abort()
                raise
            else:
                if self.txnSvc.isActive(): #could have already been aborted                    
                    self.txnSvc.addInfo(source=self.getPrincipleFunc(kw))

                    self.txnSvc.state.result = result
                    self.txnSvc.state.contextNode = contextNode
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
                
        def doActions(self, sequence, kw=None, contextNode=None,retVal=None,
                      errorSequence=None, newTransaction=False):
            if kw is None: kw = {}
            result = None
            #todo: reexamine this logic
            if isinstance(contextNode, type([])) and contextNode:
                contextNode = contextNode[0]        

            kw['__requestor__'] = self.requestDispatcher
            kw['__server__'] = self
            kw['__context'] = [ contextNode ]  

            try:
                if newTransaction:
                    retVal = self._doActionsTxn(sequence, kw,
                                                contextNode, retVal)
                else:                    
                    if '__current-transaction' not in kw:
                        txnCtxtResult = self.domStore.getTransactionContext()
                        kw['__current-transaction'] = txnCtxtResult or []
                    result, contextNode, retVal = self._doActionsBare(
                                    sequence, kw, contextNode, retVal)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                #print newTransaction, self.txnSvc.state.timestamp
                exc_info = sys.exc_info()
                if isinstance(exc_info[1], ActionWrapperException):
                    result, contextNode, retVal = exc_info[1].state
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
                            return locals()

                        kw['_errorInfo'] = getErrorKWs()
                        self.log.warning("invoking error handler on exception:\n"+
                                         kw['_errorInfo']['details'])
                        #todo: provide a protocol for mapping exceptions attributes to
                        #XPath variables, e.g. line # for parsing errors
                        #kw['errorInfo'].update(self.extractXPathVars(
                        #                  kw['errorInfo']['value']) )
                        try:
                            return self.callActions(errorSequence, result, kw,
                                    contextNode, retVal, self.globalRequestVars,
                            #if we're creating a new transaction,
                            #it has been aborted by now, so start a new one:
                                                newTransaction=newTransaction)
                        finally:
                            del kw['_errorInfo']
                    else:
                        #traceback.print_exception(*exc_info)
                        raise exc_info[1] or exc_info[0], None, exc_info[2]
                finally:
                    self.inErrorHandler -= 1
            return retVal

        def callActions(self, actions,resultNodeset, kw, contextNode, retVal,
                    globalVars=None, errorSequence=None, newTransaction=False):
            '''
            process another set of actions using the current context as input,
            but without modified the current context.
            Particularly useful for template processing.
            '''
            #merge previous prevkw, overriding vars as necessary
            prevkw = kw.get('_prevkw', {}).copy() 
            templatekw = {}
            
            globalVars = globalVars or []
            #globalVars are variables that should be present throughout the
            #whole request so copy them into templatekw instead of _prevkw
            globalVars = self.COMPLEX_REQUESTVARS + globalVars

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
            if hasattr(retVal, 'read'):
                #todo: delay doing this until $_contents is required
                retVal = retVal.read()            
            templatekw['_contents'] = retVal
            
            #nodeset containing current resource
            templatekw['_previousContext']=contextNode and [contextNode] or []
            templatekw['_originalContext']=kw.get('_originalContext',
                                                templatekw['_previousContext'])
            #use the resultNodeset as the contextNode for call action
            if isinstance(resultNodeset, list): 
                contextNode = resultNodeset
            else:
                contextNode = None
            return self.doActions(actions, templatekw, contextNode,
                errorSequence=errorSequence, newTransaction=newTransaction)             
                                
    class HTTPRequestProcessor(RequestProcessor):
        '''
        Adds functionality for handling an HTTP request.
        '''                

        def __init__(self,*args, **kwargs):
            self.COMPLEX_REQUESTVARS += ['_request','_response', '_session']            

            super(HTTPRequestProcessor,self).__init__(*args, **kwargs)                

        def handleHTTPRequest(self, name, kw):
            if self.requestsRecord is not None:
                self.requestsRecord.append([ name, kw ])

            request = kw['_request']
            
            #cgi: REQUEST_METHOD [http[s]://] HTTP_HOST : SERVER_PORT REQUEST_URI ? QUERY_STRING            
            #cherrypy:
            #request.base = http[s]:// + request.headerMap['host'] + [:port]
            #request.browserUrl = request.base + / + [request uri]
            #request.path = [path w/no leading or trailing slash]
            #name = urllib.unquote(request.path) or _usevdomain logic

            request.browserBase = request.base + '/'
            
            #the operative part of the url
            #(browserUrl = browserBase + browserPath + '?' + browserQuery)
            path = request.browserUrl[len(request.browserBase):]
            if path.find('?') > -1:
                request.browserPath, request.browserQuery = path.split('?', 1)
            else:
                request.browserPath, request.browserQuery = path, ''

            appBaseLength = len(self.appBase)
            if appBaseLength > 1: #appBase always starts with /
                name = name[appBaseLength:] 
            if not name or name == '/':
                name = self.defaultPageName
            kw['_name'] = name
            #import pprint; pprint.pprint(kw)

            #if the request name has an extension try to set
            #a default mimetype before executing the request
            i=name.rfind('.')
            if i!=-1:
                ext=name[i:]      
                contentType=mimetypes.types_map.get(ext)
                if contentType:
                    kw['_response'].headerMap['content-type']=contentType

            try:                
                rc = {}
                #requestContext is used by all Requestor objects
                #in the current thread 
                rc['_session']=kw['_session']
                rc['_request']=kw['_request']
                self.requestContext.append(rc)
                
                self.validateExternalRequest(kw)
                result = self.runActions('http-request', kw)
                if result is not None: #'cause '' is OK
                    if hasattr(result, 'read'):
                        #we don't yet support streams
                        result = result.read()
                    else:
                        #an http request must return a string
                        #(not unicode, for example)
                        result = str(result) 

                    if (self.defaultExpiresIn and
                        'expires' not in kw['_response'].headerMap):
                        if self.defaultExpiresIn == -1:
                            expires = '-1'
                        else:                             
                            expires = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                            time.gmtime(time.time() + self.defaultExpiresIn))
                        kw['_response'].headerMap['expires'] = expires
                    
                    #if mimetype is not set, make another attempt
                    if not kw['_response'].headerMap.get('content-type'):
                        contentType = self.guessMimeTypeFromContent(result)
                        if contentType:
                            kw['_response'].headerMap['content-type']=contentType
                    #todo:
                    #if isinstance(kw['_response'].headerMap.get('status'), (float, int)):
                    #    kw['_response'].headerMap['status'] = self.stdStatusMessages[
                    #                              kw['_response'].headerMap['status']]
                    if self.useEtags:
                        resultHash = kw['_response'].headerMap.get('etag')
                        #if the app already set the etag use that value instead
                        if resultHash is None:
                            import md5                                
                            resultHash = '"' + md5.new(result).hexdigest() + '"'
                            kw['_response'].headerMap['etag'] = resultHash
                        etags = kw['_request'].headerMap.get('if-none-match')
                        if etags and resultHash in [x.strip() for x in etags.split(',')]:
                            kw['_response'].headerMap['status'] = "304 Not Modified"
                            return ''

                    #todo: we don't support if-modified-since header but we set this so
                    #pages can check if they've load from the browser cache
                    #or not using document.lastModified javascript
                    #if self.setLastModified:
                    #   kw['_response'].headerMap['last-modified'] = time.strftime(
                    #        "%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time()))
                    return result
            finally:
                self.requestContext.pop()
                    
            return self.default_not_found(kw)

        def guessMimeTypeFromContent(self, result):
            #obviously this could be improved,
            #e.g. detect encoding in xml header or html meta tag
            #or handle the BOM mark in front of the <?xml 
            #detect binary vs. text, etc.
            test = result[:30].strip().lower()
            if test.startswith("<html") or result.startswith("<!doctype html"):
                return "text/html"
            elif test.startswith("<?xml") or test[2:].startswith("<?xml"): 
                return "text/xml"
            elif self.DEFAULT_MIME_TYPE:
                return self.DEFAULT_MIME_TYPE
            else:
                return None

        def default_not_found(self, kw):            
            kw['_response'].headerMap["content-type"]="text/html"
            kw['_response'].headerMap['status'] = "404 Not Found"
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

    DEFAULT_cmd_usage = 'python raccoon.py -l [log.config] -r -d [debug.pkl] -x -s server.cfg -p path -m store.nt -a config.py '
    cmd_usage = '''
    -h this help message
    -s server.cfg specify an alternative server.cfg
    -l [log.config] specify a config file for logging
    -r record requests (ctrl-c to stop recording) 
    -d [debug.pkl]: debug mode (replay the requests saved in debug.pkl)
    -x exit after executing config specific cmd arguments
    -p specify the path (overrides RACCOONPATH env. variable)
    -m [store.nt] load the RDF model
       (default model supports .rdf, .nt, .mk)
    -a config.py run the application specified
    '''

    DEFAULT_LOGLEVEL = logging.INFO

    def main(argv, out=sys.stdout):
        root = None
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
                if sys.version_info < (2, 3):
                    import logging22.config as log_config
                else:
                    import logging.config as log_config
                if not os.path.exists(logConfig):
                    raise CmdArgError("%s not found" % logConfig)
                log_config.fileConfig(logConfig)
                #any logger already created and not explicitly
                #specified in the log config file is disabled this
                #seems like a bad design -- certainly took me a while
                #to why understand things weren't getting logged so
                #re-enable the loggers
                for logger in logging.Logger.manager.loggerDict.itervalues():
                    logger.disabled = 0        
            else: #set defaults        
                logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
                logging.root.setLevel(DEFAULT_LOGLEVEL)
                logging.basicConfig()

            kw = argsToKw(rootArgs, DEFAULT_cmd_usage)
            kw['argsForConfig'] = configArgs        
            #print 'ma', mainArgs
            if '-h' in mainArgs or '--help' in mainArgs:
                raise CmdArgError('')
            try:
                root = HTTPRequestProcessor(appName='root', **kw)
            except (TypeError), e:               
                index = str(e).find('argument') #bad keyword arguement
                if index > -1:                    
                    raise CmdArgError('invalid ' +str(e)[index:])
                else:
                    raise
            if '-d' in mainArgs: 
                try:
                    debugFileName=mainArgs[mainArgs.index("-d")+1]
                    if debugFileName[0] == '-':
                        raise ValueError
                except (IndexError, ValueError):
                    debugFileName = 'debug-wiki.pkl'
                flags = sys.version_info[:2] < (2, 3) and 'b' or 'U'
                requests = pickle.load(file(debugFileName, 'r'+flags))
                
                import repr
                rpr = repr.Repr()
                rpr.maxdict = 20 
                rpr.maxlevel = 2
                for i, request in enumerate(requests):
                    verb = getattr( request[1].get('_request'),'method', 'request')
                    login = request[1].get('_session',{}).get('login','')
                    print>>out, i, verb, request[0], 'login:', login
                    #print form variables
                    print>>out, rpr.repr(dict([(k, v) for k, v in request[1].items()
                                        if isinstance(v, (unicode, str, list))]))

                    root.handleHTTPRequest(request[0], request[1])
            elif '-x' not in mainArgs:
                #if -x (execute cmdline and exit) we're done
                sys.argv = mainArgs #hack for Server
                from rx import Server
                if '-r' in mainArgs:
                    root.requestsRecord = []                    
                    root.requestRecordPath = 'debug-wiki.pkl' 
                #print 'starting server!'
                Server.start_server(root) #kicks off the whole process
                #print 'dying!'
        except (CmdArgError), e:
            print>>out, e
            if root:
                cmd_line = root.cmd_usage
            else:
                cmd_line = DEFAULT_cmd_usage +'[config specific options]'
            print>>out, 'usage:'
            print>>out, cmd_line
            print>>out, cmd_usage

            
