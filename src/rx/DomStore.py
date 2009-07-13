"""
    DOMStore classes used by Raccoon.

    Copyright (c) 2004-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
from rx import RxPath, transactions
import StringIO, os, os.path
import logging

class DomStore(transactions.TransactionParticipant):
    '''
    Abstract interface for DomStores
    '''
    log = logging.getLogger("domstore")

    #impl. must expose the DOM as a read-only attribute named "dom"
    dom = None

    addTrigger = None
    removeTrigger = None
    newResourceTrigger = None

    def __init__(**kw):
        pass
    
    def loadDom(self,requestProcessor, location, defaultDOM):
        ''' 
        Load the DOM located at location (a filepath).
        If location does not exist create a new DOM that is a copy of 
        defaultDOM, a file-like of appropriate type
        (e.g. an XML or RDF NTriples file).
        '''
        self.log = logging.getLogger("domstore." + requestProcessor.appName)
                        
    def commitTransaction(self, txnService):
        pass

    def abortTransaction(self, txnService):
        pass

    def getStateKey(self):
        '''
        Returns the a hashable object that uniquely identifies the current state of DOM.
        Used for caching.
        If this is not implemented, it should raise KeyError (the default implementation).
        '''
        raise KeyError

    def getTransactionContext(self):
        return None
        
    def _normalizeSource(self, requestProcessor, path):
        #if source was set on command line, override config source
        if requestProcessor.source:            
            source = requestProcessor.source
        else:
            source = path

        if not source:
            self.log.warning('no model path given and STORAGE_PATH'
                             ' is not set -- model is read-only.')            
        elif not os.path.isabs(source):
            #XXX its possible for source to not be file path
            #     -- this will break that
            source = os.path.join( requestProcessor.baseDir, source)
        return source
            
class BasicDomStore(DomStore):

    def __init__(self, modelFactory=RxPath.IncrementalNTriplesFileModel,
                 schemaFactory=RxPath.defaultSchemaClass,                 
                 STORAGE_PATH ='',
                 STORAGE_TEMPLATE='',
                 APPLICATION_MODEL='',
                 transactionLog = '',
                 saveHistory = False,
                 VERSION_STORAGE_PATH='',
                 versionModelFactory=None, **kw):
        '''
        modelFactory is a RxPath.Model class or factory function that takes
        two parameters:
          a location (usually a local file path) and iterator of Statements
          to initialize the model if it needs to be created
        '''
        self.modelFactory = modelFactory
        self.versionModelFactory = versionModelFactory or modelFactory
        self.schemaFactory = schemaFactory 
        self.APPLICATION_MODEL = APPLICATION_MODEL        
        self.STORAGE_PATH = STORAGE_PATH        
        self.VERSION_STORAGE_PATH = VERSION_STORAGE_PATH
        self.STORAGE_TEMPLATE = STORAGE_TEMPLATE
        self.transactionLog = transactionLog
        self.saveHistory = saveHistory
            
    def loadDom(self, requestProcessor):        
        self.log = logging.getLogger("domstore." + requestProcessor.appName)

        normalizeSource = getattr(self.modelFactory, 'normalizeSource',
                                                DomStore._normalizeSource)
        #source is the data source for the store, usually a file path
        source = normalizeSource(self, requestProcessor, self.STORAGE_PATH)
        
        model, defaultStmts, historyModel, lastScope = self.setupHistory(
                                                    requestProcessor, source)
        if not model:
            #setupHistory didn't initialize the store, so do it now
            #modelFactory will load the store specified by `source` or create
            #new one at that location and initializing it with `defaultStmts`
            model = self.modelFactory(source=source, 
                                            defaultStatements=defaultStmts)

        #if there's application data (data tied to the current revision
        #of your app's implementation) include that in the model
        if self.APPLICATION_MODEL:
            from rx.RxPathGraph import APPCTX #'context:application:'
            stmtGen = RxPath.parseRDFFromString(self.APPLICATION_MODEL, 
                requestProcessor.MODEL_RESOURCE_URI, scope=APPCTX) 
                                        
            appmodel = RxPath.MemModel(stmtGen)
            #XXX MultiModel is not very scalable -- better would be to store 
            #the application data in the model and update it if its difference 
            #from what's stored (of course this requires a context-aware store)
            model = RxPath.MultiModel(model, appmodel)
        
        #turn on update logging if a log file is specified, which can be used to 
        #re-create the change history of the store
        if self.transactionLog:
            model = RxPath.MirrorModel(model, RxPath.IncrementalNTriplesFileModel(
                self.transactionLog, []) )

        self.model = model

        if self.saveHistory:
            self.graphManager = RxPathGraph.NamedGraphManager(model, historyModel,lastScope)
        else:
            self.graphManager = None
        
        #set the schema (default is no-op)
        self.schema = self.schemaFactory(model)
        #XXX need for graphManager?:
        #self.schema.setEntailmentTriggers(self._entailmentAdd, self._entailmentRemove)
        if isinstance(self.schema, RxPath.Model):
            self.model = self.schema

    def setupHistory(self, requestProcessor, source):
        if self.saveHistory:
            #if we're going to be recording history we need a starting context uri
            from rx import RxPathGraph
            initCtxUri = RxPathGraph.getTxnContextUri(requestProcessor.MODEL_RESOURCE_URI, 0)
        else:
            initCtxUri = ''
        
        #data used to initialize a new store
        defaultStmts = RxPath.parseRDFFromString(self.APPLICATION_MODEL, 
                        requestProcessor.MODEL_RESOURCE_URI, scope=initCtxUri) 
                
        #if we're using a separate store to hold the change history, load it now
        #(it's called delmodel because it only stores removals as the history 
        #is recorded soley as adds and removes)
        if self.VERSION_STORAGE_PATH:
            normalizeSource = getattr(self.versionModelFactory, 
                    'normalizeSource', DomStore._normalizeSource)
            versionStoreSource = normalizeSource(self, requestProcessor,
                                                 self.VERSION_STORAGE_PATH)
            delmodel = self.versionModelFactory(source=versionStoreSource,
                                                defaultStatements=[])
        else:
            delmodel = None

        #open or create the model (the datastore)
        #if savehistory is on and we are loading a store that has the entire 
        #change history (e.g. we're loading the transaction log) we also load 
        #the history into a separate model
        #
        #note: to override loadNtriplesIncrementally, set this attribute
        #on your custom modelFactory
        if self.saveHistory and getattr(
                self.modelFactory, 'loadNtriplesIncrementally', False):
            if not delmodel:
                delmodel = RxPath.MemModel()
            dmc = RxPathGraph.DeletionModelCreator(delmodel)            
            model = self.modelFactory(source=source,
                    defaultStatements=defaultStmts, incrementHook=dmc)
            lastScope = dmc.lastScope        
        else:
            model = None
            lastScope = None
            
        return model, defaultStmts, delmodel, lastScope

    def isDirty(self, txnService):
        '''return True if this transaction participant was modified'''    
        return txnService.state.additions or txnService.state.removals
        
    def commitTransaction(self, txnService):
        if self.graphManager:
            self.graphManager.commit(txnService.getInfo())
        else:
            self.model.commit(**txnService.getInfo())

    def abortTransaction(self, txnService):        
        if not self.isDirty(txnService):
            return

        #from rx import MRUCache
        #key = self.dom.getKey()

        self.model.rollback()
        if self.graphManager:
            self.graphManager.rollback()        
                
        #if isinstance(key, MRUCache.InvalidationKey):
        #    if txnService.server.actionCache:
        #        txnService.server.actionCache.invalidate(key)

    def getTransactionContext(self):
        if self.graphManager:
            return self.dom.graphManager.getTxnContext() #return a contextUri
        return None

    def update(statements):    
        resources = set()
        for s in statements:
            if self.newResourceTrigger:
                subject = s[0]
                if subject not in resources:
                    resource.update(subject)
                    if not model.findUri(subject): #XXX
                        self.newResourceTrigger(subject)

            try:
                if self.graphManager:
                    self.graphManager.addTrigger(s)
                elif self.addTrigger:
                    self.addTrigger(s)
                if self.graphManager:
                    self.graphManager.add(s)
                else:
                    self.model.addStatement(s)
            except IndexError:
                #thrown by _orderedInsert: statement already exists in the model
                log.debug('statement %s already exists for %s' % (typeName, uri))
            
    def remove(statements):    
        for s in statements:
            if self.removeTrigger:
                self.removeTrigger(s)

            if self.ownerDocument.graphManager:
                self.ownerDocument.graphManager.remove(s)
            else:
                self.ownerDocument.model.removeStatement(s)
