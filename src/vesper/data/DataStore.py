"""
    vesper.data.DataStore
    =====================
    
    This module defined the high-level public interface to a data store.
"""
import StringIO, os, os.path
import logging

from vesper.data import base, transactions
from vesper.data.base import graph as graphmod # avoid aliasing some local vars
from vesper.data.store.basic import MemStore, FileStore, IncrementalNTriplesFileStoreBase
from vesper.utils import debugp, flatten
from vesper.data.base.utils import OrderedModel
from vesper.data.base.schema import defaultSchemaClass

from vesper import pjson

def _toStatements(contents, **kw):
    if not contents:
        return [], None
    if isinstance(contents, (list, tuple)):
        if isinstance(contents[0], (tuple, base.BaseStatement)):
            return contents, None #looks like a list of statements
    #assume pjson:
    return pjson.tostatements(contents, setBNodeOnObj=True, **kw), contents

class DataStore(transactions.TransactionParticipant): # XXX this base class can go away
    '''
    Abstract interface for DataStores
    '''
    log = logging.getLogger("datastore")

    addTrigger = None
    removeTrigger = None
    newResourceTrigger = None

    def __init__(requestProcessor, **kw):
        pass
    
    def load(self):
        self.log = logging.getLogger("datastore." + requestProcessor.appName)
                        
    def commitTransaction(self, txnService):
        pass

    def abortTransaction(self, txnService):
        pass

    def getStateKey(self):
        '''
        Returns the a hashable object that uniquely identifies the current state of this datastore.
        Used for caching.
        If this is not implemented, it should raise KeyError (the default implementation).
        '''
        raise KeyError

    def getTransactionContext(self):
        return None
        
    def _normalizeSource(self, requestProcessor, source):
        if not source:
            self.log.warning('no model path given and STORAGE_PATH'
                             ' is not set -- model is read-only.')            
        elif not os.path.isabs(source):
            #XXX its possible for source to not be file path
            #     -- this will break that
            source = os.path.join( requestProcessor.baseDir, source)
        return source
            
class BasicStore(DataStore):

    def __init__(self, requestProcessor, modelFactory=None,
                 schemaFactory=defaultSchemaClass,
                 STORAGE_PATH ='',
                 STORAGE_TEMPLATE='',
                 APPLICATION_MODEL='',
                 transactionLog = '',
                 saveHistory = False,
                 VERSION_STORAGE_PATH='',
                 versionModelFactory=None,
                 storageTemplateOptions=None,
                 modelOptions=None,
                 CHANGESET_HOOK=None,
                 trunkId = '0A', 
                 branchId = None,                  
                 **kw):
        '''
        modelFactory is a base.Model class or factory function that takes
        two parameters:
          a location (usually a local file path) and iterator of Statements
          to initialize the model if it needs to be created
        '''
        self.requestProcessor = requestProcessor
        self.modelFactory = modelFactory
        self.versionModelFactory = versionModelFactory
        self.schemaFactory = schemaFactory 
        self.APPLICATION_MODEL = APPLICATION_MODEL        
        self.STORAGE_PATH = STORAGE_PATH        
        self.VERSION_STORAGE_PATH = VERSION_STORAGE_PATH
        self.STORAGE_TEMPLATE = STORAGE_TEMPLATE
        self.transactionLog = transactionLog
        self.saveHistory = saveHistory
        self.storageTemplateOptions = storageTemplateOptions or {}
        self.trunkId = trunkId
        self.branchId = branchId
        self.changesetHook = CHANGESET_HOOK
        self._txnparticipants = []
        self.modelOptions = modelOptions or {}

    def load(self):
        requestProcessor = self.requestProcessor
        self.log = logging.getLogger("datastore." + requestProcessor.appName)

        #normalizeSource = getattr(self.modelFactory, 'normalizeSource',
        #                                        DataStore._normalizeSource)
        #source is the data source for the store, usually a file path
        #source = normalizeSource(self, requestProcessor, self.STORAGE_PATH)
        source = self.STORAGE_PATH
        model, defaultStmts, historyModel, lastScope = self.setupHistory(source)
        if not model:
            #setupHistory didn't initialize the store, so do it now
            #modelFactory will load the store specified by `source` or create
            #new one at that location and initializing it with `defaultStmts`
            modelFactory = self.modelFactory or FileStore
            model = modelFactory(source=source, defaultStatements=defaultStmts,
                                                            **self.modelOptions)
                
        #if there's application data (data tied to the current revision
        #of your app's implementation) include that in the model
        if self.APPLICATION_MODEL:
            stmtGen = base.parseRDFFromString(self.APPLICATION_MODEL, 
                requestProcessor.MODEL_RESOURCE_URI, scope=graphmod.APPCTX) 
            appmodel = MemStore(stmtGen)
            #XXX MultiModel is not very scalable -- better would be to store 
            #the application data in the model and update it if its difference 
            #from what's stored (this requires a context-aware store)
            model = base.MultiModel(model, appmodel)
        
        if self.saveHistory:
            model, historyModel = self._addModelTxnParticipants(model, historyModel)
            self.model = self.graphManager = graphmod.MergeableGraphManager(model, 
                historyModel, requestProcessor.MODEL_RESOURCE_URI, lastScope, self.trunkId, self.branchId) 
            if self._txnparticipants:
                self._txnparticipants.insert(0, #must go first
                        TwoPhaseTxnGraphManagerAdapter(self.graphManager))
        else:
            self.model = self._addModelTxnParticipants(model)[0]
            self.graphManager = None

        #turn on update logging if a log file is specified, which can be used to 
        #re-create the change history of the store
        if self.transactionLog:
            #XXX doesn't log history model if store is split
            #XXX doesn't log models that are TransactionParticipants themselves
            #XXX doesn't log models that don't support updateAdvisory 
            if isinstance(self.model, ModelWrapper):
                self.model.adapter.logPath = self.transactionLog
            else:                
                self.log.warning("transactionLog is configured but not compatible with model")
        
        if self.changesetHook:
            assert self.saveHistory, "replication requires saveHistory to be on"
            self.model.notifyChangeset = self.changesetHook
        
        #set the schema (default is no-op)
        self.schema = self.schemaFactory(self.model)
        #XXX need for graphManager?:
        #self.schema.setEntailmentTriggers(self._entailmentAdd, self._entailmentRemove)
        if isinstance(self.schema, base.Model):
            self.model = self.schema

        #hack!:
        if self.storageTemplateOptions.get('generateBnode') == 'counter':            
            model.bnodePrefix = '_:'
            self.model.bnodePrefix = '_:'

    def setupHistory(self, source):
        requestProcessor = self.requestProcessor
        if self.saveHistory:
            #if we're going to be recording history we need a starting context uri
            initCtxUri = graphmod.getTxnContextUri(requestProcessor.MODEL_RESOURCE_URI, 0)
        else:
            initCtxUri = ''
        
        #data used to initialize a new store
        defaultStmts = base.parseRDFFromString(self.STORAGE_TEMPLATE, 
                        requestProcessor.MODEL_RESOURCE_URI, scope=initCtxUri, 
                        options=self.storageTemplateOptions) 
        
        if self.saveHistory == 'split':                                                
            versionModelFactory = self.versionModelFactory
            versionModelOptions = {}
            if not versionModelFactory:
                if self.VERSION_STORAGE_PATH and self.modelFactory:
                    #if a both a version path and the modelfactory was set, use the model factory
                    versionModelFactory = self.modelFactory
                    versionModelOptions = self.modelOptions
                elif self.STORAGE_PATH or self.VERSION_STORAGE_PATH: #use the default
                    versionModelFactory = IncrementalNTriplesFileStoreBase
                else:
                    versionModelFactory = MemStore

            if not self.VERSION_STORAGE_PATH and issubclass(versionModelFactory, FileStore):
                #generate path based on primary path
                assert self.STORAGE_PATH
                import os.path
                versionStoreSource = os.path.splitext(self.STORAGE_PATH)[0] + '-history.nt'
            else:
                versionStoreSource = self.VERSION_STORAGE_PATH
            
            if versionStoreSource:
                normalizeSource = getattr(self.versionModelFactory, 
                        'normalizeSource', DataStore._normalizeSource)
                versionStoreSource = normalizeSource(self, requestProcessor,
                                                     versionStoreSource)
            revisionModel = versionModelFactory(source=versionStoreSource,
                                        defaultStatements=[], **versionModelOptions)
        else:
            #either no history or no separate model
            revisionModel = None
        
        #WARNING this feature hasn't been tested in a long time and is probably broken
        #XXX: write unittest or remove this code
        #if savehistory is on and we are loading a store that has the entire 
        #change history (e.g. we're loading the transaction log) we also load 
        #the history into a separate model
        #
        #note: to override loadNtriplesIncrementally, set this attribute
        #on your custom modelFactory        
        if self.saveHistory == 'split' and getattr(
                self.modelFactory, 'loadNtriplesIncrementally', False):
            if not revisionModel:
                revisionModel = MemStore()
            dmc = graphmod.DeletionModelCreator(revisionModel)
            model = self.modelFactory(source=source,
                    defaultStatements=defaultStmts, incrementHook=dmc)
            lastScope = dmc.lastScope
        else:
            model = None
            lastScope = None
        
        return model, defaultStmts, revisionModel, lastScope

    def isDirty(self, txnService):
        '''return True if this transaction participant was modified'''    
        return txnService.state.additions or txnService.state.removals
    
    def is2PhaseTxn(self):
        return not not self._txnparticipants

    def commitTransaction(self, txnService):
        if self.is2PhaseTxn():
            return
        self.model.commit(**txnService.getInfo())

    def abortTransaction(self, txnService):
        if self.is2PhaseTxn():
            return 
        
        if not self.isDirty(txnService):
            if self.graphManager:
               self.graphManager.rollback() 
            return
        self.model.rollback()

    def getTransactionContext(self):
        if self.graphManager:
            return self.graphManager.getTxnContext() #return a contextUri
        return None
    
    def join(self, txnService, setCurrentTxn=True):
        if not txnService.isActive():
            return False
        if setCurrentTxn and hasattr(txnService.state, 'kw'):
            txnCtxtResult = self.getTransactionContext()
            txnService.state.kw['__current-transaction'] = txnCtxtResult
        
        super(BasicStore,self).join(txnService)
        
        for participant in self._txnparticipants:
            participant.join(txnService)
        return True

    def _addModelTxnParticipants(self, model, historyModel=None):
        if historyModel:
            txnOk = model.updateAdvisory or isinstance(model, 
                                           transactions.TransactionParticipant)
            if txnOk:
                txnOk = historyModel.updateAdvisory or isinstance(historyModel,
                                           transactions.TransactionParticipant)
        else:
            txnOk = model.updateAdvisory or isinstance(model, 
                                           transactions.TransactionParticipant)
        
        if txnOk:
            def add(model):
                if not model:
                    return model
                if not isinstance(model, transactions.TransactionParticipant):
                    participant = TwoPhaseTxnModelAdapter(model)
                    model = ModelWrapper(model, participant)
                else:
                    participant = model
                self._txnparticipants.append(participant)
                return model
            
            return [add(m) for m in model, historyModel]
        else:
            self.log.warning(
"model(s) doesn't support updateAdvisory, unable to participate in 2phase commit")
            return model, historyModel
    
    def _toStatements(self, json):
        #if we parse pjson, make sure we generate the same kind of bnodes as the model
        return _toStatements(json, generateBnode=self.storageTemplateOptions.get('generateBnode'))
        
    def add(self, adds):
        '''
        Adds data to the store.

        `adds`: A list of either statements or pjson conforming dicts
        '''
        if not self.join(self.requestProcessor.txnSvc):
            #not in a transaction, so call this inside one
            func = lambda: self.add(adds)
            return self.requestProcessor.executeTransaction(func)
        
        stmts, jsonrep = self._toStatements(adds)
        resources = set()
        newresources = []
        for s in stmts:
            if self.newResourceTrigger:
                subject = s[0]
                if subject not in resources:
                    resource.update(subject)
                    if not self.model.filter(subject=subject, hints=dict(limit=1)): 
                        newresources.append(subject)
        
        if self.newResourceTrigger and newresources:  
            self.newResourceTrigger(newresources)
        if self.addTrigger and stmts:
            self.addTrigger(stmts, jsonrep)
        
        self.model.addStatements(stmts)
        return jsonrep or stmts

    def _removePropLists(self, stmts):
        if not self.model.canHandleStatementWithOrder:
            for stmt in stmts:
                #check if the statement is part of a json list, remove that list item too
                rows = list(pjson.findPropList(self.model, stmt[0], stmt[1], stmt[2], stmt[3], stmt[4]))
                for row in rows:
                    self.model.removeStatement(base.Statement(*row[:5]))
        
    def remove(self, removes):
        '''
        Removes data from the store.
        `removes`: A list of either statements or pjson conforming dicts
        '''
        if not self.join(self.requestProcessor.txnSvc):
            #not in a transaction, so call this inside one
            func = lambda: self.remove(removes)
            return self.requestProcessor.executeTransaction(func)
        
        stmts, jsonrep = self._toStatements(removes)

        if self.removeTrigger and stmts:
            self.removeTrigger(stmts, jsonrep)        
        self._removePropLists(stmts)
        self.model.removeStatements(stmts)
        
        return jsonrep or stmts


    def update(self, updates):
        '''
        Update the store by either adding or replacing the property value pairs
        given in the update, depending on whether or not the pair currently 
        appears in the store.

        See also `replace`.

        `updates`: A list of either statements or pjson conforming dicts
        '''
        return self.updateAll(updates, [])

    def replace(self, replacements):
        '''
        Replace the given objects in the store. Unlike `update` this method will
        remove properties in the store that aren't specified.
        Also, if the data contains json object and an object has no properties
        (just an `id` property), the object will be removed.

        See also `update` and `updateAll`.

        `replacements`: A list of either statements or pjson conforming dicts
        '''
        return self.updateAll([], replacements)

    def updateAll(self, update, replace, removedResources=None):
        '''
        Add, remove, update, or replace resources in the store.

        `update`: A list of either statements or pjson conforming dicts that will
        be processed with the same semantics as the `update` method.
        
        `replace`: A list of either statements or pjson conforming dicts that will
        be processed with the same semantics as the `replace` method.
        
        `removedResources`: A list of ids of resources that will be removed 
        from the store.
        '''
        if not self.join(self.requestProcessor.txnSvc):
            #not in a transaction, so call this inside one
            func = lambda: self.updateAll(update, replace, removedResources)
            return self.requestProcessor.executeTransaction(func)
        
        newStatements = []
        newListResources = set()
        removedResources = set(removedResources or [])
        removals = []
        #update:
        #XXX handle scope: if a non-empty scope is specified, only compare
        updateStmts, ujsonrep = self._toStatements(update)
        root = OrderedModel(updateStmts)
        skipResource = None
        for (resource, prop, values) in root.groupbyProp():            
            #note: for list resources, rdf:type will be sorted first 
            #but don't assume they are present
            if skipResource == resource:
                continue
            if (prop == base.RDF_MS_BASE+'type' and 
                (pjson.PROPSEQTYPE, base.OBJECT_TYPE_RESOURCE) in values):
                skipResource = resource
                newListResources.add(resource)
                continue
            if prop in (base.RDF_SCHEMA_BASE+u'member', 
                                base.RDF_SCHEMA_BASE+u'first'):
                #if list just replace the entire list
                removedResources.add(resource)
                skipResource = resource
                newListResources.add(resource)
                continue
            currentStmts = self.model.getStatements(resource, prop)
            if currentStmts:
                #the new proplist probably have different ids even for values that
                #don't need to be added so remove all current proplists                
                self._removePropLists(currentStmts)                
                for currentStmt in currentStmts:
                    if (currentStmt.object, currentStmt.objectType) not in values:
                        removals.append(currentStmt)
                    else:
                        values.remove((currentStmt.object, currentStmt.objectType))
            for value, valueType in values:                
                newStatements.append( base.Statement(resource,prop, value, valueType) )
        
        for listRes in newListResources:            
            newStatements.extend( root.subjectDict[listRes] )
                
        replaceStmts, replaceJson = self._toStatements(replace)
        if replaceJson:
            for o in replaceJson:
                #the object is empty so make it for removal
                #we need to do this here because empty objects won't show up in
                #replaceStmts
                if len(o) == 1 and 'id' in o: #XXX what about namemapped pjson?
                    removeid = o['id']
                    removedResources.add(removeid)

        #replace:
        #get all statements with the subject and remove them (along with associated lists)
        root = OrderedModel(replaceStmts)
        for resource in root.resources:
            currentStmts = self.model.getStatements(resource)
            for stmt in currentStmts:
                if stmt not in replaceStmts:
                    removals.append(stmt)
                else:
                    replaceStmts.remove(stmt)
            #the new proplist probably have different ids even for values that
            #don't need to be added so remove all current proplists
            self._removePropLists(currentStmts)
        newStatements.extend(replaceStmts)
        
        #remove: remove all statements and associated lists        
        for r in removedResources:
            currentStmts = self.model.getStatements(r)
            for s in currentStmts:
                #it's a list, we need to follow all the nodes and remove them too
                if s.predicate == base.RDF_SCHEMA_BASE+u'next':                    
                    while s:                        
                        listNodeStmts = self.model.getStatements(c.object)
                        removals.extend(listNodeStmts)
                        s = flatten([ls for ls in listNodeStmts 
                                    if ls.predicate == base.RDF_SCHEMA_BASE+u'next'])                    
            removals.extend(currentStmts)

        self.remove(removals)        
        addStmts = self.add(newStatements)

        return addStmts, removals

    def query(self, query=None, bindvars=None, explain=None, debug=False, forUpdate=False, captureErrors=False):
        import vesper.query
        return vesper.query.getResults(query, self.model,bindvars,explain,debug,forUpdate,captureErrors)

    def merge(self,changeset): 
        if not self.join(self.requestProcessor.txnSvc, False):
            #not in a transaction, so call this inside one
            func = lambda: self.merge(changeset)
            return self.requestProcessor.executeTransaction(func) 
        
        assert isinstance(self.graphManager, graphmod.MergeableGraphManager)
        assert isinstance(self._txnparticipants[0], TwoPhaseTxnGraphManagerAdapter)
        graphTxnManager = self._txnparticipants[0]
        try:
            #TwoPhaseTxnGraphManagerAdapter.isDirty() might be wrong when merging
            graphTxnManager.dirty = self.graphManager.merge(changeset)
            return graphTxnManager.dirty
        except:
            graphTxnManager.dirty = True
            raise

class TwoPhaseTxnModelAdapter(transactions.TransactionParticipant):
    '''
    Adapts models which doesn't support transactions or only support simple (one-phase) transactions.
    '''
    logPath = None
    
    def __init__(self, model):
        self.model = model
        self.committed = False
        self.undo = []
        assert model.updateAdvisory, 'need this for undo to work accurately'
    
    def isDirty(self,txnService):
        '''return True if this transaction participant was modified'''
        return txnService.state.additions or txnService.state.removals    
    
    def voteForCommit(self, txnService):
        #the only way we can tell if commit will succeed is to do it now 
        #if not self.model.autocommit:
        self.model.commit(**txnService.getInfo())
        #else: already committed
        self.committed = True
                                        
    def commitTransaction(self, txnService):
        #already commited, nothing to do
        assert self.committed
        if self.logPath:
            self.logChanges(txnService)
    
    def abortTransaction(self, txnService):
        if not self.committed and not self.model.autocommit:
            self.model.rollback()
        else:
            #already committed, commit a compensatory transaction
            #if we're in recovery mode make sure the change isn't already in the store
            if self.undo:
                try:
                    for stmt in self.undo:
                        if stmt[0] is base.Removed:
                            #if not recover or self.model.getStatements(*stmt[1]):
                            self.model.addStatement( stmt[1] )
                        else:
                            self.model.removeStatement( stmt[0] )
                    #if not self.model.autocommit:
                    self.model.commit()
                except:
                    import traceback; traceback.print_exc()
                    pass

    def logChanges(self, txnService):
        import time
        outputfile = file(self.logPath, "a+")
        changelist = self.undo
        def unmapQueue():
            for stmt in changelist:
                if stmt[0] is base.Removed:
                    yield base.Removed, stmt[1]
                else:
                    yield stmt[0]
                    
        comment = txnService.getInfo().get('source','')
        if isinstance(comment, (list, tuple)):                
            comment = comment and comment[0] or ''
            
        outputfile.write("#begin " + comment + "\n")            
        base.writeTriples( unmapQueue(), outputfile)            
        outputfile.write("#end " + time.asctime() + ' ' + comment + "\n")
        outputfile.close()

    def finishTransaction(self, txnService, committed):
        super(TwoPhaseTxnModelAdapter,self).finishTransaction(txnService, committed)
        self.undo = []
        self.committed = False

class TwoPhaseTxnGraphManagerAdapter(transactions.TransactionParticipant):
    '''
    Each underlying model is a wrapped with a TwoPhaseTxnModelAdapter if necessary.
    This must be be listed before those participants
    '''
    
    ctxStmts = None
    
    def __init__(self, graph):
        assert isinstance(graph, graphmod.NamedGraphManager)
        self.graph = graph
        self.dirty = False

    def isDirty(self,txnService):
        '''return True if this transaction participant was modified'''
        return self.dirty or txnService.state.additions or txnService.state.removals    

    def readyToVote(self, txnService):
        #need to do this now so the underlying model gets ctxStmts before it commits
        if self.graph._currentTxn: #simple merging doesn't create a graph txn 
            self.ctxStmts = self.graph._finishCtxResource()
        return True
    
    #no-op -- txnparticipants for underlying models will commit     
    #def voteForCommit(self, txnService): pass
    
    def commitTransaction(self, txnService):
        if self.ctxStmts:
            self.graph._finalizeCommit(self.ctxStmts)

    def abortTransaction(self, txnService):
        if self.ctxStmts:
            #undo the transaction we just voted for
            graph = self.graph
            assert not graph._currentTxn, 'shouldnt be inside a transaction'
           
            #set the current revision to one prior to this
            baseRevisions = [s.object for s in ctxStmts if s[1] == graphmod.CTX_NS+'baseRevision']
            assert len(baseRevisions) == 1
            baseRevision = baseRevisions[0]
            #note: call _markLatest makes db changes so this abort needs to be 
            #called before underlying models' abort
            graph._markLatest(baseRevision)
            graph.currentVersion = baseRevision
                            
    def finishTransaction(self, txnService, committed):
        super(TwoPhaseTxnGraphManagerAdapter,self).finishTransaction(txnService, committed)
        self.graph._currentTxn = None
        self.ctxStmts = None
        self.dirty = False

class ModelWrapper(base.Model):

    def __init__(self, model, adapter):
        self.model = model
        self.adapter = adapter
        
    def __getattribute__(self, name):
        #we can't use __getattr__ because we don't want base class attributes
        #returned, we want those to be delegated
        if name in ModelWrapper.__dict__ or name in ('model', 'adapter'):
            return object.__getattribute__(self, name)
        else:
            assert name not in ('addStatement', 'removeStatement', 'addStatements')
            return getattr(object.__getattribute__(self, 'model'), name)

    def addStatement(self, statement):
        '''add the specified statement to the model'''
        added = self.model.addStatement(statement)
        if added:
            self.adapter.undo.append((statement,))
        return added

    def removeStatement(self, statement):
        '''removes the statement'''
        removed = self.model.removeStatement(statement)
        if removed:
            self.adapter.undo.append((base.Removed, statement))
        return removed
        
    #disable optimized paths
    def addStatements(self, stmts):
        return base.Model.addStatements(self, stmts)

    def removeStatements(self, stmts):
        return base.Model.removeStatements(self, stmts)
