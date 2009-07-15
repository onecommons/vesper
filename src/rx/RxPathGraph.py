'''
Our approach to named graphs/contexts:
* the RxPath DOM and query engine always operate on triples, graphs are
   only used as a selection criteria to filter what appears to be in the underlying model
* this means a statement can appear multiple times in store under different context but appears as one statement
* we compose contexts from several names graphs using the a:includes and a:excludes
* we model a version history by moving removals of statements into contexts referenced by a:excludes
* when we remove a statement we remove it from every context it appears except when removing inside a shredding context
* 'applies-to' applies to the whole context, not just the named graph

a:includes
a:excludes
a:entails (use a more general property name than a:entails
  to encompass modeling things like patch sets or user customizations?)

a:applies-to
a:applies-to-all-including

Optionally this can be used with two separate stores, a primary one which has:
* txn contexts: for each added statement and references to included and excluded contexts
* add contexts: when a statement is added to a particular context

and a secondary one which has the rest of the types of contexts supported here:
* org contexts: where statements removed from a context are moved to
  (consists of an txn or add context followed by the del3 or del4 context that deleted it)
  Only be one per global remove, with preference given to non-context specific remove
* del3 contexts: context for statements globally removed from the store
  (records which contexts using a:applies-to-all-including) 
* del4 contexts: context for statements removed from a particular set of contexts
  (records which contexts using a:applies-to)  
* entailment contexts: context containing all the statements entails by a particular source
   (these are updated with adds and removes as the source changes). 
* other contexts. Same as entailment contexts... there will be an corresponding add context in the main store.

The primary one represents the current state of the model, the secondary one for version history.  
'''

from rx import RxPath, utils
from RxPath import Statement,OBJECT_TYPE_RESOURCE,OBJECT_TYPE_LITERAL,RDF_MS_BASE
import time, re
import logging #for python 2.2 compatibility
log = logging.getLogger("RxPath")

CTX_NS = u'http://rx4rdf.sf.net/ns/archive#'

TXNCTX = 'context:txn:'  #+ modeluri:starttimestamp;version
DELCTX = 'context:del3:' #+ tnxctx (the tnxctx that excludes this)
DEL4CTX = 'context:del4:'#+ tnxctx;;sourcectx (the tnxctx that excludes this)
ORGCTX = 'context:org:' #+ (txnctx|addctx);;(del3|del4)ctx (the ctx it was removed from;; the ctx that removed it)
ADDCTX = 'context:add:' # + txnctx;;sourcectx (the context that the statement was added to;; the txnctx that includes this)
EXTRACTCTX = 'context:extracted:' #+sourceuri (source of the entailment)
APPCTX = 'context:application:'

#thus we can end up with URL like:
# org:add:txn:;3;;entail:http//;;del4:txn:ctx;;
#context:org:context:add:context:txn:http://foo.com/;3;;context:entail:http://foo.com/mypage.xml;;context:del4:context:txn:http://foo.com/;40

def splitContext(ctx):
    '''
    return a dictionary with the following keys:
    ORGCTX, ADDCTX, TXNCTX, EXTRACTCTX, DELCTX
    org, add, fromtxn,  srccontext, totxn,
    '''
    parts = ctx.split(';;')
    if ctx.startswith(ORGCTX):
        if parts[1].startswith(ADDCTX):
            if parts[2].startswith(DEL4CTX):
                delctx = parts[2]+';;'+parts[1]
            else:
                delctx = parts[2]
                #  org:   add:             txn:                        ;; src ctx ;;  delctx
            result=(ctx, ctx[len(ORGCTX):], parts[0][len(ORGCTX+ADDCTX):], parts[1], delctx)
        else:
            result=(ctx, '',                parts[0][len(ORGCTX):],        ''      ,  parts[1])
    elif ctx.startswith(ADDCTX):
          #  org:   add:               txn:                 ;; src ctx  ;;delctx
        result=('', ctx[len(ORGCTX):], parts[0][len(ADDCTX):], parts[1], '')
    elif ctx.startswith(DEL4CTX):
          #  org:   add:  txn:;;                src ctx     ;; delctx
        result=('', '', parts[0][len(DEL4CTX):], parts[1], ctx)
    elif ctx.startswith(DELCTX):
          #  org:   add:  txn:;; src ctx        ;;  delctx
        result=('', '', parts[0][len(DELCTX):], '', ctx)
    elif ctx.startswith(TXNCTX):
          #  org:   add:  txn:;; src ctx    ;;  txnctx
        result=('', '',    ctx,   '', '')
    else:
        result=('', '',    '',    ctx, '')
    return dict( zip((ORGCTX, ADDCTX, TXNCTX, EXTRACTCTX, DELCTX),result) )

class CurrentTxN:
    def __init__(self, txnCtxt):
        self.txnContext = txnCtxt
        self.adds = {} #(stmt, stmt.scope) => [model, stmt)*]
        self.removes = {} #(stmt, stmt.scope) => [[(add/remove, model, stmt)*], visible]
        self.specificContexts = [None]
        # ctxt => [src stmts], [new stmts] #remove new when src is empty
        self.includeCtxts = {}
        self.excludeCtxts = {}
        #list of contexts that have had statement remove from it during this txn
        #addstmts, ctxstmts = currentTxn.del3Ctxts[]
        self.del3Ctxts = {} #srcContext => [src stmts], [new stmts] 
        #associate a del4context with the list of add contexts removed in it
        self.del4Ctxts = {} # del4ctx => {addcontexts=> (src stmts, new stmts)}

    def rollback(self):
        self.adds = {}
        self.removes = {}
        while self.specificContexts:
            contextDoc = self.specificContexts.pop()
            if contextDoc:
                contextDoc.valid = False
        self.specificContexts = [None]

        self.includeCtxts = {}
        self.excludeCtxts = {}
        self.del3Ctxts = {}
        self.del4Ctxts = {}      

    def recordAdd(self, stmt, model, newstmt):
        self.adds.setdefault((stmt,stmt.scope),[]).append(
                                (model, newstmt) )
        model.addStatement(newstmt)        

    def recordRemoves(self, stmt, model, add, newstmt):
        newstmts,vis = self.removes.setdefault((stmt,stmt.scope),[[], False])
        newstmts.append((add, model, newstmt) )
        if add:
            model.addStatement(newstmt)
        else:
            model.removeStatement(newstmt)            
        
class _StatementWithRemoveForContextMarker(Statement):
    removeForContext = True

def getTxnContextUri(modelUri, versionnum):
    return TXNCTX + modelUri + ';' + str(versionnum)

class _NamedGraphManagerBase(object):
    dontPropagate=False
    
    def __init__(self, addmodel, delmodel):
        self.delmodel = delmodel
        self.managedModel = addmodel

    def setDoc(self, doc):
        self.doc = doc        
        doc.graphManager = self
        doc.model = self.managedModel
      
    def isRemoveNecessary(self, node):
        return True

    def propagateRemove(self, doc, stmt):
        if stmt.predicate == RDF_MS_BASE+'rest':                
            return #these aren't included in the DOM
        try:
            #remove the stmt to the ContextDoc
            #we set dontPropagate because its already been removed from
            #this doc
            saveDontPropagate = doc.graphManager.dontPropagate
            doc.graphManager.dontPropagate = True
            subjectNode = doc.findSubject(stmt.subject)
            if subjectNode:
                predicateNode = subjectNode.findPredicate(stmt)
                if predicateNode:
                    subjectNode.removeChild(predicateNode)
                    return True
            return False
        finally:
            doc.graphManager.dontPropagate = saveDontPropagate                          

    def propagateAdd(self, doc, stmt):
        return self._propagateAdds(doc, [stmt])
        
    def _propagateAdds(self, doc, stmts):
        try:
            #add the stmt to the doc
            #we set dontPropagate because its already been added to this doc
            saveDontPropagate = doc.graphManager.dontPropagate
            doc.graphManager.dontPropagate = True                
            try:
                return RxPath.addStatements(doc, stmts)
            except KeyError:
                #this can happen with list statements that we don't want
                #to add to the DOM anyways
                return None
        finally:
            doc.graphManager.dontPropagate = saveDontPropagate
        
    def revertRemoveIfNecessary(self, stmt, currentTxn):
        if (stmt,stmt.scope) in currentTxn.removes:
            newstmts, visible = currentTxn.removes[(stmt,stmt.scope)]
            for add, model, newstmt in newstmts:
                if add: #remove add
                    model.removeStatement(newstmt)
                else: #revert remove
                    model.addStatement(newstmt)

                if newstmt.scope in currentTxn.excludeCtxts:
                    addstmts, excludeModel, ctxstmts = currentTxn.excludeCtxts[
                      newstmt.scope]
                    addstmts.remove(stmt)
                    if not addstmts:
                        for ctxStmt in ctxstmts:
                            excludeModel.removeStatement(ctxStmt)
                        del currentTxn.excludeCtxts[newstmt.scope]

                parts = splitContext(newstmt.scope)
                delctx,addctx = parts[DELCTX], parts[ADDCTX]
                if delctx and addctx and delctx in currentTxn.del4Ctxts:
                    delCtxStmtsDict = currentTxn.del4Ctxts[delctx]                    
                    addstmts, ctxstmts = delCtxStmtsDict[addctx]
                    addstmts.remove(stmt)
                    if not addstmts:
                        for ctxStmt in ctxstmts:
                            self.delmodel.removeStatement(ctxStmt)
                        del delCtxStmtsDict[addctx]

                if addctx and delctx.startswith(DELCTX):
                    srcContext = parts[EXTRACTCTX]
                    addstmts, ctxstmts = currentTxn.del3Ctxts[srcContext]
                    addstmts.remove(stmt)
                    if not addstmts:
                        for ctxStmt in ctxstmts:
                            self.delmodel.removeStatement(ctxStmt)
                        del currentTxn.del3Ctxts[srcContext]
            del currentTxn.removes[(stmt,stmt.scope)]
            return newstmts, visible
        return ()

    def revertAddIfNecessary(self, stmt, currentTxn):        
        if (stmt,stmt.scope) in currentTxn.adds:
            for model, newstmt in currentTxn.adds[(stmt,stmt.scope)]:
                model.removeStatement(newstmt)
                if newstmt.scope.startswith(ADDCTX):
                    addstmts, ctxstmts = currentTxn.includeCtxts[newstmt.scope]
                    addstmts.remove(stmt)
                    if not addstmts:
                        for ctxStmt in ctxstmts:
                            self.doc.model.removeStatement(ctxStmt)
                        del currentTxn.includeCtxts[newstmt.scope]
            del currentTxn.adds[(stmt,stmt.scope)]
            return True
        return False

    def _doRemove(self, model, stmt, currentDelContext, currentTxn):
        txnContext = self.getTxnContext()
        
        if currentDelContext not in currentTxn.excludeCtxts:
            #deleting stmts for the first time in this transaction
            #add statement declaring the deletion context
            if self.createCtxResource:
                removeCtxStmt = Statement(txnContext, CTX_NS+'excludes',
                   currentDelContext,OBJECT_TYPE_RESOURCE, txnContext)
                model.addStatement(removeCtxStmt)
                ctxStmts = [removeCtxStmt]
            else:
                ctxStmts = []
            currentTxn.excludeCtxts[currentDelContext] = ([stmt], model, ctxStmts)
        else:
            currentTxn.excludeCtxts[currentDelContext][0].append(stmt)

        currentTxn.recordRemoves(stmt, self.delmodel, True,
            Statement(stmt[0],stmt[1],stmt[2],stmt[3], currentDelContext))

    def addTrigger(self, node):
        if node and self.doc.addTrigger:
            self.doc.addTrigger(node)
    
class NamedGraphManager(_NamedGraphManagerBase):
    createCtxResource = True
    markLatest = True    
    
    def __init__(self, addmodel, delmodel, lastScope):        
        if not delmodel:
            #if we don't use a separate store for version history
            #wrap the store so that DOM only sees the latest version of the model
            delmodel = addmodel
            managedModel = ExcludeDeletionsModel(addmodel)
        else:
            delmodel = delmodel
            managedModel = addmodel

        super(NamedGraphManager, self).__init__(managedModel,delmodel)

        if not lastScope:
            oldLatest = addmodel.getStatements(predicate=CTX_NS+'latest')
            if oldLatest:
                lastScope = oldLatest[0].subject

        if lastScope:
            parts = lastScope.split(';')
            #context urls will always start with a txn context,
            #with the version after the first ;
            #assert int(parts[1])
            self.lastVersion = int(parts[1])   
        else:
            self.lastVersion = 0

    def setDoc(self, doc):
        super(NamedGraphManager, self).setDoc(doc)
        if not self.lastVersion and self.createCtxResource:
            #model hasn't been saved yet, so create initial context resource
            scope = getTxnContextUri(self.doc.modelUri, self.lastVersion)
            self._createCtxResource(scope)
        self.initializeTxn()
 
    def getTxnContext(self):
        return self.currentTxn.txnContext
    
    def incrementTxnContext(self):
        self.lastVersion += 1
        return getTxnContextUri(self.doc.modelUri, self.lastVersion)        

    def addTrigger(self, node):            
        specificContext = self.currentTxn.specificContexts[-1]
        if isinstance(node, tuple):
            stmt = node
            node = None
        else:
            stmt = node.stmt
            
        if specificContext:            
            addToContextInstead = False
            if node:
                removed=self.currentTxn.removes.get(
                          (stmt,specificContext.contexturi))
                if removed and not removed[1]:#removed[1] == visibleGlobally
                    #we'll be reverting a remove that wasn't visible to this doc
                    #so don't add the node to this doc
                    addToContextInstead = True
            else:
                #the node can't be inserted (it already exists)
                #but we still want to try to add it to the context doc
                addToContextInstead = True

            if addToContextInstead:
                #try to add to contextdoc instead of this one
                addedNodes = self.propagateAdd(specificContext, stmt)
                if addedNodes:
                    if self.doc.addTrigger:
                      #we not adding new node to the base doc but still call 
                      #the trigger (because contextdocs don't have the trigger)
                      assert len(addedNodes) == 1
                      self.doc.addTrigger(addedNodes[0])
                #raise so the node isn't added to this doc:
                raise IndexError('inserting node into current context instead') 
        
        if node and self.doc.addTrigger:
            self.doc.addTrigger(node)                  
        
    def add(self, stmt, node=None):
        if self.dontPropagate:
            #already handled by the context graph manager
            return

        specificContext = self.currentTxn.specificContexts[-1]
        if specificContext:
            self.propagateAdd(specificContext, stmt)
        else:
            #make sure no scope is set
            stmt = Statement(scope='', *stmt[:4])
            if self.revertRemoveIfNecessary(stmt,self.currentTxn):
                return
            newstmt = Statement(scope=self.getTxnContext(), *stmt[:4])
            self.currentTxn.recordAdd(stmt, self.doc.model, newstmt)

    def isRemoveNecessary(self, node):
        '''
        When need this check to handle the case where removing a
        node from the base doc while in a specific context.
        In this case, we want to leave the node in the base doc
        if the statement appears in other contexts.
                        
        returns False if the node should remain in the DOM
        '''
        #node is either a resource or predicate node
        contextDoc = self.currentTxn.specificContexts[-1]
        if not contextDoc:
            return True
        
        noneAreVisible = True
        if isinstance(node, RxPathDom.Resource):
            predNodes = node.childNodes
        else:
            assert isinstance(node, RxPathDom.BasePredicate)
            predNodes = [node]

        for prednode in predNodes:
            stmt = prednode.stmt
            if not self.dontPropagate:
                #do the remove now (else: it has already been done)
                if not self.propagateRemove(contextDoc, stmt):
                    raise RxPathDom.NotFoundErr("trying to remove a statement "
                        "(%s)  that doesn't exist in the specified context (%s)"
                                        % (str(stmt), contextDoc.contexturi))            
            #see if the node is still visible
            #stmt will not be in removes if the remove just reverted an add
            #so set the default visibility to False because in that case the
            #node was added here also and should be removed
            newstmts, visible = self.currentTxn.removes.get(
                      (stmt,contextDoc.contexturi), (1, False))
            if visible:
                noneAreVisible = False
        if not noneAreVisible and self.doc.removeTrigger:
            #we not gonna be removing this node, so invoke the trigger now
            #(because contextdoc triggers don't fire)
            self.doc.removeTrigger(node)
        return noneAreVisible
        
    def remove(self, srcstmt):
        if self.dontPropagate:
            return #already handled by context graph manager

        if self.currentTxn.specificContexts[-1]:
            #we've already handled this in isRemoveNecessary()
            return
        
        srcstmt = Statement(scope='', *srcstmt[:4]) #make sure no scope is set
        if self.revertAddIfNecessary(srcstmt,self.currentTxn):
            return

        currentDelContext = DELCTX + self.getTxnContext()
        #remove all statements that match        
        stmts = self.doc.model.getStatements(asQuad=True, *srcstmt[:4])
        if not len(stmts):
            log.debug('remove failed '+ str(srcstmt))
            return

        #there should only be at most one stmt with a txncontext as scope
        #and any number of stmts with a addcontext as scope
        txnctxEncountered = 0        
        for stmt in stmts:            
            #some statements are read-only and can't be removed
            if stmt.scope.startswith(APPCTX):
                continue

            if stmt.scope.startswith(TXNCTX):
                assert not txnctxEncountered
                txnctxEncountered = 1
                self.currentTxn.recordRemoves(stmt, self.delmodel, True,
                  Statement(stmt[0],stmt[1],stmt[2],                                                     
                  stmt[3],ORGCTX+ stmt.scope +';;'+currentDelContext))
            elif stmt.scope.startswith(ADDCTX):
                self.currentTxn.recordRemoves(stmt, self.delmodel, True,
                    Statement(stmt[0],stmt[1],stmt[2],                                                     
                         stmt[3],ORGCTX+ stmt.scope +';;'+currentDelContext))
                #record each context we're deleting from
                srcContext = stmt.scope.split(';;')[1]
                assert srcContext
                if srcContext not in self.currentTxn.del3Ctxts:
                    ctxStmt = Statement(currentDelContext,
                    u'http://rx4rdf.sf.net/ns/archive#applies-to-all-including',
                        srcContext,OBJECT_TYPE_RESOURCE, currentDelContext)
                    
                    self.delmodel.addStatement(ctxStmt)                    
                    self.currentTxn.del3Ctxts[srcContext] = ([stmt],[ctxStmt])
                else:
                    self.currentTxn.del3Ctxts[srcContext][0].append(stmt)
            elif not stmt.scope:
                scope = getTxnContextUri(self.doc.modelUri, 0)
                self.currentTxn.recordRemoves(stmt, self.delmodel, True,
                    Statement(stmt[0],stmt[1],stmt[2],                                                     
                    stmt[3],ORGCTX+ scope +';;'+currentDelContext))
            else:                
                log.warn('skipping remove, unexpected context ' + stmt.scope)
                continue
            self.currentTxn.recordRemoves(stmt, self.doc.model, False, stmt)
        #use managedmodel to skip schema and entailment triggers  
        self._doRemove(self.managedModel, srcstmt, currentDelContext,
                       self.currentTxn)
            
    def commit(self, kw):
        if self.delmodel != self.managedModel:
            self.delmodel.commit(**kw)
        
        while self.currentTxn.specificContexts:
            contextDoc = self.currentTxn.specificContexts.pop()
            if contextDoc:
                #the context doc will no longer be synchronized
                contextDoc.valid = False
 
        #commit the transaction
        self.doc.model.commit(**kw)
        self.initializeTxn()

    def initializeTxn(self):
        #increment version and set new transaction and context
        self.currentTxn = CurrentTxN(self.incrementTxnContext())   

        scope = self.getTxnContext()
        assert scope

        if not self.createCtxResource:
            return
 
        self._createCtxResource(scope)

    def _createCtxResource(self, scope):
        '''create a new context resource'''
 
        ctxStmts = [
            Statement(scope, RDF_MS_BASE+'type',CTX_NS+'TransactionContext',
                      OBJECT_TYPE_RESOURCE, scope),
        ]

        if self.markLatest:
            oldLatest = self.doc.model.getStatements(predicate=CTX_NS+'latest')
            if oldLatest:
                self.doc.model.removeStatement(oldLatest[0])
            ctxStmts.append(Statement(scope, CTX_NS+'latest', 
                unicode(self.lastVersion),OBJECT_TYPE_LITERAL, scope))

        #first add the context stmts to the DOM tree if its been created 
        if self.doc._childNodes is not None:
            self._propagateAdds(self.doc, ctxStmts)
        #then add the stmts (triggers entailments)
        for stmt in ctxStmts:            
            self.doc.model.addStatement(stmt)

    def rollback(self):
        if self.delmodel != self.managedModel:
            self.delmodel.rollback()
        #we want to increment the version # because we might still have cache 
        #keys referencing this transaction id
        self.initializeTxn()
 
    def pushContext(self,baseContext):
        '''
        Any future changes to the DOM will be specific to this context
        '''
        if not baseContext:
            self.currentTxn.specificContexts.append(None)
            return
        
        assert not self.currentTxn.specificContexts[-1], ("pushContext() only"
        " allowed inside a transaction context, not " +
                              self.currentTxn.specificContexts[-1].contexturi)

        if baseContext.startswith(ORGCTX):
            #special case to allow history to be changed
            #this is used so we can efficiently store deltas
            assert 0 #todo!        
        
        self.currentTxn.specificContexts.append(
                ContextDoc(self.doc, baseContext) )
            
    def popContext(self):
        lastContext = self.currentTxn.specificContexts.pop()
        if lastContext:
            #we're done with this 
            lastContext.valid = False
        elif not self.currentTxn.specificContexts:
            self.currentTxn.specificContexts.append(None)

    def getStatementsInGraph(self, contexturi):
        if contexturi.startswith(TXNCTX) or contexturi.startswith(ADDCTX):
            return self.doc.model.getStatements(context=contexturi)
        else:
            return self.delmodel.getStatements(context=contexturi)

from rx import RxPathDom
class ContextDoc(RxPathDom.Document):
    '''
    Once the transaction that created this has ended, this will become invalid:
    Its representation maybe become out of date and attempting to modify it will raise a RuntimeException.
    Check the "valid" property to find out if this document is still valid.
    '''
    
    def __init__(self, basedoc, contexturi):
        self.contexturi = contexturi
        self.basedoc = basedoc
        if [uri for uri in[TXNCTX,ADDCTX,APPCTX] if contexturi.startswith(uri)]:
            basemodel = basedoc.model
        else:
            basemodel = basedoc.graphManager.delmodel            

        if isinstance(basemodel, RxPath.BaseSchema):
            #don't include entailments
            stmts = basemodel.model.getStatements(context=contexturi)
        else:
            stmts = basemodel.getStatements(context=contexturi)
        model = RxPath.ViewModel(basemodel, stmts)
        graphManager = ContextNamedGraphManager(model, 
                        basedoc.graphManager.delmodel)
        schemaClass = RxPath.BaseSchema #basedoc.schemaClass
        super(ContextDoc, self).__init__(model, basedoc.nsRevMap,
                                         basedoc.modelUri, schemaClass,
                                         graphManager=graphManager)
        #note: triggers won't be set, but in general we don't want them to be
        self.valid = True

    def getKey(self):
        return (self.basedoc.getKey(), self.contexturi)
                
    def commit(self, **kw):
        raise RuntimeError("invalid operation for ContextDoc")

    def rollback(self):
        raise RuntimeError("invalid operation for ContextDoc")

    def pushContext(self,uri):
        raise RuntimeError("invalid operation for ContextDoc")

    def popContext(self):
        raise RuntimeError("invalid operation for ContextDoc")
        
class ContextNamedGraphManager(_NamedGraphManagerBase):        
    createCtxResource = property(
        lambda self: self.doc.basedoc.graphManager.createCtxResource)
    
    def getTxnContext(self):
        return self.doc.basedoc.graphManager.currentTxn.txnContext

    def add(self, stmt, node=None):
        if not self.doc.valid:
            raise RuntimeError('trying to modify a invalid ContextDoc')
          
        currentTxn = self.doc.basedoc.graphManager.currentTxn
        if not currentTxn.specificContexts[-1]:
          assert not self.dontPropagate
          #we're trying to add a node in a ContextDoc while in the global context
          #so treat this as a global add and don't modify this ContextDoc
          self.propagateAdd(self.doc.basedoc, stmt)
          return

        assert self.doc is currentTxn.specificContexts[-1]
        baseContext = self.doc.contexturi
        #stmt won't necessarily have the right scope set
        #(e.g. when coming from the node of basedoc)
        stmt = Statement(scope=baseContext, *stmt[:4])
        
        revert = self.revertRemoveIfNecessary(stmt,currentTxn)
        if not revert:
            txnCtxt = currentTxn.txnContext                    
            addContext = ADDCTX + txnCtxt + ';;' + baseContext

            if addContext not in currentTxn.includeCtxts:
                if self.createCtxResource:                
                    #add info about the included context                
                    newCtxStmts = [
                    Statement(txnCtxt, CTX_NS+'includes',
                            addContext,OBJECT_TYPE_RESOURCE, txnCtxt),
                    #just infer this from a:includes rdfs:range a:Context
                    #Statement(addContext, RDF_MS_BASE+'type',
                    #    'http://rx4rdf.sf.net/ns/archive#Context',
                    #              OBJECT_TYPE_RESOURCE, addContext),
                    Statement(addContext, CTX_NS+'applies-to',
                            baseContext, OBJECT_TYPE_RESOURCE, addContext),
                    ]                
                    for ctxStmt in newCtxStmts: 
                        #use managedmodel to skip schema and entailment triggers           
                        self.doc.basedoc.graphManager.managedModel.addStatement(
                                                                        ctxStmt)
                else:
                    newCtxStmts = []
                currentTxn.includeCtxts[addContext] = ([stmt], newCtxStmts)
            else:
                currentTxn.includeCtxts[addContext][0].append(stmt)

            currentTxn.recordAdd(stmt, self.doc.basedoc.model,
                                  Statement(scope=addContext, *stmt[:4]) )
            #save the statement again using the original scope
            currentTxn.recordAdd(stmt, self.delmodel,stmt)
        else:
            #this node removed in this transaction, just revert that remove
            #check if this node was also removed from the base doc too            
            newstmts, visibleGlobally = revert
            if not visibleGlobally:
                #it wasn't, so don't propagate this add                
                if not self.dontPropagate and node:
                    #but we do want to invoke the tigger
                    if self.doc.basedoc.addTrigger:
                        self.doc.basedoc.addTrigger(node)
                return 

        if not self.dontPropagate:
            self.propagateAdd(self.doc.basedoc, stmt)

    def remove(self, stmt):      
        '''
        Remove from the current specific context only:
        * remove the statement 
        * Add the stmt to the current del4 context    
        '''
        if not self.doc.valid:
            raise RuntimeError('trying to modify a invalid ContextDoc')


        currentTxn = self.doc.basedoc.graphManager.currentTxn
        if not currentTxn.specificContexts[-1]:
          assert not self.dontPropagate
          #try to remove a node in a ContextDoc while in the global context
          #so treat this as a global remove and don't modify this ContextDoc
          self.propagateRemove(self.doc.basedoc, stmt)
          return
          
        assert self.doc is currentTxn.specificContexts[-1], (
           'wrong context: %s not %s' % (currentTxn.specificContexts[-1], self.doc))

        sourceContext = self.doc.contexturi
        #stmt won't necessarily have the right scope set
        #(e.g. when coming from the node of basedoc)
        stmt = Statement(scope=sourceContext, *stmt[:4])

        if self.revertAddIfNecessary(stmt,currentTxn):
            if not self.dontPropagate:
                #if dontPropagate isn't set, the removal occured on the contextdoc          
                #so remove the node from the base doc too
                self.propagateRemove(self.doc.basedoc, stmt)
            return
        
        currentDelContext = DEL4CTX +  self.getTxnContext() + ';;' + sourceContext        
        #we need to search the global model since we need the ADDCTX info        
        stmts = self.doc.basedoc.model.getStatements(asQuad=True,*stmt[:4])
        for matchstmt in stmts:
            if matchstmt.scope.startswith(ADDCTX) and matchstmt.scope.endswith(
                                                    ';;'+sourceContext):
                #a bit of a hack: we use _StatementWithRemoveForContextMarker
                #to signal to the incremental NTriples file model that this
                #remove is specific to this context, not global
                currentTxn.recordRemoves(stmt, self.doc.basedoc.model, False,
                              _StatementWithRemoveForContextMarker(*matchstmt) )
                orginalAddContext = matchstmt.scope
                break
        else:
            orginalAddContext = None            
        
        stillVisibleGlobally = len(stmts) - bool(orginalAddContext) > 0        
        currentTxn.recordRemoves(stmt, self.delmodel, False, stmt)
        currentTxn.removes[(stmt,stmt.scope)][1] = stillVisibleGlobally

        if not self.dontPropagate:
            #if dontPropagate isn't set, the removal occured on the contextdoc          
            #so remove the node from the base doc too
            #we do this even if stillVisibleGlobally is false
            #to ensure the basedoc's trigger is called
            self.propagateRemove(self.doc.basedoc, stmt)

        if not orginalAddContext:            
            #this stmt must have already been deleted by a global delete,
            #so we'll have to figure out when this statement was added
            #to the source context
            delstmts = self.delmodel.getStatements(asQuad=True,*stmt[:4])
            orgcontexts = [s.scope for s in delstmts
                if (s.scope.startswith(ORGCTX+ADDCTX)
                    and splitContext(s.scope)[EXTRACTCTX] == sourceContext)]
            if not orgcontexts:
                log.debug('remove failed '+ str(stmt) +
                      ' for context ' + self.doc.contexturi)              
                return False #not found!!                
            orgcontexts.sort(lambda x,y: comparecontextversion(
                  getTransactionContext(x),getTransactionContext(y)) )
            orginalAddContext = splitContext(orgcontexts[-1])[ADDCTX]
        assert orginalAddContext        

        #we add the stmt to the org context to record which transaction
        #the removed statement was added
        #don't include the srcContext in ORGCTX since its already in the ADDCTX
        currentDelContextWithoutSrc = DEL4CTX + self.getTxnContext()
        currentTxn.recordRemoves(stmt, self.delmodel, True, 
            Statement(stmt[0],stmt[1],stmt[2],stmt[3],
            ORGCTX+ orginalAddContext +';;'+currentDelContextWithoutSrc))

        delCtxStmtsDict = currentTxn.del4Ctxts.setdefault(currentDelContext,{})
        if orginalAddContext not in delCtxStmtsDict:
            ctxStmt = Statement(currentDelContext, CTX_NS+'applies-to',
                orginalAddContext,OBJECT_TYPE_RESOURCE, currentDelContext)
            self.delmodel.addStatement(ctxStmt)            
            delCtxStmtsDict[orginalAddContext] = ([stmt], [ctxStmt])
        else:
            delCtxStmtsDict[orginalAddContext][0].append(stmt)
            
        #use managedmodel to skip schema and entailment triggers            
        self._doRemove(self.doc.basedoc.graphManager.managedModel, stmt,
            currentDelContext, currentTxn)
        
class DeletionModelCreator(object):
    '''
    This reconstructs the delmodel from add and remove events generated by
    loading a NTriples transaction log (see NTriples2Statements)
    '''
    doUpgrade = False #todo

    def __init__(self, delmodel):
        self.currRemoves = []
        self.currRemovesForContext = {}
        self.delmodel = delmodel
        self.lastScope = None

    oldContextPattern = re.compile('context:(.*)_....-..-..T.._.._..Z')
    def _upgradeScope(self,scope):
        return scope      
##        if scope:
##            match = oldContextPattern.match(scope)
##            if match:                
##                #looks like an old transaction context
##                self.lastScope = TXNCTX + match.group(1) + ';' + str(self.lastVersion)                
##            else:
##                #convert to an ADDCTX
##                scope = ADDCTX + self.lastScope + ';;' + scope
##        return scope
              
    def add(self, stmt):
        scope = self._upgradeScope(stmt[4])
        if stmt[4]:
            if stmt[4].startswith(ADDCTX):
                #reconstruct user defined contexts
                self.delmodel.addStatement(
                    Statement(stmt[0],stmt[1],stmt[2],stmt[3],
                       stmt[4].split(';;')[1]))
            elif not scope.startswith(TXNCTX):
                assert self.doUpgrade
            self.lastScope = stmt[4]
        return stmt

    def _looksLikeSystemRemove(self, stmt):
        if (stmt[1] == 'http://rx4rdf.sf.net/ns/archive#latest'
            or stmt[4].startswith(APPCTX)):
            return True
        else:
            return False
          
    def remove(self, stmt, forContext):
        scope = stmt[4]
        if forContext:
            assert scope.startswith(ADDCTX)
            assert forContext == scope
            self.currRemovesForContext.setdefault(forContext,[]).append(stmt)
        elif scope.startswith(TXNCTX) or scope.startswith(ADDCTX):
            if not self._looksLikeSystemRemove(stmt):
                self.currRemoves.append(stmt)
        else:
            assert self.doUpgrade #this should only occur when upgrading
            
        return stmt

    def comment(self, line):
        if line.startswith('begin'):
            self.currRemoves = []
            self.currRemovesForContext = {}

        if line.startswith('end'):
            #transaction ended
            #record removes that were specific to a context
            for ctxt, stmts in self.currRemovesForContext.items():
                assert ctxt.startswith(ADDCTX)
                srcCtxt = ctxt.split(';;')[1]

                #reconstruct user defined contexts
                currentDelContext = DEL4CTX + self.lastScope + ';;' + srcCtxt
                for stmt in stmts:
                    assert stmt[4]==ctxt, "%s != %s" % (stmt[4], ctxt)

                    self.delmodel.removeStatement(
                        Statement(stmt[0],stmt[1],stmt[2],stmt[3], srcCtxt))
                  
                    self.delmodel.addStatement(
                     Statement(stmt[0],stmt[1],stmt[2],stmt[3], 
                        currentDelContext))
                    
                    self.delmodel.addStatement(
                        Statement(stmt[0],stmt[1],stmt[2],stmt[3],
                       ORGCTX+ctxt +';;' +DEL4CTX+self.lastScope))

                #re-create statements that would be added to the delmodel:
                self.delmodel.addStatement(Statement(currentDelContext,
                u'http://rx4rdf.sf.net/ns/archive#applies-to',
                srcCtxt,OBJECT_TYPE_RESOURCE, currentDelContext))

            #record global removes 
            currentDelContext = DELCTX + self.lastScope
            stmtsRemovedSoFar = set()
            for stmt in self.currRemoves:
                if stmt not in stmtsRemovedSoFar:
                    self.delmodel.addStatement(
                        Statement(stmt[0],stmt[1],stmt[2],stmt[3],
                       currentDelContext))
                    stmtsRemovedSoFar.add(stmt)
                
                scope = stmt[4] or getTxnContextUri(self.doc.modelUri, 0)
                self.delmodel.addStatement(
                    Statement(stmt[0],stmt[1],stmt[2],stmt[3],
                      ORGCTX+ stmt[4] +';;'+currentDelContext))

            for srcCtx in set([s[4].split(';;')[1] for s in self.currRemoves
                               if s[4].startswith(ADDCTX)]):           
                self.delmodel.addStatement(Statement(currentDelContext,
                u'http://rx4rdf.sf.net/ns/archive#applies-to-all-including',
                srcCtx,OBJECT_TYPE_RESOURCE, currentDelContext))

            self.currRemovesForContext = {}                
            self.currRemoves = []

class ExcludeDeletionsModel(RxPath.MultiModel):
    def __init__(self, model):
        RxPath.MultiModel.__init__(self, model)

    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=False):      
        result = self.models[0].getStatements(subject, predicate, object,
                                            objecttype,context,True)
        statements = filter(lambda s: not s.scope or s.scope == APPCTX or
                          s.scope.startswith(TXNCTX)
                          or s.scope.startswith(ADDCTX), result)
        if not asQuad:
            return RxPath.removeDupStatementsFromSortedList(statements, asQuad)
        else:
            return statements
        
def getTransactionContext(contexturi):
    txnpart = contexturi.split(';;')[0]
    index = txnpart.find(TXNCTX)
    if index < 0:
        return '' #not a txn context (e.g. empty or context:application, etc.)
    return txnpart[index:]
    
def _getRevisions(graphManager, resourceuri):    
    stmts = graphManager.model.getStatements(subject=resourceuri, asQuad=True)        
    delstmts = graphManager.delmodel.getStatements(
                                            subject=resourceuri,asQuad=True)
    #get a unique set of transaction context uris, sorted by revision order
    import itertools
    contexts = set([getTransactionContext(s.scope)
                for s in itertools.chain(stmts, delstmts)
                if getTransactionContext(s.scope)]) #XXX: what about s.scope == ''?
    contexts = list(contexts)
    contexts.sort(comparecontextversion)
    return contexts, [s for s in stmts if s.scope], delstmts

def isModifiedAfter(graphManager, contextUri, resources, excludeCurrent = True):
    '''
    returns nodeset of resources that were modified after the given context.
    '''    
    currentContextUri = graphManager.getTxnContext()
    contexts = [] 
    for resUri in resources:
        rescontexts = _getRevisions(graphManager, resUri)[0]
        if rescontexts:
            latestContext = rescontexts.pop()
            if excludeCurrent:              
                #don't include the current context in the comparison
                cmpcurrent = comparecontextversion(currentContextUri, 
                latestContext)
                assert cmpcurrent >= 0, 'context created after current context!?'
                if cmpcurrent == 0:
                    if not rescontexts:
                        continue
                    latestContext = rescontexts.pop()
            contexts.append((latestContext, resUri))
    if not contexts:
        return []
    contexts.sort(lambda x, y: comparecontextversion(x[0],y[0]))
    #include resources that were modified after the given context
    return [resUri for latestContext, resUri in contexts
              if comparecontextversion(contextUri, latestContext) < 0]
          
def getRevisionContexts(graphManager, resuri):    
    contexts, addstmts, removestmts = _getRevisions(graphManager, resuri)
    contexts.sort(cmp=comparecontextversion)
    return contexts  
    
def _showRevision(contexts, addstmts, removestmts, revision):
    '''revision is 0 based'''
    #print contexts, addstmts, removestmts, revision
    rev2Context = dict( [ (x[1],x[0]) for x in enumerate(contexts)] )

    #include every add stmt whose context <= revision    
    stmts = [s for s in addstmts 
                if rev2Context[getTransactionContext(s.scope)] <= revision]    
    
    #include every deleted stmt whose original context <= revision 
    #and whose deletion context > revision        
    stmts.extend([s for s in removestmts if s.scope.startswith(ORGCTX)                                
        and rev2Context[
            getTransactionContext(s.scope)
            ] <= revision 
        and rev2Context[
            s.scope[len(ORGCTX):].split(';;')[-1][len(DELCTX):]
            ] > revision])
    
    return stmts

def getRevisionStmts(graphManager, subjectUri, revisionNum):
    contextsenum, addstmts, removestmts = _getRevisions(graphManager, subjectUri)        
    stmts = _showRevision(contextsenum, addstmts, removestmts, revisionNum)
    return stmts

def comparecontextversion(ctxUri1, ctxUri2):    
    assert (not ctxUri1 or ctxUri1.startswith(TXNCTX),
                    ctxUri1 + " doesn't look like a txn context URI")
    assert (not ctxUri2 or ctxUri2.startswith(TXNCTX),
                    ctxUri2 + " doesn't look like a txn context URI")
    assert not ctxUri2 or len(ctxUri1.split(';'))>1, ctxUri1 + " doesn't look like a txn context URI"
    assert not ctxUri2 or len(ctxUri2.split(';'))>1, ctxUri2 + " doesn't look like a txn context URI"

    return cmp(ctxUri1 and int(ctxUri1.split(';')[1]) or 0,
               ctxUri2 and int(ctxUri2.split(';')[1]) or 0)

##def comparecontextversion(versionString1, versionString2):
##    '''return True if the versionId1 is a superset of versionId2'''
##    versionId1, versionId2 = versionString1[1:].split('.'), versionString2[1:].split('.')    
##    if len(versionId1) != len(versionId2):
##        return False
##    for i, (v1, v2) in enumerate(zip(versionId1, versionId2)): 
##      if i%2: #odd -- revision number
##         if int(v1) < int(v2):
##            return False            
##      else: #even -- branch id
##        if v1 != v2:
##            return False
##    return True

def _getContextRevisions(model, delmodel, srcContext):
    #find the txn contexts with changes to the srcContext
    addchangecontexts = [s.subject for s in model.getStatements(
        object=srcContext,
        predicate='http://rx4rdf.sf.net/ns/archive#applies-to',asQuad=True)]
    #todo: this is redundent if model and delmodel are the same:    
    delchangecontexts = [s.subject for s in delmodel.getStatements(
        object=srcContext,
        predicate='http://rx4rdf.sf.net/ns/archive#applies-to',asQuad=True)]
    del3changecontexts = [s.subject for s in delmodel.getStatements(
        object=srcContext,
        predicate='http://rx4rdf.sf.net/ns/archive#applies-to-all-including',
        asQuad=True)]

    #get a unique set of transaction context uris, sorted by revision order
    txns = {}
    for ctx in addchangecontexts:
        txns.setdefault(getTransactionContext(ctx), []).append(ctx)
    for ctx in delchangecontexts:
        txns.setdefault(getTransactionContext(ctx), []).append(ctx)
    for ctx in del3changecontexts:
        txns.setdefault(getTransactionContext(ctx), []).append(ctx)
    txncontexts = txns.keys()
    txncontexts.sort(comparecontextversion)
    return txncontexts, txns
  
def showContextRevision(graphManager, srcContextUri, revision):
    model = graphManager.model
    delmodel = graphManager.delmodel
    
    txncontexts, txns = _getContextRevisions(model, delmodel, srcContextUri)

    stmts = set()
    delstmts = set()
    for rev, txnctx in enumerate(txncontexts):
        if rev > revision:
            break
        for ctx in txns[txnctx]:
            if ctx.startswith(ADDCTX):
                addstmts = set([s for s in model.getStatements(context=ctx) 
                    if s.subject != ctx])
                stmts += addstmts
                delstmts -= addstmts
            elif ctx.startswith(DELCTX):
                globaldelstmts = set([s for s in 
                    delmodel.getStatements(context=ctx) if s.subject != ctx])
                #re-add these if not also removed by del4
                globaldelstmts -= delstmts
                stmts += globaldelstmts
            elif ctx.startswith(DEL4CTX):
                delstmts = set([s for s in 
                    delmodel.getStatements(context=ctx) if s.subject != ctx])
                stmts -= delstmts
            else:
                assert 0, 'unrecognized context type: ' + ctx
            
    return list(stmts)

def getRevisionContextsForContext(graphManager, srcContextUri):    
    contexts, txns = _getContextRevisions(graphManager.model,
                        graphManager.delmodel, srcContextUri)    

    contexts.sort(cmp=comparecontextversion)
    return contexts  
