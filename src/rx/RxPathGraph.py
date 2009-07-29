'''
This module maintains a transaction history of all changes to the underlying store.
The history can be saved in the store if its supports contexts (aka named graphs) 
or saved into a separate store. 

In addition, you can specify contexts that it should manage and they will be 
associated with the history -- for example to indicate the authorship or source 
of particular changes. 

It can be used for retrieving the version history of particular resources or contexts.
It can also be used to implement distributed or optimistic transactions by 
by providing the ability to undo or redo a transaction in the history.

* we compose contexts from several names graphs using the a:includes and a:excludes
* add contexts: when a statement is added to a particular context (including the empty context)
* del contexts: context for statements removed from a particular contexts
  (including the empty context) (records which contexts using a:applies-to)
* each transaction is recorded inside a txn contexts and included and excluded 
  context in that transaction is associated with the txn context using the 
  a:includes and a:excludes property
* The 'a:applies-to' property associates an add or remove context with the source context
'''

from RxPath import OBJECT_TYPE_LITERAL
from RxPath import OBJECT_TYPE_RESOURCE
from RxPath import RDF_MS_BASE
from RxPath import Statement
import logging
from rx import RxPath
log = logging.getLogger("RxPath")

CTX_NS = u'http://rx4rdf.sf.net/ns/archive#'

TXNCTX = 'context:txn:'  #+ modeluri:starttimestamp;version
DELCTX = 'context:del:' #+ tnxctx (the tnxctx that excludes this)
ADDCTX = 'context:add:' # + txnctx;;sourcectx (the context that the statement was added to;; the txnctx that includes this)
ORGCTX = 'context:org:' #+ (txnctx|addctx);;(del3|del4)ctx (the ctx it was removed from;; the ctx that removed it)
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
                delctx = parts[2] + ';;' + parts[1]
            else:
                delctx = parts[2]
                #  org:   add:             txn:                        ;; src ctx ;;  delctx
            result = (ctx, ctx[len(ORGCTX):], parts[0][len(ORGCTX + ADDCTX):], parts[1], delctx)
        else:
            result = (ctx, '', parts[0][len(ORGCTX):], '', parts[1])
    elif ctx.startswith(ADDCTX):
        #  org:   add:               txn:                 ;; src ctx  ;;delctx
        result = ('', ctx[len(ORGCTX):], parts[0][len(ADDCTX):], parts[1], '')
    elif ctx.startswith(DELCTX):
        #  org:   add:  txn:;;                src ctx     ;; delctx
        result = ('', '', parts[0][len(DELCTX):], parts[1], ctx)
    elif ctx.startswith(TXNCTX):
        #  org:   add:  txn:;; src ctx    ;;  txnctx
        result = ('', '', ctx, '', '')
    else:
        result = ('', '', '', ctx, '')
    return dict(zip((ORGCTX, ADDCTX, TXNCTX, EXTRACTCTX, DELCTX), result))

def isTransactionContext(contexturi):
    for prefix in [TXNCTX,ADDCTX,DELCTX]:
        if contexturi.startswith(prefix):
            return True
    return False

class CurrentTxN:
    def __init__(self, txnCtxt):
        self.txnContext = txnCtxt
        self.adds = {} #orignal_stmt => (primarystore stmt or none, txn_stmt)
        self.removes = {} #orignal_stmt => (primarystore stmt or none, txn_stmt)
        
    def rollback(self):
        self.adds = {}
        self.removes = {}

def getTxnContextUri(modelUri, versionnum):
    return TXNCTX + modelUri + ';' + str(versionnum)

class NamedGraphManager(RxPath.Model):
    #The orginal implementation included ADD and TXN contexts when it stored statements in the primary store, 
    #and so the secondary store only recorded deletions.  While this is more 
    #space and update time efficient (because no extra statements were stored) it requires the primary be a quad store 
    #with support for asQuad = False -- which limits what kinds of stores could be used. 
    #
    #If this functionality was reintroduced then revert old behavior for getStatements and "remove w/ no context" 
    #(which would have to delete matching ADD*contexts and add ORG:contexts)
    #and re-add support for ORG:context (i.e. in getRevisionStmts)
    
    #: set this to false to suppress the graph manager from adding statements to the store (useful for testing)
    createCtxResource = True 
    markLatest = True
    lastLatest = None

    autocommit = property(lambda self: self.managedModel.autocommit,
        lambda self, set:
            setattr(self.managedModel, 'autocommit', set) or
            setattr(self.revisionModel, 'autocommit', set)
        )
    
    def __init__(self, primaryModel, revisionModel, modelUri, lastScope=None):
        '''
        primaryModel: store containing current state of data unmodified
        only needs to support data as used by application
        (e.g. only needs to be a triple-store if the application
        doesn't use contexts)

        revisionModel: Contains version history, must support arbitrary quads.
        If None, the revision statements will be stored in the primaryModel.
        '''
        if not revisionModel:
            #don't use a separate store for version history
            self.revisionModel = primaryModel
            self.managedModel = primaryModel
        else:
            self.revisionModel = revisionModel
            self.managedModel = primaryModel
        
        self.modelUri = modelUri
        
        if lastScope:
            parts = lastScope.split(';')
            #context urls will always start with a txn context,
            #with the version after the first ;
            #assert int(parts[1])
            self.lastVersion = int(parts[1])   
        else:
            self.lastVersion = -1 #so the 1st version # will be 0
        self._currentTxn = None
        
    def getStatements(self, subject=None, predicate=None, object=None,
        objecttype=None, context=None, asQuad=True, hints=None):
        '''
        Retrieve matching statements from the primary model unless a context is 
        specified that is managed by the revision model. In that case, the
        matching statements are found by reconstructing the current state of that
        context from the revision model (which can be compartively expensive).
        '''
        hints = hints or {}

        if not context:
            stmts = self.managedModel.getStatements(subject, predicate, object,
                objecttype, context, asQuad, **hints)
            if context is None and self.revisionModel is self.managedModel:
                #using single model and searching across all contexts, 
                #so we need to filter out TXN contexts  
                stmts = filter(lambda s: self.isContextForPrimaryStore(s.scope), stmts)
                #XXX limit and offset could be wrong if filter shortens results
                if not asQuad or hints:
                    stmts.sort()
                    stmts = RxPath.removeDupStatementsFromSortedList(stmts, 
                        asQuad, **hints)
            return stmts
        else:
            if self.isContextForPrimaryStore(context):
                model = self.managedModel
            else:
                if isTransactionContext(context):
                    model = self.revisionModel
                else:
                    #note: this can be expensive!
                    model = MemModel(self.getStatementsForContextAndRevision(context))
                    context = None

            return model.getStatements(subject, predicate, object,
                objecttype, context, asQuad, **hints)
         
    @property
    def currentTxn(self):
        if not self._currentTxn:
            self.initializeTxn()
        return self._currentTxn
    
    def getTxnContext(self):
        return self.currentTxn.txnContext
    
    def incrementTxnContext(self):
        if self.markLatest:
            oldLatest = self.getStatements(predicate=CTX_NS + 'latest')
            if oldLatest:
                self.lastLatest = oldLatest[0]
                lastScope = oldLatest[0].subject
                parts = lastScope.split(';')
                #context urls will always start with a txn context,
                #with the version after the first ;
                #assert int(parts[1])                
                self.lastVersion = int(parts[1]) + 1
            else:
                self.lastLatest = None
                self.lastVersion = 0
        else:
            self.lastVersion += 1

        return getTxnContextUri(self.modelUri, self.lastVersion)        

    def isContextForPrimaryStore(self, context):
        '''
        Returns true if the context URI is for the primary model
        By default, contexts that start with "context:" are excluded from
        the primary model.

        :context: the context URI or an empty string
        '''
        return not context.startswith('context:')

    def addStatement(self, srcstmt):
        '''
        Add the statement to the primary and revision models.
        If the statement has a context that is managed by the revision model
        then store it in the primary model without a context (but first make sure
        the statement with the empty context isn't already in the primary model).
        '''
        srcstmt = Statement( * srcstmt) #make sure it's an unmutable statement
        if self.revertRemoveIfNecessary(srcstmt):
            return #the add just reverts a remove, so we're done

        scope = srcstmt.scope
        if not self.isContextForPrimaryStore(scope):
            if isTransactionContext(scope):
                raise RuntimeError("can't directly add scope: "+scope)
            stmt = Statement(scope='', *srcstmt[:4])
            if self.managedModel.getStatements(*stmt):
                #already exists, so don't re-add
                stmt = None 
        else:
            stmt = srcstmt
        if stmt:
            self.managedModel.addStatement(stmt)

        currentTxn = self.currentTxn
        txnCtxt = currentTxn.txnContext
        addContext = ADDCTX + txnCtxt + ';;' + scope                    
        newstmt = Statement(scope=addContext, * srcstmt[:4])
        self.revisionModel.addStatement(newstmt)

        currentTxn.adds[srcstmt] = (stmt, newstmt)
        
    def removeStatement(self, srcstmt):
        '''
        Remove the statement from the primary model and add the statement using
        the DEL context to the revision model. If the statement has a context
        that is managed by the revision model remove the equivalent statement
        without a context from  the primary model (but only if the statement
        wasn't already added with an empty context or with another revision model
        managed context).
        '''
        srcstmt = Statement(*srcstmt) #make sure its an unmutable statement
        if self.revertAddIfNecessary(srcstmt):
            return

        removeStmt = None
        if self.isContextForPrimaryStore(srcstmt.scope):
            removeStmt = srcstmt
        else:
            if isTransactionContext(srcstmt.scope):
                raise RuntimeError("can't directly remove with scope: "+srcstmt.scope)
            #if the scope isn't intended for the primary store
            #we assume there might be multiple statements with different scopes
            #that map to the triple in the primary store
            #so search the delmodel for live adds, if there's only one,
            #remove the statement from the primary store
            stmts = self.revisionModel.getStatements(*srcstmt[:4])
            stmts.sort(cmp=comparecontextversion)
            adds = 0
            for s in stmts:
                orginalscope = splitContext(s.scope)[EXTRACTCTX]
                if orginalscope and self.isContextForPrimaryStore(orginalscope):
                    #dont include this statement in the count because it's in a
                    #context that primary store manages
                    continue
                if s.scope.startswith(DELCTX):
                    adds -= 1#del adds[orginalscope]
                elif s.scope.startswith(ADDCTX):
                    adds += 1 #adds[orginalscope] = s
                else:
                    if self.revisionModel is not self.managedModel:
                        #should only encounter this in single model mode 
                        raise RuntimeError('unexpected statement: %s' % s)
            
            if adds == 1: #len(adds) == 1:
                #last one in the primary model, so delete it
                removeStmt = Statement(scope='', * srcstmt[:4])
        
        #record deletion in history store
        txnContext = self.getTxnContext()
        delContext = DELCTX + txnContext + ';;' + srcstmt.scope
        delStmt = Statement(scope=delContext, * srcstmt[:4])
        self.revisionModel.addStatement(delStmt)
        if removeStmt:
            self.managedModel.removeStatement(removeStmt)
        self.currentTxn.removes[srcstmt] = (removeStmt, delStmt)

    def commit(self, ** kw):
        self._finishCtxResource()
        if self.revisionModel != self.managedModel:
            self.revisionModel.commit( ** kw)
         
        #commit the transaction
        self.managedModel.commit( ** kw)
        self._currentTxn = None

    def initializeTxn(self):
        #increment version and set new transaction and context
        self._currentTxn = CurrentTxN(self.incrementTxnContext())   

        self._createCtxResource()

    def _createCtxResource(self):
        '''create a new context resource'''

        txnContext = self.getTxnContext()
        assert txnContext

        if self.createCtxResource:
            self.revisionModel.addStatement(
                Statement(txnContext, RDF_MS_BASE + 'type',
                CTX_NS + 'TransactionContext', OBJECT_TYPE_RESOURCE, txnContext)
            )
        if self.markLatest:
            if self.lastLatest:
                self.revisionModel.removeStatement(self.lastLatest)
            self.revisionModel.addStatement(
                Statement(txnContext, CTX_NS + 'latest',
                    unicode(self.lastVersion), OBJECT_TYPE_LITERAL, txnContext)
            )

    def _finishCtxResource(self):
        if not self.createCtxResource:
            return

        txnContext = self.getTxnContext()
        assert txnContext
        ctxStmts = []

        def findContexts(changes):
            return set([(key.scope, value[1].scope)
                for key, value in changes.items()])

        excludeCtxts = findContexts(self.currentTxn.removes)
        for (scope, delContext) in excludeCtxts:
            #add statement declaring the deletion context
            removeCtxStmt = Statement(txnContext, CTX_NS + 'excludes',
                delContext, OBJECT_TYPE_RESOURCE, txnContext)
            ctxStmts.append(removeCtxStmt)            
            ctxStmts.append(Statement(delContext, CTX_NS + 'applies-to',
                scope, OBJECT_TYPE_RESOURCE, delContext))

        includeCtxts = findContexts(self.currentTxn.adds)
        for (scope, addContext) in includeCtxts:
            #add info about the included context
            ctxStmts.append(
                Statement(txnContext, CTX_NS + 'includes',
                    addContext, OBJECT_TYPE_RESOURCE, txnContext)
            )
            #just infer this from a:includes rdfs:range a:Context
            #Statement(addContext, RDF_MS_BASE+'type',
            #    'http://rx4rdf.sf.net/ns/archive#Context',
            #              OBJECT_TYPE_RESOURCE, addContext),
            ctxStmts.append(Statement(addContext, CTX_NS + 'applies-to',
                scope, OBJECT_TYPE_RESOURCE, addContext))

        self.revisionModel.addStatements(ctxStmts)

    def rollback(self):
        self.managedModel.rollback()
        if self.revisionModel != self.managedModel:
            self.revisionModel.rollback()
        self._currentTxn = None
        #note: we might still have cache 
        #keys referencing this version (transaction id)  
 
    def revertAddIfNecessary(self, stmt):
        add = self.currentTxn.adds.pop(stmt, None)
        if add:
            (stmt, newstmt) = add
            if stmt:
                self.managedModel.removeStatement(stmt)
            self.revisionModel.removeStatement(newstmt)
            return True

        return False

    def revertRemoveIfNecessary(self, stmt):
        remove = self.currentTxn.removes.pop(stmt, None)
        if remove:
            (stmt, newstmt) = remove
            if stmt:
                self.managedModel.addStatement(stmt)
            self.revisionModel.removeStatement(newstmt)
            return True

        return False

    ###### revision querying methods #############
        
    def isModifiedAfter(self, contextUri, resources, excludeCurrent=True):
        '''
        Given a list of resources, return list a of the ones that were modified
        after the given context.
        '''    
        currentContextUri = self.getTxnContext()
        contexts = [] 
        for resUri in resources:
            rescontexts = self.getRevisionContexts(resUri)
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
        contexts.sort(lambda x, y: comparecontextversion(x[0], y[0]))
        #include resources that were modified after the given context
        return [resUri for latestContext, resUri in contexts
            if comparecontextversion(contextUri, latestContext) < 0]

    def getRevisionContexts(self, resourceuri, stmts=None):
        '''
        return a list of transaction contexts that modified the given resource,
        sorted by revision order
        '''
        if stmts is None:
            stmts = self.revisionModel.getStatements(subject=resourceuri)
        contexts = set(filter(None,
            [getTransactionContext(s.scope) for s in stmts]))
        contexts = list(contexts)
        contexts.sort(comparecontextversion)
        return contexts

    def getRevisionStmts(self, subjectUri, revision):
        '''
        Return the statements visible at the given revision 
        for the specified resource.
        
        revision: 0-based revision number
        '''
        stmts = self.revisionModel.getStatements(subject=resourceuri)
        contexts = self.getRevisionContexts(subjectUri, stmts)
        rev2Context = dict([(x[1], x[0]) for x in enumerate(contexts)])

        #only include transactional statements
        stmts = [s for s in stmts if (getTransactionContext(s.scope) and
            rev2Context[getTransactionContext(s.scope)] <= revision)]
        stmts.sort(cmp=lambda x, y: comparecontextversion(x.scope, y.scope))
        revisionstmts = set()
        for s in stmts:
            if s.scope.startswith(DELCTX):
                revisionstmts.discard(s)
            elif s.scope.startswith(ADDCTX):
                revisionstmts.add(s)

        return revisionstmts
    
    def getRevisionContextsForContext(self, srcContext):
        '''
        return a list of transaction contexts that modified the given context
        '''
        #find the txn contexts with changes to the srcContext
        txncontexts = [s.subject for s in self.revisionModel.getStatements(
            object=srcContext,
            predicate='http://rx4rdf.sf.net/ns/archive#applies-to')]
    
        #get a unique set of transaction context uris, sorted by revision order
        txncontexts.sort(key=getTransactionVersion)
        return txncontexts
      
    def getStatementsForContextAndRevision(self, srcContextUri, revision=-1):
        '''
        Return the statements visible at the given revision 
        for the specified context.
        
        revision: 0-based revision number
        '''
        model = self.revisionModel
        txncontexts = self.getRevisionContextsForContext(srcContextUri)
    
        stmts = set()        
        for rev, ctx in enumerate(txncontexts):
            if revision > -1 and rev > revision:
                break

            ctxstmts = set([Triple(s) for s in
                model.getStatements(context=ctx) if s.subject != ctx])

            if ctx.startswith(ADDCTX):
                stmts += ctxstmts
            elif ctx.startswith(DELCTX):
                stmts -= ctxstmts
            else:
                assert 0, 'unrecognized context type: ' + ctx
                
        return list(stmts)

class DeletionModelCreator(object):
    '''
    This reconstructs the revision model from add and remove events generated by
    loading a NTriples transaction log (see NTriples2Statements)
    '''
    doUpgrade = False 

    def __init__(self, model):
        self.currRemovesForContext = {}
        self.revisionModel = model
        self.lastScope = None

    def _upgradeScope(self, scope):
        return scope      
              
    def add(self, stmt):
        scope = self._upgradeScope(stmt[4])
        if stmt[4]:
            if stmt[4].startswith(ADDCTX):
                #reconstruct user defined contexts
                self.revisionModel.addStatement(
                    Statement(stmt[0], stmt[1], stmt[2], stmt[3],
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
        assert scope.startswith(ADDCTX)
        assert forContext == scope
        if not self._looksLikeSystemRemove(stmt):
            self.currRemovesForContext.setdefault(forContext, []).append(stmt)

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
                currentDelContext = DELCTX + self.lastScope + ';;' + srcCtxt
                for stmt in stmts:
                    assert stmt[4] == ctxt, "%s != %s" % (stmt[4], ctxt)

                    self.revisionModel.removeStatement(
                        Statement(stmt[0], stmt[1], stmt[2], stmt[3], srcCtxt))
                  
                    self.revisionModel.addStatement(
                        Statement(stmt[0], stmt[1], stmt[2], stmt[3],
                            currentDelContext))

                #re-create statements that would be added to the delmodel:
                self.revisionModel.addStatement(Statement(currentDelContext,
                u'http://rx4rdf.sf.net/ns/archive#applies-to',
                srcCtxt, OBJECT_TYPE_RESOURCE, currentDelContext))

            self.currRemovesForContext = {}                

def getTransactionContext(contexturi):
    txnpart = contexturi.split(';;')[0]
    index = txnpart.find(TXNCTX)
    if index < 0:
        return '' #not a txn context (e.g. empty or context:application, etc.)
    return txnpart[index:]

def getTransactionVersion(contexturi):
    ctxUri1 = getTransactionContext(contexturi)
    return ctxUri and int(ctxUri.split(';')[1]) or 0

def comparecontextversion(ctxUri1, ctxUri2):    
    assert (not ctxUri1 or ctxUri1.startswith(TXNCTX),
        ctxUri1 + " doesn't look like a txn context URI")
    assert (not ctxUri2 or ctxUri2.startswith(TXNCTX),
        ctxUri2 + " doesn't look like a txn context URI")
    assert not ctxUri2 or len(ctxUri1.split(';')) > 1, ctxUri1 + " doesn't look like a txn context URI"
    assert not ctxUri2 or len(ctxUri2.split(';')) > 1, ctxUri2 + " doesn't look like a txn context URI"

    return cmp(ctxUri1 and int(ctxUri1.split(';')[1]) or 0,
               ctxUri2 and int(ctxUri2.split(';')[1]) or 0)

