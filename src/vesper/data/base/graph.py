#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
This module maintains a transaction history of all changes to the underlying store.
The history can be saved in the store if its supports contexts (aka named graphs) 
or saved into a separate store. 

In addition, you can specify contexts that it should manage and they will be 
associated with the history -- for example to indicate the authorship or source 
of particular changes. 

It can be used for retrieving the version history of particular resources or contexts.
It can also be used to implement distributed or optimistic transactions by 
by providing the ability to create a compensating transaction in the history.

* we compose contexts from several names graphs using the a:includes and a:excludes
* add contexts: when a statement is added to a particular context (including the empty context)
* del contexts: context for statements removed from a particular contexts
  (including the empty context) (records which contexts using a:applies-to)
* each transaction is recorded inside a txn contexts and included and excluded 
  context in that transaction is associated with the txn context using the 
  a:includes and a:excludes property
* The 'a:applies-to' property associates an add or remove context with the source context
'''
import logging, time

from vesper.data import base
from vesper.data.base import OBJECT_TYPE_LITERAL
from vesper.data.base import OBJECT_TYPE_RESOURCE
from vesper.data.base import RDF_MS_BASE
from vesper.data.base import Statement, isBnode, Triple
from vesper.data.store.basic import MemStore
from vesper.utils import attrdict

log = logging.getLogger("db")

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
        if parts[1].startswithth(ADDCTX):
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
    def __init__(self, txnCtxt, baseRev, newRev):
        self.txnContext = txnCtxt
        self.baseRev = baseRev
        self.currentRev = newRev
        self.adds = {} #original_stmt => (primarystore stmt or none, txn_stmt)
        self.removes = {} #original_stmt => (primarystore stmt or none, txn_stmt)
        
    def rollback(self):
        self.adds = {}
        self.removes = {}

def getTxnContextUri(modelUri, versionnum):
    '''
    Create a transaction context URI from a model uri and revision id
    '''
    return TXNCTX + modelUri + ';' + str(versionnum)

class NamedGraphManager(base.Model):
    #The orginal implementation included ADD and TXN contexts when it stored statements in the primary store, 
    #and so the secondary store only recorded deletions.  While this is more 
    #space and update time efficient (because no extra statements were stored) it requires the primary be a quad store 
    #with support for asQuad = False -- which limits what kinds of stores could be used. 
    #
    #If this functionality was reintroduced then revert old behavior for getStatements and "remove w/ no context" 
    #(which would have to delete matching ADD*contexts and add ORG:contexts)
    #and re-add support for ORG:context (i.e. in getStmtsVisibleAtRevisionForResource)
    
    #: set this to false to suppress the graph manager from adding statements to the store (useful for testing)
    createCtxResource = True 
    markLatest = True
    lastLatest = None
    
    initialRevision = 0
    trunk_id = None
    branch_id = None    
    notifyChangeset = None

    autocommit = property(lambda self: self.managedModel.autocommit,
        lambda self, set:
            setattr(self.managedModel, 'autocommit', set) or
            setattr(self.revisionModel, 'autocommit', set)
        )
   
    updateAdvisory = property(lambda self: self.managedModel.updateAdvisory
                                        and self.revisionModel.updateAdvisory)
    
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
        self._setCurrentVersion(lastScope)
        self._currentTxn = None

    def _setCurrentVersion(self, lastScope):
        if lastScope:
            parts = lastScope.split(';')
            assert parts[1]
            #context urls will always start with a txn context,
            #with the version after the first ;
            self.currentVersion = parts[1]
        else:
            self.currentVersion = self._increment('')
        
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
            if context is None and self.revisionModel is self.managedModel:
                stmts = self.managedModel.getStatements(subject, predicate, object,
                    objecttype, context, asQuad)                
                #using single model and searching across all contexts, 
                #so we need to filter out TXN contexts  
                stmts = filter(lambda s: self.isContextForPrimaryStore(s.scope), stmts)
                if not asQuad or hints:
                    stmts.sort()
                    stmts = base.removeDupStatementsFromSortedList(stmts, 
                        asQuad, **hints)
            else:
                stmts = self.managedModel.getStatements(subject, predicate, object,
                    objecttype, context, asQuad, hints)                
            return stmts
        else:
            if self.isContextForPrimaryStore(context):
                model = self.managedModel
            else:
                if isTransactionContext(context):
                    model = self.revisionModel
                else:
                    #note: this can be expensive!
                    model = MemStore(self.getStatementsForContextAndRevision(context))
                    context = None

            return model.getStatements(subject, predicate, object,
                objecttype, context, asQuad, **hints)

    def getCurrentContextUri(self):
        return getTxnContextUri(self.modelUri, self.currentVersion)
         
    @property
    def currentTxn(self):
        '''
        Return the current transaction. Creates a new transaction if there 
        currently isn't one, incrementing the revision.
        '''
        if not self._currentTxn:
            self.initializeTxn()
        return self._currentTxn
    
    def getTxnContext(self):
        '''
        Return the context revision URI for the current transaction. Creates a 
        new transaction if there currently isn't one, incrementing the revision.
        '''
        return self.currentTxn.txnContext

    def _increment(self, rev):
        if not rev:
            return self.initialRevision
        return int(rev) + 1
    
    def incrementTxnContext(self):
        '''
        Increment the revision number. If no previous version exists 
        (for example, a new database) the initial revision number will be 0.

        Returns the transaction context URI for the new revision.
        '''
        if self.markLatest:
            oldLatest = self.revisionModel.getStatements(predicate=CTX_NS + 'latest')            
            if oldLatest:
                self.lastLatest = oldLatest[0]
                
                latest = oldLatest[0].object
                #lastScope = oldLatest[0].subject
                #context urls will always start with a txn context,
                #with the version after the first ;                
                #parts = lastScope.split(';')
                #latest = parts[1]
                
                return self._increment(latest)
            else:
                self.lastLatest = None
                return self._increment(self.currentVersion)
        else:
            return self._increment(self.currentVersion)
    
    def isContextForPrimaryStore(self, context):
        '''
        Returns true if the context URI is for the primary model
        By default, contexts that start with "context:" are excluded from
        the primary model.

        :context: the context URI or an empty string
        '''
        return not context.startswith('context:')
    
    def isContextReflectedInPrimaryStore(self, context):
        '''
        Contexts managed by the version store (i.e. contexts for which
        `isContextForPrimaryStore()` is false) are by default reflected with
        statements added and removed from the primary store.

        You can overriding this method to exclude particular contexts that
        shouldn't have this behavior.

        Note that if the behavior of this method changes and impacts which
        statements should or should not be in the primary store, existing
        instances of the store may need to be updated.
        '''
        return True

    def _addPrimaryStoreStatement(self, srcstmt):
        scope = srcstmt.scope
        if not self.isContextForPrimaryStore(scope):
            if isTransactionContext(scope):
                raise RuntimeError("can't directly add scope: "+scope)
            if self.isContextReflectedInPrimaryStore(scope):
                stmt = Statement(scope='', *srcstmt[:4])
                if self.managedModel.getStatements(*stmt):
                    #already exists, so don't re-add
                    stmt = None
            else: #dont add to primary store
                stmt = None
        else:
            stmt = srcstmt
        if stmt:
            self.managedModel.addStatement(stmt)
        return stmt
        
    def addStatement(self, srcstmt):
        '''
        Add the statement to the primary and revision models.
        If the statement has a context that is managed by the revision model
        then store it in the primary model without a context (but first make sure
        the statement with the empty context isn't already in the primary model).
        '''
        srcstmt = Statement(*srcstmt) #make sure it's an unmutable statement
        if self.revertRemoveIfNecessary(srcstmt):
            return #the add just reverts a remove, so we're done
        
        stmt = self._addPrimaryStoreStatement(srcstmt)
        
        currentTxn = self.currentTxn
        txnCtxt = currentTxn.txnContext
        addContext = ADDCTX + txnCtxt + ';;' + srcstmt.scope                    
        newstmt = Statement(scope=addContext, *srcstmt[:4])
        self.revisionModel.addStatement(newstmt)

        currentTxn.adds[srcstmt] = (stmt, newstmt)

    def _removePrimaryStoreStatement(self, srcstmt):
        removeStmt = None
        if self.isContextForPrimaryStore(srcstmt.scope):
            removeStmt = srcstmt
        elif self.isContextReflectedInPrimaryStore(srcstmt.scope):
            if isTransactionContext(srcstmt.scope):
                raise RuntimeError("can't directly remove with scope: "+srcstmt.scope)

            #if the scope isn't intended to stored in the primary store
            #(but is intended to be reflected in the primary store)
            #we assume there might be multiple statements with different scopes
            #that map to the triple in the primary store
            #so search the revisionmodel for live adds, if there's only one,
            #remove the statement from the primary store
            stmts = self.revisionModel.getStatements(*srcstmt[:4])
            stmts.sort(key=self.getTransactionVersion)
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
        if removeStmt:
            self.managedModel.removeStatement(removeStmt)
        return removeStmt
                
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

        removeStmt = self._removePrimaryStoreStatement(srcstmt)
        
        #record deletion in history store
        txnContext = self.getTxnContext()
        delContext = DELCTX + txnContext + ';;' + srcstmt.scope
        delStmt = Statement(scope=delContext, *srcstmt[:4])
        self.revisionModel.addStatement(delStmt)
        self.currentTxn.removes[srcstmt] = (removeStmt, delStmt)

    def sendChangeset(self, ctxStmts):        
        if not self.notifyChangeset:
            return        
        assert self._currentTxn
        if self.createCtxResource:
            #see _createCtxResource()
            txnContext = self.getTxnContext()
            assert txnContext            
            ctxStmts.append(
                Statement(txnContext, RDF_MS_BASE + 'type',
                CTX_NS + 'TransactionContext', OBJECT_TYPE_RESOURCE, txnContext)
            )
        
        txn = self.currentTxn
        for stmt, revStmt in txn.adds.values():
            ctxStmts.append(revStmt)
        for stmt, revStmt in txn.removes.values():
            ctxStmts.append(revStmt)
        
        #XXX do we need to handle StatementWithOrder specially?
        changeset = attrdict(revision = txn.currentRev, 
                baserevision=txn.baseRev, timestamp=txn.timestamp, 
                origin=self.branch_id , statements = ctxStmts)
        try:
            self.notifyChangeset(changeset)
        except:
            #we're in the middle of commit() so its important not propagate this
            log.error("notifyChangeset raised an exception", exc_info=True)
    
    def createTxnTimestamp(self):        
        return time.time()
            
    def commit(self, ** kw):
        #assert self._currentTxn
        if not self._currentTxn:
            log.debug("no txn in commit, not committing")
            return None #no txn in commit, model wasn't modified

        ctxStmts = self._finishCtxResource()
        if self.revisionModel != self.managedModel:
            self.revisionModel.commit( ** kw)     
        #commit the transaction
        self.managedModel.commit(** kw)
        self._finalizeCommit(ctxStmts)
        return ctxStmts

    def _finalizeCommit(self, ctxStmts): 
        assert self._currentTxn
        self.currentVersion = self.currentTxn.currentRev
        #if successful, broadcast changeeset
        self.sendChangeset(ctxStmts)    
        self._currentTxn = None

    def initializeTxn(self):
        #increment version and set new transaction and context
        newRev= self.incrementTxnContext()
        ctxUri = getTxnContextUri(self.modelUri, newRev)
        assert ctxUri
        self._currentTxn = CurrentTxN(ctxUri, self.currentVersion, newRev)
        self._createCtxResource()

    def _createCtxResource(self):
        '''create a new context resource'''
        
        assert self._currentTxn
        txnContext = self._currentTxn.txnContext
        assert txnContext

        if self.createCtxResource:
            self.revisionModel.addStatement(
                Statement(txnContext, RDF_MS_BASE + 'type',
                CTX_NS + 'TransactionContext', OBJECT_TYPE_RESOURCE, txnContext)
            )
        
        self._markLatest(self._currentTxn.currentRev)

    def _markLatest(self, txnRev):
        if self.markLatest:
            if self.lastLatest:
                self.revisionModel.removeStatement(self.lastLatest)
            #assert self.currentVersion, 'currentVersion not set'
            txnContext = getTxnContextUri(self.modelUri, txnRev)
            self.lastLatest = Statement(txnContext, CTX_NS + 'latest',
                    unicode(txnRev), OBJECT_TYPE_LITERAL, txnContext)
            self.revisionModel.addStatement(self.lastLatest)

    def _finishCtxResource(self):
        assert self._currentTxn
        self.currentTxn.timestamp = self.createTxnTimestamp()
        
        if not self.createCtxResource:
            return
        
        txnContext = self._currentTxn.txnContext
        assert txnContext

        ctxStmts = []

        ctxStmts.append(Statement(txnContext, CTX_NS+'baseRevision',
            unicode(self._currentTxn.baseRev), OBJECT_TYPE_LITERAL, txnContext))
        ctxStmts.append(Statement(txnContext, CTX_NS+'hasRevision',
            unicode(self._currentTxn.currentRev), OBJECT_TYPE_LITERAL, txnContext))        
        ctxStmts.append(Statement(txnContext, CTX_NS+'createdOn',
            unicode(self._currentTxn.timestamp), OBJECT_TYPE_LITERAL, txnContext))
        
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
                scope, OBJECT_TYPE_RESOURCE, txnContext))

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
                scope, OBJECT_TYPE_RESOURCE, txnContext))

        self.revisionModel.addStatements(ctxStmts)
        return ctxStmts

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

    @staticmethod
    def getTransactionVersion(contexturi):
        ctxUri1 = getTransactionContext(contexturi)
        return ctxUri and int(ctxUri.split(';')[1]) or 0

    @staticmethod
    def comparecontextversion(ctxUri1, ctxUri2):    
        #assert not ctxUri1 or ctxUri1.startswith(TXNCTX),(
        #    ctxUri1 + " doesn't look like a txn context URI")
        #assert not ctxUri2 or ctxUri2.startswith(TXNCTX),(
        #    ctxUri2 + " doesn't look like a txn context URI")
        assert not ctxUri2 or len(ctxUri1.split(';')) > 1, ctxUri1 + " doesn't look like a txn context URI"
        assert not ctxUri2 or len(ctxUri2.split(';')) > 1, ctxUri2 + " doesn't look like a txn context URI"

        return cmp(ctxUri1 and int(ctxUri1.split(';')[1]) or 0,
                   ctxUri2 and int(ctxUri2.split(';')[1]) or 0)
        
    ###### revision querying methods #############
        
    def isModifiedAfter(self, contextUri, resources, excludeCurrent=True):
        '''
        Given a list of resources, return a list of the ones that were modified
        after the given context.
        '''    
        currentContextUri = self.getCurrentContextUri()        
        contexts = [] 
        for resUri in resources:
            rescontexts = self.getRevisionContextsForResource(resUri)
            if rescontexts:
                latestContext = rescontexts.pop()
                if excludeCurrent:              
                    #don't include the current context in the comparison
                    cmpcurrent = self.comparecontextversion(currentContextUri, 
                        latestContext)
                    assert cmpcurrent >= 0, ('context created after current context!? %s > %s', currentContextUri, 
                            latestContext)
                    if cmpcurrent == 0:
                        if not rescontexts:
                            continue
                        latestContext = rescontexts.pop()
                contexts.append((latestContext, resUri))
        if not contexts:
            return []
        contexts.sort(lambda x, y: self.comparecontextversion(x[0], y[0]))
        #include resources that were modified after the given context
        return [resUri for latestContext, resUri in contexts
            if self.comparecontextversion(contextUri, latestContext) < 0]

    def getRevisionContextsForResource(self, resourceuri, stmts=None):
        '''
        return a list of transaction contexts that modified the given resource,
        sorted by revision order
        '''
        if stmts is None:
            stmts = self.revisionModel.getStatements(subject=resourceuri)
        contexts = set(filter(None,
            [getTransactionContext(s.scope) for s in stmts]))
        contexts = list(contexts)
        contexts.sort(key=self.getTransactionVersion)
        return contexts

    def getStatementsForResourceVisibleAtRevision(self, subjectUri, revision):
        '''
        Return the statements visible at the given revision 
        for the specified resource.
        
        revision: 0-based revision number
        '''
        stmts = self.revisionModel.getStatements(subject=subjectUri)
        contexts = self.getRevisionContextsForResource(subjectUri, stmts)
        rev2Context = dict([(x[1], x[0]) for x in enumerate(contexts)])
        #only include transactional statements
        stmts = [s for s in stmts if (getTransactionContext(s.scope) and
            rev2Context[getTransactionContext(s.scope)] <= revision)]
        stmts.sort(cmp=lambda x, y: self.comparecontextversion(x.scope, y.scope))
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
            object=srcContext, objecttype=OBJECT_TYPE_RESOURCE,
            predicate='http://rx4rdf.sf.net/ns/archive#applies-to')]
    
        #get a unique set of transaction context uris, sorted by revision order
        txncontexts.sort(key=self.getTransactionVersion)
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
            
def getTransactionContext(contexturi):
   txnpart = contexturi.split(';;')[0]
   index = txnpart.find(TXNCTX)
   if index < 0:
       return '' #not a txn context (e.g. empty or context:application, etc.)
   return txnpart[index:]

def defaultMergeFollow(resource, add):
   if isBnode(resource):
       #XXX optimize by extracting owner
       owner = None#trytoextractowner(resource)
       if owner:
           add(owner)
           return (), [None]

       #follow all parents and children
       return [None], [None]
   else:
       #dont follow anything
       return (),()

class MergeableGraphManager(NamedGraphManager):
    '''
    Manage a store so that the store can be replicated and changes to the replicants
    (branches) can be merged back into the root or with other replicants.
    
    MergeableGraphManager associates the state of each branch with a revision string 
    that can be compared to another branch's revision string to determine if that 
    branch incorporates all the changes of the first branch (the "precedes(R1,R2)" operator)
    This string has the format `("branchid"."revisionnum",")+`, ordered by branchid.
    (For example "A1,B1")
    
    If a revision string has a revisionnum greater than revisionnum of the same branchid 
    on another revision string then the first revision has all changes made on that branch.
    (the "partialprecede(branchid, R1, R2)" operator)
    
    If branchid and revisionnum are sufficiently left padded then if precedes(R1, R2)
    is true then lexicographically R1 < R2 assuming that (1) R1, R2 at least share a 
    trunk branchid and (2) branchids are lexicographically greater than the trunk branchid.
    These assumptions are made by MergeableGraphManager.
    
    It follows that if R1 < R2 lexicographically then at least
    partialprecede(branchid, R1, R2) where branchid is the trunk but not necessarily for any 
    other branch id. (E.g. "A1B2C3" < "A1C2" even though partialprecede("C", "A1B1C3", A1C2") is not true.
    This could happen if a branch at "A1B1" pulled in "A1C3" changes resulting in "A1B2C3".) 
    If the revision strings contains the exact same branchids, then lexicographic comparison
    is equivalent to `precedes`.
    
    When a branch merges with (pull in changes from) another branch updates its revision string with 
    the max revisionnum for each branchid (and adding new branchid if not present) and also increments
    its own revisionnum. 
    
    If a branch is into merged back into the trunk we can drop that branch id
    ("retire" it) as long as the branch no longer changes because the new trunk revision
    will lexicographically compare greater than any prior revision that has that branch id.
    But if non-trunk branch dropped another non-trunk branch after a merge we 
    no longer can compare revisions of the retired branch that didn't contain the branch
    that dropped it. (For example, A1C1 merges A1B1 => A1B1C2. A1B1 < A1B1C2 but we couldn't know that 
    if B1 was dropped leaving A1C2.)
    
    As lexigraphic order is only signficant between the subset of the revisions 
    strings that share branchids, a heuristic for displaying revision order would be 
    to partition by significant order and order the insignificant revisions by the 
    revision creation timestamp.
        
    In summary, for revision string R1 and R2, if R1 >= R2 then it can't R1 precede R2.
    R2 may precede R1 and at least R2 partialprecedes R1 on the trunk branch.
    '''
    
    initialRevision = '0'
    maxPendingQueueSize = 1000
    pendingQueue = {} 

    def __init__(self, primaryModel, revisionModel, modelUri, lastScope=None, trunk_id='0A', branch_id=None):
        self.trunk_id = trunk_id.rjust(2, '0')
        if branch_id:        
            self.branch_id = branch_id.rjust(2, '0')
        else:
            self.branch_id = trunk_id
        super(MergeableGraphManager, self).__init__(primaryModel, revisionModel, modelUri, lastScope)

    @staticmethod
    def getTransactionVersion(contexturi):
        ctxUri = getTransactionContext(contexturi)
        return ctxUri and ctxUri.split(';')[1] or ''

    @staticmethod
    def comparecontextversion(ctxUri1, ctxUri2):    
        #assert not ctxUri1 or ctxUri1.startswith(TXNCTX),(
        #    ctxUri1 + " doesn't look like a txn context URI")
        #assert not ctxUri2 or ctxUri2.startswith(TXNCTX),(
        #    ctxUri2 + " doesn't look like a txn context URI")
        assert not ctxUri2 or len(ctxUri1.split(';')) > 1, (
                ctxUri1 + " doesn't look like a txn context URI")
        assert not ctxUri2 or len(ctxUri2.split(';')) > 1, (
                ctxUri2 + " doesn't look like a txn context URI")

        return cmp(ctxUri1 and ctxUri1.split(';')[1] or '',
                   ctxUri2 and ctxUri2.split(';')[1] or '')

    @staticmethod
    def getBranchRev(rev,branchid):
        '''
        Return the revision number for the given branch of the given revision string
        '''
        for node in rev.split(','):
            if node.startswith(branchid):
                return node[len(branchid):]

    def _increment(self, rev):
        if not rev:
            return self.initialRevision
        
        if self.branch_id not in rev:
            #create new branch            
            if [node for node in rev.split(',') if node > self.branch_id]:
                 raise RuntimeError(
                    'branch_id %s precedes current branches in rev %s' % 
                                                    (self.branch_id, rev))
            if rev == self.initialRevision:
                return self.branch_id + '00001'            
            return rev + ',' + self.branch_id + '00001'

        def incLocalBranch(node):
            if node.startswith(self.branch_id):
                start = len(self.branch_id)
                inc = int(node[start:])+1
                return self.branch_id + '%05d' % inc
            else:
                return node
        return ','.join([incLocalBranch(node) for node in rev.split(',')])
        
    def mergeVersions(self, rev1, rev2):
        nodes = {}
        branchLen = len(self.branch_id)
        for branchRev in (rev1+','+rev2).split(','):
            branch_id = branchRev[:branchLen]
            branchNum = branchRev[branchLen:]
            num = nodes.get(branch_id)
            if not num or num < branchNum:
                nodes[branch_id] = branchNum
        return ','.join( sorted([k+v for (k,v) in nodes.items()]) )
        
    def _addChangesetStatements(self, changeset, setrevision):
        '''
        Directly adds changeset statements to the revision store
        and adds and removes statements from the primary store.
        '''
        assert not self._currentTxn
        for s in changeset.statements:
            scope = s[4]
            if scope.startswith(ADDCTX):
                originalscope = splitContext(scope)[EXTRACTCTX]
                orgstmt = Statement(scope=originalscope, *s[:4])
                self._addPrimaryStoreStatement(orgstmt)
            elif scope.startswith(DELCTX):
                originalscope = splitContext(scope)[EXTRACTCTX]
                orgstmt = Statement(scope=originalscope, *s[:4])
                self._removePrimaryStoreStatement(orgstmt)
            self.revisionModel.addStatement(s)
        
        if self.currentVersion == self.initialRevision:
            #just take the changesets revision
            assert changeset.baserevision == self.initialRevision
            if changeset.origin not in (self.trunk_id, self.branch_id):
                #we can't allow this since we need changesets to at least 
                #share the trunk revision
                raise RuntimeError("merging a changeset into an empty store "
                    "but neither its trunkid %s not its branchid %s "
                    "match the changeset's branchid: %s" %
                            (self.trunk_id, self.branch_id, changeset.origin))
        else:
            branchidlen = len(self.branch_id)
            if (changeset.revision[:branchidlen] != 
                    self.currentVersion[:branchidlen]):
                raise RuntimeError(
                'can not add a changeset that does not share trunk branch: %s, %s'
                                    % (self.currentVersion, changeset.revision))        
        if setrevision:
            self.currentVersion = changeset.revision        
            self._markLatest(self.currentVersion)

    def _createMergeChangeset(self, otherrev, adds=(), removes=()):
        '''
        Creates an merge changeset 
        '''
        txnContext = self.getTxnContext()
        self.revisionModel.addStatement(Statement(txnContext, CTX_NS+'baseRevision',
            unicode(otherrev), OBJECT_TYPE_LITERAL, txnContext))        
        if adds: self.addStatements(adds)
        if removes: self.removeStatements(removes)
        #use self.currentTxn.currentRev for the incremented version
        self.currentVersion = self.mergeVersions(self.currentTxn.currentRev, otherrev)
        self._markLatest(self.currentVersion)
        self.commit()

    def findMergeClosures(self, resources, followFunc=None):
        '''
        For each given resource find related resources that should be considered
        when determining if 
        
        Default policy: if resource is a bnode add ancestor and descendant resources 
        to the closure and recursively follow as long as the resource is a bnode.
        '''
        #handle cases like: 
        #base has an object like { prop : [{id : bnode:a},{id : bnode:b}] }
        #and local changes bnode:a and remote changes bnode:b
        #we need to detect a conflict
        
        followFunc = followFunc or defaultMergeFollow
        
        closures = []
        for r in resources:
            closure = set([r])
            todo = set([r])
            def add(res):
                if res not in closure:
                    todo.add(res)
                    closure.add(res)
            
            while todo:            
                r = todo.pop()            
                childPreds, parentPreds = followFunc(r,add)
                #add children to resources
                for pred in childPreds:
                    for s in self.getStatements(subject=r, predicate=pred):
                        if s.objectType == OBJECT_TYPE_RESOURCE:
                            add(s.object)
                #add parents
                for pred in parentPreds:
                    for s in self.getStatements(object=r, predicate=pred, 
                                            objecttype=OBJECT_TYPE_RESOURCE):
                        add(s.subject)
            closures.append(closure)

        return closures
    
    
    def resolveConflicts(self, conflicts, remotechanges, localchanges):
        '''
        stategies: error, create special merge context, discard later, later wins
        
        take earlier, take later, take both, exclude both
        
        "level" to apply:
         
        all changes 
        "merge group"
        resource with bnodes
        resource
        property with lists
        property item (statement)
        '''
        
        #default strategy:
        # take the later version of the resource        
        for (resources, local, remote) in conflicts:            
            pass
        #changes different from just committing the incoming changeset
        
        #issue -- order of merge 
        #node A commit a1 already, node B commited b1 first
        #node merges b1 and send a2b2 with resolved changes
        #that resolution has to be the exact same as what node B would have done 
        #if it received and resolved a1     
        return adds, removes
    
    def merge(self, changeset):
        '''
        Merge an external changeset into the local store. If the local store doesn't
        have the base revision of the changeset it is add to the pending queue.
        If no changes have been made to the local store after the base revision
        then the changeset is added to store with no modification. But if changes
        have been made, these changes are merged with the external changeset.
        If the merge succeeds, the changeset is added, followed by a merge
        changeset.
        '''
        if self._currentTxn:
            raise RuntimeError('cannot merge while transaction in progress')
        if not isinstance(changeset, attrdict):
            changeset = attrdict(changeset)

        if changeset.origin == self.branch_id:
            #handle receiving own changeset
            raise RuntimeError("merge received changeset from itself: " + self.branch_id)

        if changeset.baserevision != self.initialRevision and not list(
                                    self.getRevisions(changeset.baserevision)):
            if changeset.baserevision in self.pendingQueue:
                assert changeset != self.pendingQueue[changeset.baserevision], changeset
                self.merge(self.pendingQueue[changeset.baserevision])
            elif len(self.pendingQueue) < self.maxPendingQueueSize:
                #xxx queue should be persistent because we're SOL if 
                #we miss a changeset
                self.pendingQueue[changeset.baserevision] = changeset            
                log.warning(
                  'adding to pending queue because base revision "%s" is missing'
                                                    , changeset.baserevision)
                return False
            else:
                raise RuntimeError(
                'can not perform merge because base revision "%s" is missing' 
                                                    % changeset.baserevision)

        if not changeset.baserevision or changeset.baserevision == self.currentVersion:
            #just add changeset, no need for merge changeset
            self._addChangesetStatements(changeset, True)
            return True
        
        remoteResources = set(s[0] for s in changeset.statements)                
        remoteResources.update(set(s[2] for s in changeset.statements 
                                        if s[3] == OBJECT_TYPE_RESOURCE))
        
        localResources = self.getChangedResourcesAfterBranchRevision(
                    changeset.baserevision, changeset.origin)     
        closures = self.findMergeClosures(localResources | remoteResources)
        
        conflicts = []
        for closure in closures:
            #its a conflict if an object in the closure appears in both the 
            #remote and local changed resources            
            remote = closure & remoteResources
            local = closure & localResources
            if remote and local:
                #XXX only conflict if remote and local changes are different
                conflicts.append( (closure, local, remote) )
                        
        if conflicts:
            assert False, 'merging conflicts not yet implemented! %s current %s %s' % (
                                changeset.baserevision, self.currentVersion, conflicts)
            self.resolveConflicts(conflicts)
            #stategies: error, create special merge context, discard later, later wins
        else:
            #no conflicts just add changeset
            self._addChangesetStatements(changeset, False)
            self._createMergeChangeset(changeset.revision)
        
        return True

    def getStatementsAfterBranchRevision(self, baserev, branchid):
        '''
        Return all the changes made after the given branch revision, 
        but don't include statements that were added and then removed
        or removed and then re-added.
        
        Returns a pair of sets of statements of added and removed statements, 
        respectively.
        '''
        addStmts = set()
        removeStmts = set()
        branchrev = self.getBranchRev(baserev,branchid)
        for rev in sorted(self.findBranchRevisions(branchid, branchrev,baserev)):
            revTxnUri = getTxnContextUri(self.modelUri, rev)
            for txnStmt in self.revisionModel.getStatements(context=revTxnUri):
                if txnStmt.predicate not in [CTX_NS + 'includes',CTX_NS + 'excludes']:
                    continue
                isAdd = txnStmt.predicate == CTX_NS + 'includes'
                ctxStmts = set([Triple(*s) for s in self.revisionModel.getStatements(
                                                context=txnStmt.object)])
                if isAdd:
                    addStmts |= ctxStmts
                    removeStmts -= ctxStmts
                else:
                    addStmts -= ctxStmts
                    removeStmts |= ctxStmts
        return addStmts, removeStmts
        
    def getChangedResourcesAfterBranchRevision(self, baserev,branchid):
        '''
        Return the set of resources that had changes made to it after
        the given branch revision. 
        
        Any resource that appears as a subject in an added
        or removed statement will be in the set.
        '''
        addStmts, removeStmts = self.getStatementsAfterBranchRevision(
                                                baserev,branchid)
        resources = set()
        import itertools
        for s in itertools.chain(addStmts, removeStmts):
            resources.add(s.subject)
            if s.objectType == OBJECT_TYPE_RESOURCE:
                resources.add(s.object)
        return resources

    def getRevisions(self, revision=None):
        '''
        Return all the revisions strings in the model unless `revision` is 
        specified, in that case yield that revision if it exists in the model.
        '''
        for s in self.revisionModel.getStatements(predicate=CTX_NS+'hasRevision', object=revision):
            if s.subject.startswith(TXNCTX) and s.subject.startswith(s.scope):
                yield s.object
        
    def findBranchRevisions(self, branchid, branchrev, baserev=''):
        '''
        Return all the revisions that follow the branch rev (inclusive), 
        starting with `baserev`.
        '''
        #can a changeset > rev not include branch? 
        #yes: e.g. node c: has rev = b2c3 and changeset from node b = b3c2 
        #but changeset < rev is not possible assuming node
        #has all branchids where branchid < node's branchid
        #and the local store has all base changesets
        #Is this a valid assumption?
        #it is, if 1) we never "retire" branches and 
        #2) the new branches start with the current state of trunk
        #3) branchids automically increment from the trunk
        #4) new branches update the trunk at branch-time so that 
        #subsequent branches have their initial state

        #note: we could optimize the case where if baserev not specified with 
        #a leftsideCache so baserev = leftsideCache[branchRevision]
        #where baserev would be the first rev with branchRevision 
        #e.g. C3 => A2B1 if the first rev with C3 A2B1C3        
        
        for rev in self.getRevisions():
            if rev > baserev and self.getBranchRev(rev, branchid) >= branchrev:
                yield rev

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

