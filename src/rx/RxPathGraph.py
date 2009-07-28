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
        
    def rollback(self):
        self.adds = {}
        self.removes = {}

class _StatementWithRemoveForContextMarker(Statement):
    removeForContext = True

def getTxnContextUri(modelUri, versionnum):
    return TXNCTX + modelUri + ';' + str(versionnum)

class NamedGraphManager(RxPath.Model):
    '''           
    split store mode:
    
    primary: store containing current state of data unmodified     
    only needs to support data as used by application 
    (e.g. only needs to be a triple-store if the application 
    doesn't use contexts)
        
    secondary: contains version history, must support arbitrary quads
    
    get: query from primary store unless context specified
    
    add, no context: add with '', add with TXN
    
    w/context: add with '', add with ADD*context 
    
    remove w/ no context: remove with '', add statement w/DEL3

    remove w/ context: find stmts, if no others: remove with '', add statement DELGC ?? 
                       add statement w/DEL4

    
    single store: 
    
    operations with no context specified:
    =======  =================                ===========
    op       no context                       context
    =======  =================                ============    
    get      filter results                   get from primary store
             by txn context and
             strip context from result  
    add      add with TXN context             add unchanged to primary and      
                                              add with TXN context to second
    remove   1) find matches across contexts, same as one store, with step 1 
             for each matching statement,     on second, step 2 on primary 
             add statement using              and step 3 on second
             ORG*context*DEL3 
             2) remove given statement  
             3) remove all matching statements 

    operations with context specified:
    =======  =================               ===========
    op       one store                       split store
    =======  =================               ===========    
    get      get from store                  get from second store
    add      add with ADD*context            primary: add unchanged
                                             second: add with ADD*context
    remove   remove with ADD*context         primary: remove unchanged              
             --------------------------------------------
             for both, but in second store if split:
             find ADD*context (extract from ORG+ADD if necessary)
             add the statement w/ORG+ADD*context+DEL4
             add the statement w/DEL4*context
    
    XXX remove orginal statement from first    
    XXX context resource should only be written to delmodel    
    '''
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

    autocommit = property(lambda self: self.managedModel.autocommit,
                 lambda self, set: 
                   setattr(self.managedModel, 'autocommit', set) or
                   setattr(self.delmodel, 'autocommit', set)
                 )
    
    def __init__(self, addmodel, delmodel, modelUri, lastScope=None):        
        if not delmodel:
            #don't use a separate store for version history
            self.delmodel = addmodel
            self.managedModel =addmodel
        else:
            self.delmodel = delmodel
            self.managedModel = addmodel
        
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
        
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):      
        hints = hints or {}

        if not context:
            stmts = self.managedModel.getStatements(subject, predicate, object,
                                            objecttype,context,asQuad, **hints)
            if context is None and self.delmodel is self.managedModel:
                #using single model and searching across all contexts, 
                #so we need to filter out TXN contexts  
                stmts = filter(lambda s: self.isContextForPrimaryStore(s.scope), stmts)
                if not asQuad or hints:
                    stmts.sort()
                    stmts =  RxPath.removeDupStatementsFromSortedList(stmts, 
                                                               asQuad, **hints)
            return stmts
        else:
            if self.isContextForPrimaryStore(context):
                model = self.managedModel
            else:
                #XXX find all TXN contexts with this context and reconstruct
                #the current state
                raise RuntimeException(
                        'searching by non-primary contexts not yet supported')
                model = self.delModel
                
            return model.getStatements(subject, predicate, object,
                                            objecttype,context, asQuad, **hints)
         
    @property
    def currentTxn(self):
        if not self._currentTxn:
            self.initializeTxn()
        return self._currentTxn
    
    def getTxnContext(self):
        return self.currentTxn.txnContext
    
    def incrementTxnContext(self):
        if self.markLatest:
            oldLatest = self.getStatements(predicate=CTX_NS+'latest')
            if oldLatest:
                lastScope = oldLatest[0].subject
                parts = lastScope.split(';')
                #context urls will always start with a txn context,
                #with the version after the first ;
                #assert int(parts[1])                
                self.lastVersion = int(parts[1])+1     
                self.managedModel.removeStatement(oldLatest[0])
        else:
            self.lastVersion += 1

        return getTxnContextUri(self.modelUri, self.lastVersion)        

    def isContextForPrimaryStore(self, context):
        return not scope.startswith('context:')

    def addStatement(self, srcstmt):
        srcstmt = Statement(*srcstmt) #make sure it's an unmutable statement
        if self.revertRemoveIfNecessary(srcstmt):
            return #the add just reverts a remove, so we're done

        scope = srcstmt.scope
        if not self.isContextForPrimaryStore(scope):
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
        newstmt = Statement(scope=addContext, *triple[:4])
        self.delModel.addStatement(newstmt)

        currentTxn.adds[ srcsrc ] = (stmt, newstmt)
        
    def removeStatement(self, srcstmt):
        srcstmt = Statement(*srcstmt) #make sure its an unmutable statement
        if self.revertAddIfNecessary(srcstmt):
            return

        removeStmt = None
        if self.isContextForPrimaryStore(srcstmt.scope):
            removeStmt = srcstmt
        else:                
            #if the scope isn't intended for the primary store
            #we assume there might be multiple statements with different scopes
            #that map to the triple in the primary store
            #so search the delmodel for live adds, if there's only one,
            #remove the statement from the primary store
            stmts = self.delmodel.getStatements(*srcstmt[:4])
            stmts.sort(key=comparecontextversion)
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
                    if self.delmodel is not self.managedModel:
                        #should only encounter this in single model mode 
                        raise RuntimeException('unexpected statement: %s' % s)
            
            if adds == 1: #len(adds) == 1:
                #last one in the primary model, so delete it
                removeStmt = Statement(scope='', *srcstmt[:4])
        
        #record deletion in history store
        txnContext = self.getTxnContext()
        delContext = DELCTX + txnContext+ ';;' + srcstmt.scope
        delStmt = Statement(scope=delContext, *srcstmt[:4])
        self.delModel.addStatement(delStmt)
        if removeStmt:
            self.managedModel.removeStatement(removeStmt)
        self.currentTxn.removes[srcstmt] = (delStmt, removeStmt)

    def commit(self, **kw):
        self._finishCtxResource()
        if self.delmodel != self.managedModel:
            self.delmodel.commit(**kw)
         
        #commit the transaction
        self.managedModel.commit(**kw)
        self._currentTxn = None

    def initializeTxn(self):
        #increment version and set new transaction and context
        self._currentTxn = CurrentTxN(self.incrementTxnContext())   

        if self.createCtxResource:            
            self._createCtxResource()

    def _createCtxResource(self):
        '''create a new context resource'''

        txnContext = self.getTxnContext()
        assert txnContext

        ctxStmts = [
            Statement(txnContext, RDF_MS_BASE+'type',CTX_NS+'TransactionContext',
                      OBJECT_TYPE_RESOURCE, txnContext),
        ]

        if self.markLatest:
            ctxStmts.append(Statement(txnContext, CTX_NS+'latest',
                unicode(self.lastVersion),OBJECT_TYPE_LITERAL, txnContext))

        self.delModel.addStatements(ctxStmts)

    def _finishCtxResource(self):
        txnContext = self.getTxnContext()
        assert txnContext

        def findContexts(changes):
            return set([(key.scope, value[1].scope)
                    for key, value in changes.items()])

        excludeCtxts = findContexts(self.currentTxn.removes)
        for (scope, delContext) in excludeCtxts:
            #add statement declaring the deletion context
            removeCtxStmt = Statement(txnContext, CTX_NS+'excludes',
                   delContext,OBJECT_TYPE_RESOURCE, txnContext)
            ctxStmts.append( removeCtxStmt )            
            ctxStmts.append( Statement(delContext, CTX_NS+'applies-to',
                    scope, OBJECT_TYPE_RESOURCE, delContext) )

        includeCtxts = findContexts(self.currentTxn.adds)
        for (scope, addContext) in includeCtxts:
            #add info about the included context
            ctxStmts.append(
                Statement(txnCtxt, CTX_NS+'includes',
                        addContext, OBJECT_TYPE_RESOURCE, txnCtxt)
            )
            #just infer this from a:includes rdfs:range a:Context
            #Statement(addContext, RDF_MS_BASE+'type',
            #    'http://rx4rdf.sf.net/ns/archive#Context',
            #              OBJECT_TYPE_RESOURCE, addContext),
            ctxStmts.append( Statement(addContext, CTX_NS+'applies-to',
                    scope, OBJECT_TYPE_RESOURCE, addContext) )

        self.delModel.addStatements(ctxStmts)

    def rollback(self):
        self.managedModel.rollback()
        if self.delmodel != self.managedModel:
            self.delmodel.rollback()
        self._currentTxn = None
        #note: we might still have cache 
        #keys referencing this version (transaction id)  
 
    def revertAddIfNecessary(self, stmt):
        add = self.currentTxn.adds.pop(stmt, None)
        if add:
            (stmt, newstmt) = add
            if stmt:
                self.managedModel.removeStatement(stmt)
            self.delModel.removeStatement(newstmt)
            return True

        return False

    def revertRemoveIfNecessary(self, stmt):
        remove = self.currentTxn.removes.pop(stmt, None)
        if remove:
            (stmt, newstmt) = remove
            if stmt:
                self.managedModel.addStatement(stmt)
            self.delModel.removeStatement(newstmt)
            return True

        return False

    ###### revision querying methods #############
        
    def isModifiedAfter(self, contextUri, resources, excludeCurrent = True):
        '''
        given a list of resources, return list a of the ones that were modified
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
        contexts.sort(lambda x, y: comparecontextversion(x[0],y[0]))
        #include resources that were modified after the given context
        return [resUri for latestContext, resUri in contexts
                  if comparecontextversion(contextUri, latestContext) < 0]

    def getRevisionContexts(self, resourceuri,stmts=None):
        '''
        return a list of transaction contexts that modified the given resource,
        sorted by revision order
        '''
        if stmts is None:
            stmts = self.delmodel.getStatements(subject=resourceuri)
        contexts = set(filter(None,
                [getTransactionContext(s.scope) for s in stmts)] ))
        contexts = list(contexts)
        contexts.sort(comparecontextversion)
        return contexts

    def getRevisionStmts(self, subjectUri, revision):
        '''
        Return the statements visible at the given revision 
        for the specified resource.
        
        revision: 0-based revision number
        '''
        stmts = self.delmodel.getStatements(subject=resourceuri)
        contexts = self.getRevisionContexts(subjectUri, stmts)
        rev2Context = dict( [ (x[1],x[0]) for x in enumerate(contexts)] )

        #only include transactional statements
        stmts = [s for s in stmts if (getTransactionContext(s.scope) and
                    rev2Context[getTransactionContext(s.scope)] <= revision)]
        stmts.sort(cmp=lambda x,y: comparecontextversion(x.scope, y.scope))
        revisionstmts = set()
        for s in stmts:
            if s.scope.startswith(DELCTX):
                revisionstmts.discard(s)
            elif s.scope.startswith(ADDCTX):
                revisionstmts.add(s)

        return revisionstmts
    
    def _getContextRevisions(self, srcContext):
        '''
        return a list of transaction contexts that modified the given context
        '''
        model = self.delmodel
    
        #find the txn contexts with changes to the srcContext
        addchangecontexts = [s.subject for s in model.getStatements(
            object=srcContext,
            predicate='http://rx4rdf.sf.net/ns/archive#applies-to')]
        #todo: this is redundent if model and delmodel are the same:    
        delchangecontexts = [s.subject for s in model.getStatements(
            object=srcContext,
            predicate='http://rx4rdf.sf.net/ns/archive#applies-to')]
    
        #get a unique set of transaction context uris, sorted by revision order
        txns = {}
        for ctx in addchangecontexts:
            txns.setdefault(getTransactionContext(ctx), []).append(ctx)
        for ctx in delchangecontexts:
            txns.setdefault(getTransactionContext(ctx), []).append(ctx)
        txncontexts = txns.keys()
        txncontexts.sort(comparecontextversion)
        return txncontexts, txns
      
    def showContextRevision(self, srcContextUri, revision):
        '''
        Return the statements visible at the given revision 
        for the specified context.
        
        revision: 0-based revision number
        '''
        model = self.managedModel
        delmodel = self.delmodel
        
        txncontexts, txns = self._getContextRevisions(srcContextUri)
    
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
    
    def getRevisionContextsForContext(self, srcContextUri):
        '''
        return a list of transaction contexts that modified the given context
        '''
        contexts, txns = self._getContextRevisions(srcContextUri)
    
        contexts.sort(cmp=comparecontextversion)
        return contexts

class DeletionModelCreator(object):
    '''
    This reconstructs the delmodel from add and remove events generated by
    loading a NTriples transaction log (see NTriples2Statements)
    '''
    doUpgrade = False 

    def __init__(self, delmodel):
        self.currRemoves = []
        self.currRemovesForContext = {}
        self.delmodel = delmodel
        self.lastScope = None

    def _upgradeScope(self,scope):
        return scope      
              
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

def getTransactionContext(contexturi):
    txnpart = contexturi.split(';;')[0]
    index = txnpart.find(TXNCTX)
    if index < 0:
        return '' #not a txn context (e.g. empty or context:application, etc.)
    return txnpart[index:]
  
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
