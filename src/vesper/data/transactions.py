#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
#
#Most of the code here is based on PEAK's transactions.py, specifically:
#http://cvs.eby-sarna.com/PEAK/src/peak/storage/transactions.py?rev=1.33
#(also available at http://svn.eby-sarna.com/*checkout*/PEAK/src/peak/storage/transactions.py?rev=1573 )
#Copyright (C) 1996-2004 by Phillip J. Eby and Tyler C. Sarna.
#All rights reserved.  This software may be used under the same terms
#as Zope or Python.  THERE ARE ABSOLUTELY NO WARRANTIES OF ANY KIND.

import time
import os, os.path
import logging

from vesper import pjson, utils

class TransactionError(Exception):
    '''Base transaction error'''

class NotReadyError(TransactionError):
    """One or more transaction participants were unready too many times"""

class TransactionInProgress(TransactionError):
    """Action not permitted while transaction is in progress"""

class OutsideTransaction(TransactionError):
    """Action not permitted while transaction is not in progress"""

class BrokenTransaction(TransactionError):
    """Transaction can't commit, due to participants breaking contracts
       (E.g. by throwing an exception during the commit phase)"""

class BasicTxnErrorHandler(object):
    """Simple error handling policy, w/simple logging, no retries"""

    def voteFailed(self, txnService, participant):
        txnService.log.warning(
            "%s: error during participant vote", txnService, exc_info=True
        )

        # Force txn to abort
        txnService.fail()
        raise

    def commitFailed(self, txnService, participant):
        txnService.log.critical(
            "%s: unrecoverable transaction failure", txnService,
            exc_info=True
        )
        txnService.fail()
        raise

    def abortFailed(self, txnService, participant):
        txnService.log.warning(
            "%s: error during participant abort", txnService,
            exc_info=True
        )
        # ignore the error

    def finishFailed(self, txnService, participant, committed):
        txnService.log.warning(
            "%s: error during participant finishTransaction", txnService,
            exc_info=True
        )
        # ignore the error

class TransactionState(object):
    """Helper object representing a single transaction's state"""

    timestamp    = None
    safeToJoin   = True
    cantCommit   = False
    inCommit     = False
    inAbort      = False
    aborted      = False 
    
    def __init__(self):
       self.participants = []
       self.info = {}

    def addInfo(self, info):
        self.info.update(info)

class TransactionService(object):
    """Basic transaction service component"""

    #state          = binding.Make(TransactionState)
    errorHandler   = BasicTxnErrorHandler() #binding.Make(BasicTxnErrorHandler)
    stateFactory = TransactionState
        
    def __init__(self, loggerName='transactions'):         
        self.state = self.stateFactory()
        self.log = logging.getLogger(loggerName)

    def join(self, participant, readOnly=False):
        if not self.isActive():
            raise OutsideTransaction
        elif self.state.cantCommit:
            raise BrokenTransaction
        elif self.state.safeToJoin:
            if participant not in self.state.participants:
                self.state.participants.append(participant)
                assert not participant.inTransaction
                participant.inTransaction = self
            else:
                assert participant.inTransaction == self
        else:
            raise TransactionInProgress

    def isDirty(self):
        '''return True if any of the transaction participants were modified'''    
        if not self.isActive():
            raise OutsideTransaction

        for p in self.state.participants:
            if p.isDirty(self):
                return True
        return False

    def _prepareToVote(self):

        """Get votes from all participants

        Ask all participants if they're ready to vote, up to N+1 times (where
        N is the number of participants), until all agree they are ready, or
        an exception occurs.  N+1 iterations is sufficient for any acyclic
        structure of cascading data managers.  Any more than that, and either
        there's a cascade cycle or a broken participant is always returning a
        false value from its readyToVote() method.

        Once all participants are ready, ask them all to vote."""

        tries = 0
        unready = True
        state = self.state

        while unready and tries <= len(state.participants):
            unready = [p for p in state.participants if not p.readyToVote(self)]
            tries += 1

        if unready:
            raise NotReadyError(unready)

        self.state.safeToJoin = False

    def _vote(self):
        for p in self.state.participants:
            try:
                p.voteForCommit(self)
            except:
                self.errorHandler.voteFailed(self,p)

    def begin(self, **info):
        if self.isActive():
            raise TransactionInProgress

        self.state = self.stateFactory()
        self.state.timestamp = time.time()
        self.addInfo(**info)

    def commit(self):
        if not self.isActive():
            raise OutsideTransaction

        if self.state.cantCommit:
            raise BrokenTransaction
        
        try:
            self._prepareToVote()
            self._vote()
        except:
            self.abort()
            raise
        
        try:
            self.state.inCommit = True 
            for p in self.state.participants:
                try:
                    p.commitTransaction(self)
                except:
                    self.errorHandler.commitFailed(self,p)
        finally:
            self.state.inCommit = False 
        
        self._cleanup(True) 

    def fail(self):
        if not self.isActive():
            raise OutsideTransaction
        self.state.cantCommit = True
        self.state.safeToJoin = False

    def removeParticipant(self,participant):
        self.state.participants.remove(participant)
        participant.inTransaction = False

    def abort(self):
        if not self.isActive():
            raise OutsideTransaction

        self.fail()

        try:
            self.state.inAbort = True 
            for p in self.state.participants[:]:
                try:
                    p.abortTransaction(self)
                except:
                    self.errorHandler.abortFailed(self,p)
        finally:
            self.state.inAbort = False 
            self.state.aborted = True 

        self._cleanup(False)

    def getTimestamp(self):
        """Return the time that the transaction began, in time.time()
        format, or None if no transaction in progress."""

        return self.state.timestamp

    def addInfo(self, **info):
        if self.state.cantCommit:
            raise BrokenTransaction
        elif self.state.safeToJoin:
            self.state.addInfo(info)
        else:
            raise TransactionInProgress

    def getInfo(self):
        return self.state.info

    def _cleanup(self, committed):
        for p in self.state.participants[:]:
            try:
                p.finishTransaction(self,committed)
            except:
                self.errorHandler.finishFailed(self,p,committed)

        self.state = self.stateFactory()
        
    def isActive(self):
        return self.state.timestamp is not None

    def __contains__(self,ob):
        return ob in self.state.participants

class TransactionParticipant(object):
    inTransaction = False

    def isDirty(self,txnService):
        '''return True if this transaction participant was modified'''    
        return True #default to True if we don't know one way or the other

    def join(self, txnService, readOnly=False):
        return txnService.join(self, readOnly)
    
    def readyToVote(self, txnService):
        return True

    def voteForCommit(self, txnService):
        pass

    def commitTransaction(self, txnService):
        pass

    def abortTransaction(self, txnService):
        pass

    def finishTransaction(self, txnService, committed):
        self.inTransaction = False

class ProcessorTransactionState(TransactionState):
    def __init__(self):
        super(ProcessorTransactionState, self).__init__()        
        self.additions = []
        self.removals = []
        self.newResources = []
        self.kw = {}
        self.retVal = None
        self.lock = None
        self.readOnly = True

    def addInfo(self, info):
        self.kw = info.pop('kw', self.kw)        
        self.retVal = info.pop('retVal', self.retVal)
        self.info.update(info)

class ProcessorTransactionService(TransactionService,utils.ObjectWithThreadLocals):
    stateFactory = ProcessorTransactionState

    def __init__(self, server):        
        self.server = server
        #one transaction context per thread so that transactions are thread-specific
        self.initThreadLocals(state=ProcessorTransactionState())
        super(ProcessorTransactionService, self).__init__(server.log.name)
        
    def newResourceHook(self, uris):
        '''
        This is intended to be set as the DataStore's newResourceTrigger
        '''
        if self.isActive() and self.state.safeToJoin:
            self.state.newResources.extend(uri)
            self._runActions('before-new', {'_newResources' : uris})

    def addHook(self, stmts, jsonrep=None):
        '''
        This is intended to be set as the DataStore's addTrigger
        '''
        state = self.state
        if self.isActive() and state.safeToJoin:            
            state.additions.extend(stmts)
            for stmt in stmts:
                if stmt in state.removals:
                    state.removals.pop( state.removals.index(stmt) )
            if 'before-add' in self.server.actions:
                if jsonrep is None:
                    jsonrep = pjson.tojson(stmts)
                kw = {
                    '_addedStatements' : state.additions,
                    '_added' : jsonrep, 
                    '_newResources' : state.newResources }
                self._runActions('before-add', kw)

    def removeHook(self, stmts, jsonrep=None):
        '''
        This is intended to be set as the DataStore's removeTrigger
        '''
        state = self.state
        if self.isActive() and state.safeToJoin:
            state.removals.extend(stmts)
            for stmt in stmts:
                if stmt in state.additions:
                    state.additions.pop( state.additions.index(stmt) )
            if 'before-remove' in self.server.actions:
                if jsonrep is None:
                    jsonrep = pjson.tojson(stmts)
                kw = {
                    '_removedStatements' : state.removals, 
                    '_removed' : jsonrep, 
                    '_newResources' : state.newResources 
                }
                self._runActions('before-remove', kw)
    
    def _runActions(self, trigger, morekw=None):      
       actions = self.server.actions.get(trigger)       
       if actions:
            state = self.state
            kw = state.kw.copy()
            if morekw is None:                
                morekw = { 
                '_addedStatements' : state.additions,
                '_removedStatements' : state.removals,
                '_added' : pjson.tojson(state.additions),
                '_removed' : pjson.tojson(state.removals),
                '_newResources' : state.newResources
                }
            kw.update(morekw)
            errorSequence= self.server.actions.get(trigger+'-error')
            self.server.callActions(actions, kw, state.retVal,
                    globalVars=morekw.keys(),
                    errorSequence=errorSequence)

    def join(self, participant, readOnly=False):
        super(ProcessorTransactionService, self).join(participant, readOnly)
        if not readOnly:
            if not self.state.lock: 
                #lock on first participant joining that will write
                self.state.lock = self.server.getLock()
            self.state.readOnly = False
   
    def _cleanup(self, committed):        
        if committed:
            #if transaction completed successfully
            self._runActions('after-commit')
        elif self.state.aborted:
            self._runActions('after-abort')
        #else: nothing happened, i.e. a transaction with no writes
            
        try:
            lock = self.state.lock
            super(ProcessorTransactionService, self)._cleanup(committed)
        finally:
            if lock:  #hmm, can we release the lock earlier?
                lock.release()
        
    def _prepareToVote(self):
        #xxx: treating these this action and "finalize-commit" as 
        #   transaction participants either end of the list
        #   with the action running in voteForCommit() would be more elegant
        
        #we're about to complete the transaction,
        #here's the last chance to modify it
        try:
            self._runActions('before-commit')
        except:
            self.abort()
            raise

        super(ProcessorTransactionService, self)._prepareToVote()
        
    def _vote(self):
        super(ProcessorTransactionService, self)._vote()
        
        #all participants have successfully voted to commit the transaction
        #you can't modify the transaction any more but
        #this trigger let's you look at the completed state    
        #and gives you one last chance to abort the transaction        
        assert not self.state.safeToJoin 
        try:
            self._runActions('finalize-commit')
        except:
            self.abort()
            raise

class FileFactory(object):
    """Stream factory for a local file object"""

    def __init__(self, filename):    
        self.filename = filename

    def open(self,mode,seek=False,writable=False,autocommit=False):
        return self._open(mode, 'r'+(writable and '+' or ''), autocommit)

    def create(self,mode,seek=False,readable=False,autocommit=False):
        return self._open(mode, 'w'+(readable and '+' or ''), autocommit)

    def update(self,mode,seek=False,readable=False,append=False,autocommit=False):
        return self._open(mode, 'a'+(readable and '+' or ''), autocommit)

    def exists(self):
        return os.path.exists(self.filename)

    def _acRequired(self):
        raise NotImplementedError(
            "Files require autocommit for write operations"
        )

    def _open(self, mode, flags, ac):
        if mode not in ('t','b','U'):
            raise TypeError("Invalid open mode:", mode)

        if not ac and flags<>'r':
            self._acRequired()
        return open(self.filename, flags+mode)

    def delete(self,autocommit=False):
        if not autocommit:
            self._acRequired()
        os.unlink(self.filename)
        
    # XXX def move(self, other, overwrite=True, mkdirs=False, autocommit=False):

class TxnFileFactory(TransactionParticipant, FileFactory):
    """Transacted file (stream factory)"""
    isDeleted = False
    _isDirty = False

    def __init__(self, filename):
        super(TxnFileFactory, self).__init__(filename)  
        self.tmpName = self.filename+'.$$$'

    def _txnInProgress(self):
        raise TransactionInProgress(
            "Can't use autocommit with transaction in progress"
        )

    def isDirty(self,txnService):
        return self._isDirty or self.isDeleted
    
    def delete(self, autocommit=False):
        if self.inTransaction:
            if autocommit:
                self._txnInProgress()   # can't use ac in txn

            if not self.isDeleted:
                os.unlink(self.tmpName)
                self.isDeleted = True
        elif autocommit:
            os.unlink(self.filename)
        else:
            # Neither autocommit nor txn, join txn and set deletion flag
            self.isDeleted = True

    def _open(self, mode, flags, ac):
        if mode not in ('t','b','U'):
            raise TypeError("Invalid open mode:", mode)
        elif self.inTransaction:
            if ac:
                self._txnInProgress()
            if flags!='r':
                self._isDirty = True
            return open(self.tmpName, flags+mode)
        # From here down, we're not currently in a transaction...
        elif ac or flags=='r':
            # If we're reading, just read the original file
            # Or if autocommit, then also okay to use original file
            return open(self.filename, flags+mode)
        elif '+' in flags and 'w' not in flags:
            # Ugh, punt for now
            raise NotImplementedError(
                "Mixed-mode (read/write) access not supported w/out autocommit"
            )
        elif 'a' in flags:
            # Ugh, punt for now
            raise NotImplementedError(
                "append not supported w/out autocommit"
            )        
        else:
            # Since we're always creating the file here, we don't use 'a'
            # mode.  We want to be sure to erase any stray contents left over
            # from another transaction.
            #XXX Note that this isn't safe for
            # a multiprocess environment!  We should use a lockfile.
            stream = open(self.tmpName, flags+mode)
            self.isDeleted = False
            return stream

    def exists(self):
        if self.inTransaction:
            return not self.isDeleted
        return os.path.exists(self.filename)

    def commitTransaction(self, txnService):
        if self.isDeleted:
            os.unlink(self.filename)
            return

        try:
            os.rename(self.tmpName, self.filename)
        except OSError:
            # Windows can't do this atomically.  :(  Better hope we don't
            # crash between these two operations, or somebody'll have to clean
            # up the mess.
            os.unlink(self.filename)
            os.rename(self.tmpName, self.filename)

    def abortTransaction(self, txnService):
        #todo: what if the file is open? (esp. on windows)
        if not self.isDeleted and os.path.exists(self.tmpName):
            os.unlink(self.tmpName)

    def finishTransaction(self, txnService, committed):
        super(TxnFileFactory, self).finishTransaction(txnService, committed)
        self.isDeleted = False

#class EditableFile(TxnFile): #todo?
