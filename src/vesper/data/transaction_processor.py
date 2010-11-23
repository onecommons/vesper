#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import sys

from vesper import utils
from vesper.data import DataStore, transactions

import logging
log = logging.getLogger("TransactionProcessor")

class TransactionProcessor(utils.ObjectWithThreadLocals):
    
    def __init__(self):
        self.initThreadLocals(requestContext=[{}], #stack of dicts
                                inErrorHandler=0)
        self.lock = None
        self.log = log
        self.actions = {}

    def loadDataStore(self, kw):
        self.txnSvc = transactions.RaccoonTransactionService(self)
        
        dataStoreFactory = kw.get('datastore_factory', kw.get('domStoreFactory', DataStore.BasicStore))
        self.dataStore = dataStoreFactory(self, **kw)
        self.dataStore.addTrigger = self.txnSvc.addHook
        self.dataStore.removeTrigger = self.txnSvc.removeHook

        if self.actions.get('before-new'):
            #newResourceHook is optional since it's expensive
            self.dataStore.newResourceTrigger = self.txnSvc.newResourceHook
        
        self.model_resource_uri = kw.get('model_resource_uri',
                                         self.model_uri)
        
    def getLock(self):
        '''
        Acquires and returns the lock associated with this RequestProcessor.
        Call release() on the returned lock object to release it.
        '''
        assert self.lock
        return utils.glock.LockGetter(self.lock)
    
    def loadModel(self):
        if not self.lock:
            lockName = 'r' + str(hash(self.model_resource_uri)) + '.lock'
            self.lock = self.LockFile(lockName)

        lock = self.getLock()
        try:
            self.dataStore.load()
        finally:
            lock.release()
        
    def executeTransaction(self, func, kw=None, retVal=None):
        kw = kw or {}
        self.txnSvc.begin()
        self.txnSvc.state.kw = kw
        self.txnSvc.state.retVal = retVal
        try:            
            retVal = func()            
        except:
            if not self.txnSvc.state.aborted:
                self.txnSvc.abort()
            raise
        else:
            if self.txnSvc.isActive() and not self.txnSvc.state.aborted:
                self.txnSvc.addInfo(source=self.get_principal_func(kw))
                self.txnSvc.state.retVal = retVal                
                if self.txnSvc.isDirty():
                    if kw.get('__readOnly'):
                        self.log.warning(
                        'a read-only transaction was modified and aborted')
                        self.txnSvc.abort()
                    elif not self.txnSvc.state.cantCommit:
                        self.txnSvc.commit()
                else:
                    #nothings changed, don't bother committing
                    #but need to clean up the transaction
                    self.txnSvc._cleanup(False)
                       
        return retVal

    # add a convenience contextmanager on newer versions of python
    if sys.version_info[:2] > (2,4):
        from contextlib import contextmanager

        @contextmanager
        def inTransaction(self, kw=None):
            kw = kw or {}
            self.txnSvc.begin()
            self.txnSvc.state.kw = kw

            try:
                yield self
            except:
                if not self.txnSvc.state.aborted:
                    self.txnSvc.abort()
                raise
            else:
                if self.txnSvc.isActive() and not self.txnSvc.state.aborted:
                    self.txnSvc.addInfo(source=self.get_principal_func(kw))
                    if self.txnSvc.isDirty():
                        if kw.get('__readOnly'):
                            self.log.warning(
                            'a read-only transaction was modified and aborted')
                            self.txnSvc.abort()
                        elif not self.txnSvc.state.cantCommit:
                            self.txnSvc.commit()
                    else:
                        #nothings changed, don't bother committing
                        #but need to clean up the transaction
                        self.txnSvc._cleanup(False)
