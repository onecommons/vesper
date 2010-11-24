#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import sys
import tempfile, os.path

from vesper import utils
from vesper.data import DataStore, transactions
from vesper.utils import glock

import logging
log = logging.getLogger("TransactionProcessor")

class TransactionProcessor(utils.ObjectWithThreadLocals):
    
    def __init__(self):
        self.initThreadLocals(requestContext=[{}], #stack of dicts
                                inErrorHandler=0)
        self.lockfile = None
        self.log = log

    def loadDataStore(self, configDict):
        self.model_uri = configDict.get('model_uri')
        if not self.model_uri:
            import socket
            self.model_uri= 'http://' + socket.getfqdn() + '/'        
        self.model_resource_uri = configDict.get('model_resource_uri', self.model_uri)
        self.lockfilepath = configDict.get('file_lock_path')
            
        useFileLock = configDict.get('use_file_lock')
        if useFileLock:
            if isinstance(useFileLock, type):
                self.LockFile = useFileLock
            else:
                self.LockFile = glock.LockFile
        else:
            self.LockFile = glock.NullLockFile #the default
        
        self.txnSvc = transactions.RaccoonTransactionService(self)
        
        dataStoreFactory = configDict.get('datastore_factory', 
                configDict.get('domStoreFactory', DataStore.BasicStore))
        self.dataStore = dataStoreFactory(self, **configDict)
        self.dataStore.addTrigger = self.txnSvc.addHook
        self.dataStore.removeTrigger = self.txnSvc.removeHook

        if configDict.get('actions',{}).get('before-new'):
            #newResourceHook is optional since it's expensive
            self.dataStore.newResourceTrigger = self.txnSvc.newResourceHook

    def getLockFile(self):                                 
        if not self.lockfile:
            if not self.lockfilepath:
                lockName = 'r' + str(hash(self.model_resource_uri)) + '.lock'
                lockfilepath = os.path.join(tempfile.gettempdir(), lockName)
            else:
                lockfilepath = self.lockfilepath
            self.lockfile = self.LockFile(lockfilepath)
        return self.lockfile
        
    def getLock(self):
        '''
        Acquires and returns the lock associated with this RequestProcessor.
        Call release() on the returned lock object to release it.
        '''        
        return utils.glock.LockGetter(self.getLockFile() )
    
    def loadModel(self):
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
