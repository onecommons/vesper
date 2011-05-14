#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import sys
import tempfile, os.path
import copy
from vesper import utils
from vesper.data import DataStore, transactions
from vesper.utils import glock

import logging
log = logging.getLogger("TransactionProcessor")

class TransactionProcessor(utils.ObjectWithThreadLocals):

    nonMergableConfigDicts = ()
    def __init__(self):
        self.initThreadLocals(requestContext=[{}], #stack of dicts
                                inErrorHandler=0)
        self.lockfile = None
        self.log = log
        self.get_principal_func = lambda kw: ''

    def initLock(self, configDict):
        useFileLock = configDict.get('use_file_lock')
        if useFileLock:
            if callable(useFileLock):
                self.LockFile = useFileLock
            else:
                self.LockFile = glock.LockFile
        else:
            self.LockFile = glock.NullLockFile #the default
        self.lockfilepath = configDict.get('file_lock_path')

    def loadDataStore(self, configDict, defaults, addNewResourceHook):
        if defaults:
            defaults = copy.deepcopy(defaults)
            utils.recursiveUpdate(defaults, configDict, self.nonMergableConfigDicts)
            configDict = defaults
        self.model_uri = configDict.get('model_uri')
        if not self.model_uri:
            import socket
            self.model_uri= 'http://' + socket.getfqdn() + '/'

        dataStoreFactory = configDict.get('datastore_factory', DataStore.BasicStore)
        dataStore = dataStoreFactory(self, **configDict)
        dataStore.addTrigger = self.txnSvc.addHook
        dataStore.removeTrigger = self.txnSvc.removeHook

        if addNewResourceHook:
            #newResourceHook is optional since it's expensive
            dataStore.newResourceTrigger = self.txnSvc.newResourceHook
            
        return dataStore

    def getLockFile(self):                                 
        if not self.lockfile:
            if not self.lockfilepath:
                lockName = 'r' + str(hash(self.model_uri)) + '.lock'
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
            for store in set(self.stores.values()):
                store.load()
        finally:
            lock.release()
        
    def executeTransaction(self, func, kw=None, retVal=None):
        kw = kw or {}
        self.txnSvc.begin(kw = kw, retVal = retVal)
        try:            
            retVal = func()            
        except:
            if not self.txnSvc.state.aborted:
                self.txnSvc.abort()
            raise
        else:
            if self.txnSvc.isActive() and not self.txnSvc.state.aborted:
                if self.txnSvc.isDirty():
                    if kw.get('__readOnly') or self.txnSvc.state.readOnly:
                        self.log.warning(
                            'a read-only transaction was modified and aborted')
                        self.txnSvc.abort()
                    else:
                        self.txnSvc.addInfo(source=self.get_principal_func(kw))
                        self.txnSvc.commit()
                else:
                    self.txnSvc.abort()                       
        return retVal

    # add a convenience contextmanager on newer versions of python
    if sys.version_info[:2] > (2,4):
        from contextlib import contextmanager

        @contextmanager
        def inTransaction(self, kw=None):
            kw = kw or {}
            self.txnSvc.begin(kw = kw)

            try:
                yield self
            except:
                if not self.txnSvc.state.aborted:
                    self.txnSvc.abort()
                raise
            else:
                if self.txnSvc.isActive() and not self.txnSvc.state.aborted:
                    if self.txnSvc.isDirty():
                        if kw.get('__readOnly') or self.txnSvc.state.readOnly:
                            self.log.warning(
                                'a read-only transaction was modified and aborted')
                            self.txnSvc.abort()
                        else:
                            self.txnSvc.addInfo(source=self.get_principal_func(kw))
                            self.txnSvc.commit()
                    else:
                        self.txnSvc.abort()