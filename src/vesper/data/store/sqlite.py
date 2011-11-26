#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore', 'TransactionSqliteStore']

import os, os.path
import logging

import sqlite3

from vesper.backports import *
from vesper.data.base import * # XXX

class SqliteStore(Model):
    '''
    datastore using SQLite DB using Python's sqlite3 module
    
    create table statements (
      text subject (index)
      text predicate (index)
      text object value
      text objecttype
      text context
    )

    '''
     
    def __init__(self, db_path=None, defaultStatements=None, **kw):
        if db_path is None:
            self.conn = sqlite3.connect(":memory:")
            self.cursor = self.conn.cursor()
        else:
            if not os.path.exists(db_path):
                self.conn = sqlite3.connect(db_path)
                self.cursor = self.conn.cursor()
                if defaultStatements:            
                    self.addStatements(defaultStatements)
            
    def close(self):
        # are we committed?
        self.conn.close()
        
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        ''' 
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        '''
        fs = subject is not None
        fp = predicate is not None
        fo = object is not None
        fot = objecttype is not None
        fc = context is not None
        hints = hints or {}
        limit=hints.get('limit')
        offset=hints.get('offset')

        if fo:
            if isinstance(object, ResourceUri):
                object = object.uri
                fot = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not fot:
                objecttype = OBJECT_TYPE_LITERAL

        stmts = []
        stmts.sort()        
        stmts = removeDupStatementsFromSortedList(stmts, asQuad, 
                                            limit=limit, offset=offset)
        return stmts

    def addStatements(self, stmts):
        return True

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        return True
        
    def removeStatement(self, stmt):
        '''removes the statement'''
        #p o t => c s
        #s => p o t c
        return True

class TransactionSqliteStore(TransactionModel, SqliteStore):
    '''
    Provides in-memory transactions to BdbStore

    '''
