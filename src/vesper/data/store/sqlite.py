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
      text subject
      text predicate
      text object value
      text objecttype
      c context (scope)
    )

    '''
     
    def __init__(self, db_path, defaultStatements=None, **kw):
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
        #if subject is specified, use subject index, 
        #  with/get_both if predicate is specified 
        #if predicate, use property index
        #if only object or scope is specified, get all and search manually
        #else: get all: use subject index, regenerate json_seq stmts
        #do a manual scan if subject list bnode
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
        if fs: 
            subject = _to_safe_str(subject)
            #if subject is specified, use subject index            
            scursor = self.sDb.db.cursor()
            if fp:
                val = _to_safe_str(predicate)
                if fo:
                    val += '\0'+ _to_safe_str(object)
                    if fot: 
                        val += '\0'+ _to_safe_str(objecttype)
                        if fc:
                            val += '\0'+_to_safe_str(context)
                #duplicates are sorted so we can position the cursor at the
                #first value we're interested
                rec = scursor.get(subject, val, bsddb.db.DB_GET_BOTH_RANGE)
            else:
                rec = scursor.set(subject)
            while rec:
                #s => p o t c 
                s, value = rec
                assert s == subject
                p, o, t, c = value.split('\0')                
                if fp:
                    #since dups are sorted we can break
                    if p != predicate:
                        break
                    if fo:
                        if o != object:
                            break
                        if fot:
                            if t != objecttype:
                                break
                            if fc:
                                if c != context:
                                    break      
                
                if ((not fo or o == object)
                    and (not fot or t == objecttype)
                    and (not fc or c == context)):            
                    stmts.append( Statement(s, p, o, t, c) )            
                rec = scursor.next_dup()
                
        elif fp:
            pcursor = self.pDb.db.cursor()            
            key = _to_safe_str(predicate)
            val = None
            if fo:
                key += '\0'+_to_safe_str(object)
                if fot: 
                    key += '\0'+_to_safe_str(objecttype)
                    if fc:
                        val = _to_safe_str(context)
                        rec = pcursor.get(key, val, bsddb.db.DB_GET_BOTH_RANGE)
            if val is None:
                rec = pcursor.set_range(key)                
                        
            while rec:                
                key, value = rec
                p, o, t = key.split('\0')                
                if p != predicate or (fo and o != object) or (fot and t != objecttype):
                    break  #we're finished with the range of the key we're interested in               
                c, s = value.split('\0')                            
                if not fc or c == context:                     
                    stmts.append( Statement(s, p, o, t, c) )                
                rec = pcursor.next()
                            
        else:            
            #get all            
            scursor = self.sDb.db.cursor()
            rec = scursor.first()
            while rec:
                s, value = rec
                p, o, t, c = value.split('\0')
                if ((not fo or o == object)
                    and (not fot or t == objecttype)
                    and (not fc or c == context)):
                    stmts.append( Statement(s, p, o, t, c) )
                rec = scursor.next()

        stmts.sort()        
        stmts = removeDupStatementsFromSortedList(stmts, asQuad, 
                                            limit=limit, offset=offset)
        return stmts

    def addStatements(self, stmts):
        lists = {}
        for stmt in stmts:
            if stmt[0].startswith('bnode:jlist:') or stmt[0].startswith('_:l'):
                if stmt[1].startswith('rdf:_'):
                    pos = stmt.predicate[:1]
                    lists.setdefault(stmt[0], _ListInfo() ).setPosition(stmt[2], stmt[3], pos)
                #elif stmt[1] == JSON_SEQ_PROP:
                #    lists.setdefault(stmt[0], _ListInfo()).prop = stmt[2]
            #elif stmt[1] == JSON_LIST_PROP:
            #    continue #exclude
            else:
                self.addStatement(stmt)
        #positions are indexed for each value
        for key, listinfo in lists.items():
            listinfo.positions.sort(key=int)
            ls = Statement(subject, '!list', '%s,%s' % (key, listinfo))
            #only add to subject index
                
    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        #print 'add', stmt
        try:
            #p o t => c s        
            self.pDb.db.put(_encodeValues(stmt[1], stmt[2], stmt[3]), _encodeValues(stmt[4], stmt[0]), flags=bsddb.db.DB_NODUPDATA)
            
            #s => p o t c
            self.sDb.db.put(_to_safe_str(stmt[0]), _encodeValues(stmt[1], stmt[2], stmt[3], stmt[4]), flags=bsddb.db.DB_NODUPDATA)
            
            return True
        except bsddb.db.DBKeyExistError:
            return False
        
    def removeStatement(self, stmt):
        '''removes the statement'''
        #p o t => c s
        pcursor = self.pDb.db.cursor()
        if pcursor.set_both(_encodeValues(stmt[1], stmt[2], stmt[3]), _encodeValues(stmt[4], stmt[0])):
            pcursor.delete()

        #s => p o t c
        scursor = self.sDb.db.cursor()
        if scursor.set_both(_to_safe_str(stmt[0]), _encodeValues(stmt[1], stmt[2], stmt[3], stmt[4]) ):
            scursor.delete()
            return True
        return False

class TransactionBdbStore(TransactionModel, BdbStore):
    '''
    Provides in-memory transactions to BdbStore
    '''
