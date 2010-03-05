#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['MemCacheStore', 'TransactionMemCacheStore']

from vesper.backports import *
from vesper.data.base import * # XXX
    
class _DictHack(object):
    def __init__(self, mc, prefix):
        self.mc = mc
        self.prefix = prefix
        
    def get(self, key):
        key = str(key)
        val = self.mc.get(self.prefix+str(key))
        if val:
            delcount = 0
            stmts = []
            for s in val.split('||'):
                if s.startswith('del:'):
                    #print 'stmt', s
                    s = json.loads(s[4:])
                    stmts.remove(Statement(*s) )
                    delcount += 1 #XXX compact list if count exceed some value
                else:
                    #print 'stmt', s
                    s = json.loads(s)
                    stmts.append(Statement(*s))
            return stmts
        return []
        
    def set(self, key, value):
        key = self.prefix+str(key)
        value = str(json.dumps(value))
        if self.mc.add(key, value):
            return True
        else:
            self.mc.append(key, '||'+value)
            return False
    
    def remove(self, key, value):
        value = str(json.dumps(value))
        self.mc.append(self.prefix+str(key), '||del:'+value)

class MemCacheStore(Model):
    '''
    simple in-memory module
    '''
    debug=0
    
    def __init__(self,connect='127.0.0.1:11211', defaultStatements=None, prefix='', **kw):
        import memcache
        self.mc = mc = memcache.Client([connect], debug=0)
        self.prefix = prefix 
        self.by_s = _DictHack(mc, prefix+'!s')
        self.by_p = _DictHack(mc, prefix+'!p')
        self.by_o = _DictHack(mc, prefix+'!o')
        if defaultStatements:            
            self.addStatements(defaultStatements)     

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
        
        if fs:                
            stmts = self.by_s.get(subject)            
        elif fo:
            stmts = self.by_o.get(object)
        elif fp:
            stmts = self.by_p.get(predicate)
        else:     
            #get all            
            resources = self.mc.get(self.prefix+'!all')
            if not resources:
                return []
            stmts = []
            for subject in resources.split('||'):
                for stmt in self.by_s.get(subject):
                    if not fot or stmt.objectType == objecttype:
                        if not fc or stmt.scope == context:
                            stmts.append(stmt)
            stmts.sort()
            return removeDupStatementsFromSortedList(stmts,asQuad,
                                                limit=limit,offset=offset)                
        stmts = [s for s in stmts 
                    if (not fs or s.subject == subject)
                    and (not fp or s.predicate == predicate)
                    and (not fo or s.object == object)
                    and (not fot or s.objectType == objecttype)
                    and (not fc or s.scope == context)]
        stmts.sort()
        stmts = removeDupStatementsFromSortedList(stmts, asQuad, 
                                            limit=limit, offset=offset)
        return stmts
                     
    def addStatement(self, stmt):
        '''add the specified statement to the model'''            
        if self.by_s.set(stmt[0], stmt):
            #if first time, add subject to all            
            if not self.mc.add(self.prefix+'!all', str(stmt[0])):
                self.mc.append(self.prefix+'!all', '||'+str(stmt[0]))
        self.by_p.set(stmt[1], stmt)
        self.by_o.set(stmt[2], stmt)        
        
    def removeStatement(self, stmt):
        '''removes the statement'''
        self.by_s.remove(stmt.subject, stmt)
        self.by_p.remove(stmt.predicate, stmt)
        self.by_o.remove(stmt.object, stmt)

class TransactionMemCacheStore(TransactionModel, MemCacheStore):
    '''
    Provides in-memory transactions to MemCacheStore
    '''
