from rx.RxPathModel import *
from rx import json
import bsddb, bsddb.db
try:
    bsddb.db.DB_GET_BOTH_RANGE
except AttributeError:
    #you have an old version of bsddb
    bsddb.db.DB_GET_BOTH_RANGE = 10
    
class _ListInfo(object):
    prop = ''
    
    def __init__(self):
        self.positions = {}
        
    def setPosition(self, obj, objtype, pos):
        self.positions.setdefault( (obj, objtype), []).append(pos)
        
    def __str__(self):
        #a list where each item is an index into 
        items = self.positions.items()
        items.sort()
        return self.prop+',' + ','.join(self.positions) 

def _to_safe_str(s):
    "Convert any unicode strings to utf-8 encoded 'str' types"
    if isinstance(s, unicode):
        s = s.encode('utf-8')
    elif not isinstance(s, str):
        s = str(s)
    if '\0' in s:
        raise RuntimeError(r'strings with \0 can not be save in BdbModel')
    return s 

def _encodeValues(*args):
    ''' 
    ensure (a,bb) is before (ab,a)
    We do this by using \\0 as the delimiter
    And don't allow \\0 as a valid character 
    '''
    return '\0'.join(map(_to_safe_str, args))
    
class BdbModel(Model):
    '''
    datastore using Berkeley DB using Python's bsddb module
    
    two b-tree databases with sorted duplicates

    p o t => c s
    
    s => p o t c 

    where
        
    s subject
    p predicate
    o object value
    t objecttype
    c context (scope)
    
    keys are stored so that lexigraphic sort work properly
    '''
    #add list info to each object 
    
    debug=0
     
    def __init__(self,path, defaultStatements=None, **kw):
        import os.path
        if path is not None:
            root, ext = os.path.splitext(path)
            newdb = not os.path.exists(root+'_p'+ext)        
            pPath = root+'_p'+ext
            sPath = root+'_s'+ext
        else:
            newdb = True
            pPath = sPath = None
        #note: DB_DUPSORT is faster than DB_DUP
        self.pDb = bsddb.btopen(pPath, btflags= bsddb.db.DB_DUPSORT) 
        self.pDb.db.set_get_returns_none(2)
        self.sDb = bsddb.btopen(sPath, btflags= bsddb.db.DB_DUPSORT)         
        self.sDb.db.set_get_returns_none(2)
        if newdb and defaultStatements:            
            self.addStatements(defaultStatements)     

    def close(self):
        self.pDb.close()
        self.sDb.close()
        
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
                rec = scursor.get(subject, val, bsddb.db.DB_GET_BOTH_RANGE)
            else:
                rec = scursor.set(subject)
            while rec:
                #s => p o t c 
                s, value = rec
                assert s == subject
                p, o, t, c = value.split('\0')
                stmts.append( Statement(s, p, o, t, c) )            

                rec = scursor.next_dup()
                if not rec:
                    break
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
                        first = pcursor.get(key, val, bsddb.db.DB_GET_BOTH_RANGE)
            if val is None:
                first = pcursor.set_range(key)                
            if first:
                key, value = first
                p, o, t = key.split('\0')
                if p == predicate and (not fo or o == object) and (not fot or t == objecttype):                       
                    c, s = value.split('\0')                    
                    if not fc or c == context:
                        stmts.append( Statement(s, p, o, t, c) )
                        while 1:
                            rec = pcursor.next()
                            if not rec:
                                break                                               
                            key, value = rec
                            p, o, t = key.split('\0')
                            if p != predicate or (fo and o != object) or (fot and t != objecttype):
                                break
                            c, s = value.split('\0')
                            if fc and c != context:
                                break
                            stmts.append( Statement(s, p, o, t, c) )                
        else:            
            #get all            
            scursor = self.sDb.db.cursor()
            while 1:
                rec = scursor.next()
                if not rec:
                    break
                s, value = rec
                p, o, t, c = value.split('\0')
                stmts.append( Statement(s, p, o, t, c) )
                
        stmts = [s for s in stmts 
                    if (not fs or s.subject == subject)
                    and (not fp or s.predicate == predicate)
                    and (not fo or s.object == object)
                    and (not fot or s.objectType == objecttype)
                    and (not fc or s.scope == context)]
        stmts.sort()        
        stmts = removeDupStatementsFromSortedList(stmts, asQuad, 
                                            limit=limit, offset=offset)
        #if predicate: 
        #    print 'get', predicate, object, stmts
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

class TransactionBdbModel(TransactionModel, BdbModel):
    '''
    Provides in-memory transactions to MemCacheModel
    '''
