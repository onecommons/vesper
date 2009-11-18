__all__ = ['BdbModel', 'TransactionBdbModel']

import logging
log = logging.getLogger("bdb")

from rx.python_shim import *
from rx.RxPathModel import *
import bsddb, bsddb.db

try:
    bsddb.db.DB_GET_BOTH_RANGE
except AttributeError:
    #you have an old version of bsddb
    bdbver = bsddb.db.version()
    if bdbver < (4,6):    
        bsddb.db.DB_GET_BOTH_RANGE = 12
    else:
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
    
def _btopen(env, file, flag='c', mode=0666,
            btflags=0, cachesize=None, maxkeypage=None, minkeypage=None,
            pgsize=None, lorder=None):

    flags = bsddb.db.DB_CREATE # bsddb._checkflag(flag, file)
    d = bsddb.db.DB(env)
    if pgsize is not None: d.set_pagesize(pgsize)
    if lorder is not None: d.set_lorder(lorder)
    d.set_flags(btflags)
    if minkeypage is not None: d.set_bt_minkey(minkeypage)
    if maxkeypage is not None: d.set_bt_maxkey(maxkeypage)
    d.open(file, dbtype=bsddb.db.DB_BTREE, flags=flags, mode=mode)
    return bsddb._DBWithCursor(d)

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
     
    def __init__(self, source, defaultStatements=None, **kw):
        import os, os.path
        
        if source is not None:
            source = os.path.abspath(source) # bdb likes absolute paths for everything
            log.debug("opening db at:" + source)
            # source should specify a directory
            if not os.path.exists(source):
                os.makedirs(source)
            assert os.path.isdir(source), "Bdb source must be a directory"
            
            pPath = os.path.join(source, 'pred_db')
            sPath = os.path.join(source, 'subj_db')
            newdb = not os.path.exists(pPath)
        else:
            newdb = True
            pPath = sPath = None
            
        log.debug("pPath:" + pPath)
        log.debug("sPath:" + sPath)
        log.debug("is new:" + str(newdb))

        db = bsddb.db
        self.env = bsddb.db.DBEnv()
        self.env.set_lk_detect(db.DB_LOCK_DEFAULT)
        self.env.open(source, db.DB_CREATE | db.DB_INIT_LOCK | db.DB_INIT_MPOOL | db.DB_INIT_TXN)

        self.pDb = _btopen(self.env, pPath, btflags=bsddb.db.DB_DUPSORT) # DB_DUPSORT is faster than DB_DUP        
        self.pDb.db.set_get_returns_none(2)
        self.sDb = _btopen(self.env, sPath, btflags=bsddb.db.DB_DUPSORT)         
        self.sDb.db.set_get_returns_none(2)
        
        if newdb and defaultStatements:            
            self.addStatements(defaultStatements)
            
    def close(self):
        log.debug("closing db")
        self.pDb.close()
        self.sDb.close()
        self.env.close()
        
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

class TransactionBdbModel(TransactionModel, BdbModel):
    '''
    Provides in-memory transactions to MemCacheModel
    '''
