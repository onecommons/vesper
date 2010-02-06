'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    See RxPathDOM.py for more notes and todos.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from vesper.backports import *
from vesper import utils
from vesper.data.base.utils import *

import os.path, sys, time

import logging 
log = logging.getLogger("RxPath")

class ColumnInfo(object):
    def __init__(self, label, type=object):
        self.label = label
        self.type = type

    def __repr__(self):
        return 'ColInfo'+repr((self.label,self.type))

class Tupleset(object):
    '''
    Interface for representing a set of tuples
    '''
    columns = None

    def findColumnPos(self, label, rowinfo=False, pos=()):
        if not self.columns:
            return None

        for i, col in enumerate(self.columns):
            if label == col.label:
                pos = pos+(i,)
                if rowinfo:
                    return pos, self
                else:
                    return pos
            if isinstance(col.type, Tupleset):
                match = col.type.findColumnPos(label, rowinfo, pos+(i,))
                if match:
                    return match
        return None

    def filter(self, conditions=None, hints=None):
        '''Returns a iterator of the tuples in the set
           where conditions is a position:value mapping
        '''
        raise NotImplementedError

    def toStatements(self, context):
        return self
        
    def asBool(self):
        size = self.size()
        if size < sys.maxint:
            return bool(size)
        else:
            for row in self:
                return True
            return False

    def size(self):
        '''
        If unknown return sys.maxint
        '''
        return sys.maxint

    def __iter__(self):
        return self.filter()

    def __contains__(self, row):
        #filter for a row that matches all the columns of this row
        for test in self.filter(dict(enumerate(row))):
            if row == test:
                return True
        return False

    def update(self, rows):
        raise TypeError('Tupleset is read only')

    def append(self, row, *moreRows):
        raise TypeError('Tupleset is read only')
    
class Model(Tupleset):
    canHandleStatementWithOrder = False
    updateAdvisory = False    
    bnodePrefix = BNODE_BASE
    
    ### Transactional Interface ###
    autocommit = True
    
    def commit(self, **kw):
        return

    def rollback(self):
        return

    ### Tupleset interface ###
    columns = tuple(ColumnInfo(l, i == 4 and object or unicode) for i, l in
          enumerate(('subject', 'predicate','object', 'objecttype','context', 'listpos')))

    def filter(self,conditions=None, hints=None):
        from vesper import pjson
        kw = {}
        if conditions:
            labels = ('subject', 'predicate','object', 'objecttype','context')
            for key, value in conditions.iteritems():
                kw[labels[key] ] = value
        kw['hints'] = hints
        for stmt in self.getStatements(**kw):
            objectType = stmt[3]
            if objectType == OBJECT_TYPE_RESOURCE:
                value = ResourceUri.new(stmt[2])
            else:
                value = pjson.toJsonValue(stmt[2], objectType)
            yield (stmt[0], stmt[1], value, stmt[3], stmt[4], stmt.listpos)

    def update(self, rows):
        for row in rows:
            assert len(row) == 5
            self.addStatement(row)

    def append(self, row, *moreRows):
        assert not moreRows
        assert len(row) == 5
        self.addStatement(row)

    def explain(self, out, indent=''):        
        print >>out, indent, self.__class__.__name__,hex(id(self))
        
    ### Operations ###
                       
    def getStatements(self, subject = None, predicate = None, object=None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject, predicate or object can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        If objectype is specified, it should be one of:
        OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL, an ISO language code or an URL representing the datatype.
        If asQuad is True, will return duplicate statements if their context differs.
        '''
        assert object is not None or objecttype
        raise NotImplementedError 
        
    def addStatement(self, statement):
        '''add the specified statement to the model'''
        raise NotImplementedError 

    def addStatements(self, statements):
        '''add the specified statements to the model'''
        lists = {}
        for s in statements:                        
            self.addStatement(s)
        
    def removeStatement(self, statement ):
        '''Removes the statement. If 'scope' isn't specified, the statement
           will be removed from all contexts it appears in.
        '''
        raise NotImplementedError 

    def removeStatements(self, statements):
        '''removes the statements'''
        for s in statements:
            self.removeStatement(s)

    reifiedIDs = None
    def findStatementIDs(self, stmt):        
        if self.reifiedIDs is None:
           self.reifiedIDs = getReifiedStatements(self.getStatements())
        triple = (stmt.subject, stmt.predicate, stmt.object, stmt.objectType)
        return self.reifiedIDs.get(triple)
   
def getReifiedStatements(stmts):
    '''
    Find statements created by reification and return a list of the statements being reified 
    '''
    reifyPreds = { RDF_MS_BASE+'subject':0, RDF_MS_BASE+'predicate':1, RDF_MS_BASE+'object':2}
    reifiedStmts = {} #reificationURI => (triple)
    for stmt in stmts:
        index = reifyPreds.get(stmt.predicate)
        if index is not None:
            reifiedStmts.setdefault(stmt.subject, ['','',None, ''])[index] = stmt.object
            if index == 2:
                reifiedStmts[stmt.subject][3] = stmt.objectType
    reifiedDict = {}
    #make a new dict, with the triple as key, while ignoring any incomplete statements
    for stmtUri, triple in reifiedStmts.items():
        if triple[0] and triple[1] and triple[2] is not None:
            reifiedDict.setdefault(tuple(triple), []).append(stmtUri)
        #else: log.warning('incomplete reified statement')
    return reifiedDict

def removeDupStatementsFromSortedList(aList, asQuad=True, pred=None, 
                                                limit=None, offset=None):
    def removeDups(x, y):
        if pred and not pred(y):
            return x
        if not x:
            x.append(y)
        if asQuad and x[-1] != y:
            x.append(y)
        elif not asQuad and x[-1][:4] != y[:4]: 
            #exclude scope from comparison
            x.append(y)
        return x
    aList = reduce(removeDups, aList, [])
    if 'offset' is not None:
        aList = aList[offset:]
    if 'limit' is not None:
        aList = aList[:limit]
    return aList

class MultiModel(Model):
    '''
    This allows one writable model and multiple read-only models.
    All mutable methods will be called on the writeable model only.
    Useful for allowing static information in the model, for example representations of the application.    
    '''
    
    def __init__(self, writableModel, *readonlyModels):
        self.models = (writableModel,) + readonlyModels        
        
    autocommit = property(lambda self: self.models[0].autocommit,
                 lambda self, set: setattr(self.models[0], 'autocommit', set))
    
    updateAdvisory = property(lambda self: self.models[0].updateAdvisory)
    
    def commit(self, **kw):
        self.models[0].commit(**kw)

    def rollback(self):
        self.models[0].rollback()        

    def _handleHints(self, hints, currentcount):
        return {}
        #XXX we can do this if know order isn't important       
        if not hints:
            return hints
        limit = hints.get('limit')
        if limit is not None:
            limit -= currentcount
            if limit < 1:
                return 'done'
            hints['limit'] = limit

        offset = hints.get('offset')
        if offset is not None:
            offset -= currentcount
            hints['offset'] = offset
    
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.'''
        statements = []
        changed = 0
        _hints = hints and hints.copy() or {}
        for model in self.models:
            _hints = self._handleHints(_hints, len(statements))
            if _hints == 'done':
                break
            
            moreStatements = model.getStatements(subject, predicate,object,
                                              objecttype,context, asQuad, _hints)
            if moreStatements:
                changed += 1
                statements.extend(moreStatements)

        if changed > 1 or hints:        
            statements.sort()
            return removeDupStatementsFromSortedList(statements, asQuad, 
                                                            **(hints or {}))
        else:
            return statements            
                     
    def addStatement(self, statement ):
        '''add the specified statement to the model'''
        return self.models[0].addStatement( statement )
        
    def removeStatement(self, statement ):
        '''removes the statement'''
        return self.models[0].removeStatement( statement)

class MirrorModel(Model):
    '''
    This mirrors updates to multiple models
    Updates are propagated to all models
    Reading is only done from the first model (it assumes all models are identical)
    '''
    def __init__(self, *models):
        self.models = models

    #autocommit is false if any model has autocommit == false
    autocommit = property(
        lambda self: reduce(lambda x,y: x and y, [m.autocommit for m in self.models]),
        lambda self, set: [setattr(m, 'autocommit', set) for m in self.models] and None
        )
    
    #true if true for all models
    updateAdvisory = property(lambda self: all(m.updateAdvisory for m in self.models) )
    
    def commit(self, **kw):
        for model in self.models:
            model.commit(**kw)

    def rollback(self):
        for model in self.models:
            model.rollback()
                            
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        return self.models[0].getStatements(subject, predicate, object,
                                            objecttype,context, asQuad)
                     
    def addStatement(self, statement ):
        retval = False
        for model in self.models:            
            if model.addStatement( statement ):
                retval = True
        return retval
        
    def removeStatement(self, statement ):
        retval = False
        for model in self.models:
            if model.removeStatement( statement ):
                retval = True
        return retval

class ViewModel(MirrorModel):
    '''
    View a subset of the underlying model.
    Modifications are propagated to the underlying model.
    Doesn't support a separate transaction from the underlying model:
    calling commit or rollback will raise a RuntimeError.
    '''    

    def __init__(self, model, stmts):
        '''
        Assumes stmts are part of model.
        '''
        from vesper.data.store.basic import MemStore
        subset = MemStore(stmts)
        MirrorModel.__init__(self, subset, model)

    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):        
        return self.models[0].getStatements(subject, predicate, object,
                                            objecttype,context,asQuad)
    def commit(self, **kw):
        raise RuntimeError("invalid operation for ViewModel")

    def rollback(self):
        raise RuntimeError("invalid operation for ViewModel")
                
class TransactionModel(object):
    '''
    Provides transaction functionality for models that don't already have that.
    This class typically needs to be most derived; for example:
    
    MyModel(Model):
        def __init__(self): ...
        
        def addStatement(self, stmt): ...
        
    TransactionalMyModel(TransactionModel, MyModel): pass
    '''
    queue = None 
    updateAdvisory = False
    
    def __init__(self, *args, **kw):
        #don't create a transaction for the initial statements
        self.autocommit = True 
        super(TransactionModel, self).__init__(*args, **kw)
        self.autocommit = False

    def commit(self, **kw):        
        if not self.queue:
            return
        for stmt in self.queue:
            if stmt[0] is Removed:
                super(TransactionModel, self).removeStatement( stmt[1] )
            else:
                assert len(stmt) == 1
                super(TransactionModel, self).addStatement( stmt[0] )
        super(TransactionModel, self).commit(**kw)

        self.queue = []
        
    def rollback(self):
        if self.autocommit:        
            super(TransactionModel, self).rollback()
        self.queue = []

    def _match(self, stmt, subject = None, predicate = None, object = None,
                                               objectType=None,context=None):
        if subject and stmt.subject != subject:
            return False
        if predicate and stmt.predicate != predicate:
            return False
        if object is not None and stmt.object != object:
            return False
        if objectType is not None and stmt.objectType != objectType:
            return False
        if context is not None and stmt.scope != context:
            return False
        return True
        
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated asj a wildcard that matches any value in the model.'''
        statements = super(TransactionModel, self).getStatements(subject,
                                predicate, object,objecttype,context, asQuad,hints)
        if not self.queue: 
            return statements

        #avoid phantom reads, etc.
        changed = False
        for stmt in self.queue:
            if stmt[0] is Removed:
                stmt = stmt[1] 
                if self._match(stmt, subject, predicate, object,
                                               objecttype,context):
                    try:                        
                        i = 0
                        while 1:
                            i = statements.index(stmt, i)                            
                            del statements[i]
                            changed = True
                            i+=1
                    except ValueError:
                        pass
            else:
                if self._match(stmt[0], subject, predicate, object,
                                               objecttype,context):
                    changed = True
                    statements.append( stmt[0] )

        if changed:        
            statements.sort()
            return removeDupStatementsFromSortedList(statements, asQuad, **(hints or {}))
        else:
            return statements

    def addStatement(self, statement ):
        '''add the specified statement to the model'''
        if self.autocommit:
            return super(TransactionModel, self).addStatement(statement)        
        
        if self.queue is None: 
            self.queue = []        
        try:
            i = self.queue.index( (Removed, statement))
            del self.queue[i]            
        except ValueError:
            #print 'addingtoqueue'
            self.queue.append( (statement,) )            
        
    def removeStatement(self, statement ):
        '''removes the statement'''
        if self.autocommit:
            return super(TransactionModel, self).removeStatement(statement)
        if self.queue is None: 
            self.queue = []
        try:
            i = self.queue.index((statement,))
            del self.queue[i]
        except ValueError:
            self.queue.append( (Removed, statement) )


