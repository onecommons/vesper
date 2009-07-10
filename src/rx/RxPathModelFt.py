'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    See RxPathDOM.py for more notes and todos.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from rx.RxPathModel import *

import os.path, sys, traceback

import Ft.Rdf.Model
from Ft.Rdf.Statement import Statement as FtStatement   
from Ft.Rdf.Drivers import Memory
from Ft.Rdf import OBJECT_TYPE_UNKNOWN #"?"

if not hasattr(FtStatement, 'asTuple'):
    #bug fix for pre beta1 versions of 4Suite
    def cmpStatements(self,other):
        if isinstance(other,FtStatement):        
            return cmp( (self.subject,self.predicate,self.object, self.objectType),#, self.scope),
                        (other.subject,other.predicate, other.object, other.objectType))#, other.scope))
        else:
            raise TypeError("Object being compared must be a Statement, not a %s" % type(other))
    FtStatement.__cmp__ = cmpStatements
#todo: we (and 4Suite) doesn't consider scope, change this when we change our model

def Ft2Statements(statements, defaultScope=''):
    for stmt in statements:
        if stmt.objectType == OBJECT_TYPE_UNKNOWN:
            objectType = OBJECT_TYPE_LITERAL
        else:
            objectType = stmt.objectType
        yield Statement(stmt.subject, stmt.predicate,  stmt.object,
                objectType=objectType, scope=stmt.scope or defaultScope)
    
def statement2Ft(stmt):
    return FtStatement(stmt.subject, stmt.predicate, stmt.object,
            objectType=stmt.objectType, scope=stmt.scope)

class FtModel(Model):
    '''
    wrapper around 4Suite's Ft.Rdf.Model
    '''
    def __init__(self, ftmodel):
        self.model = ftmodel

    def _beginIfNecessary(self):
        if not getattr(self.model._driver, '_db', None):
            #all the 4Suite driver classes that require begin() set a _db attribute
            #and for the ones that don't, begin() is a no-op
            self.model._driver.begin()

    def commit(self, **kw):
        self.model._driver.commit()

    def rollback(self):
        self.model._driver.rollback()        
                
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=False):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.'''
        self._beginIfNecessary()        
        statements = list(Ft2Statements(
            self.model.complete(subject, predicate, object,scope=context)))
        statements.sort()
        #4Suite doesn't support selecting based on objectype so filter here
        if objecttype:
            pred = lambda stmt: stmt.objectType == objecttype
        else:
            pred = None
        return removeDupStatementsFromSortedList(statements, asQuad, pred)
                     
    def addStatement(self, statement ):
        '''add the specified statement to the model'''
        self._beginIfNecessary()
        self.model.add( statement2Ft(statement) )

    def removeStatement(self, statement ):
        '''removes the statement'''
        self._beginIfNecessary()
        self.model.remove( statement2Ft(statement) )

class TransactionFtModel(TransactionModel, FtModel):
    '''
    Use this class when creating a 4Suite Model using a driver that is not transactional
    (in particular, the Memory driver).
    '''
    
class NTriplesFtModel(FtModel):
    def __init__(self, source='', defaultStatements=(), context='', **kw):
        self.path, stmts, format = _loadRDFFile(source,
                                                defaultStatements,context)
        db = Memory.GetDb('', 'default')
        model = Ft.Rdf.Model.Model(db)
        stmts = [statement2Ft(stmt) for stmt in stmts]
        model.add(stmts)
        FtModel.__init__(self, model)    
    
    def commit(self, **kw):
        self.model._driver.commit()
        outputfile = file(self.path, "w+", -1)
        stmts = self.model._driver._statements['default'] #get statements directly, avoid copying list
        def mapStatements(stmts):
            #map 4Suite's tuples to Statements
            for stmt in stmts:                    
                if stmt[5] == OBJECT_TYPE_UNKNOWN:
                    objectType = OBJECT_TYPE_LITERAL
                else:
                    objectType = stmt[5]
                yield (stmt[0], stmt[1], stmt[2], objectType, stmt[3])
        writeTriples(mapStatements(stmts), outputfile)
        outputfile.close()

class IncrementalNTriplesFtModel(TransactionModel, _IncrementalNTriplesFileModelBase, NTriplesFtModel): pass
