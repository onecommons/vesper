'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    See RxPathDOM.py for more notes and todos.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from rx.RxPathModel import *

# requires version from http://github.com/ericflo/pytyrant/tree/master
import pytyrant

def make_statement(d):
    return Statement(d['subj'], d['pred'],  d['obj'], OBJECT_TYPE_LITERAL, '')

class TyrantModel(Model):
    def __init__(self, host, port=1978):
        self.tyrant = pytyrant.PyTableTyrant.open(host, port)

    def commit(self, **kw):
        # XXX tyrant protocol support for this?
        pass

    def rollback(self):
        # XXX tyrant protocol support for this?
        pass
                
    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None,context=None, asQuad=False, hints=None):
        """
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        """
        #assert not asQuad
        #assert not context

        q = {}
        if subject != None:
            q['subj__streq'] = subject
        if predicate != None:
            q['pred__streq'] = predicate
        if object != None:
            q['obj__streq'] = object

        try:
            return [make_statement(x) for x in self.tyrant.search.filter(**q).items()]
        except Exception, e:
            # XXX items() throws an exception on empty results from filter
            #print "exception!"
            #print e
            return []
    
    def addStatement(self, statement):
        '''add the specified statement to the model'''
        #print "adding:", statement
        assert statement.subject
        self.tyrant[statement.subject] = {'subj':statement.subject,
                                          'pred':statement.predicate,
                                          'obj':statement.object}

    def removeStatement(self, statement):
        '''removes the statement'''
        del self.tyrant[statement.subject]

class TransactionTyrantModel(TransactionModel, TyrantModel):
    '''
    Use this class when creating a 4Suite Model using a driver that is not transactional
    (in particular, the Memory driver).
    '''

"""
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
"""