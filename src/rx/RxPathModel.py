'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    See RxPathDOM.py for more notes and todos.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from rx import utils
from rx.RxPathUtils import *

import os.path, sys, traceback

import logging 
log = logging.getLogger("RxPath")

class ColumnInfo(object):
    def __init__(self, pos, label, type=object):
        self.pos = pos
        self.label = label
        self.type = type

    def __repr__(self):
        return 'ColInfo'+repr((self.pos, self.label,self.type))

class Tupleset(object):
    '''
    Interface for representing a set of tuples
    '''
    columns = None

    def findColumn(self, label, deep=False):
        if not self.columns:
            return None

        if isinstance(label, int):
            if label >= len(self.columns):
                return None
            return self.columns[label]

        for col in self.columns:
            if label == col.label:
                return col
            if deep and isinstance(col.type, Tupleset):
                if col.type.findColumn(label, deep):
                    return col
        return None

    def findColumnPos(self, label, rowinfo=False, pos=()):
        if not self.columns:
            return None

        for col in self.columns:
            if label == col.label:
                pos = pos+(col.pos,)
                if rowinfo:
                    return pos, self
                else:
                    return pos
            if isinstance(col.type, Tupleset):
                match = col.type.findColumnPos(label, rowinfo, pos+(col.pos,))
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

    ### Transactional Interface ###
    autocommit = True
    
    def commit(self, **kw):
        return

    def rollback(self):
        return

    ### Tupleset interface ###
    columns = tuple(ColumnInfo(i, l, i == 4 and object or unicode) for i, l in
          enumerate(('subject', 'predicate','object', 'objecttype','context')))

    def filter(self,conditions=None, hints=None):
        kw = {}
        if conditions:
            labels = ('subject', 'predicate','object', 'objecttype','context')
            for key, value in conditions.iteritems():
                kw[labels[key] ] = value
        kw['hints'] = hints
        for stmt in self.getStatements(**kw):
            yield stmt

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
                      objecttype=None,context=None, asQuad=False, hints=None):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject, predicate or object can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        If objectype is specified, it should be one of:
        OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL, an ISO language code or an URL representing the datatype.
        If asQuad is True, will return duplicate statements if their context differs.
        '''
        assert object is not None or objecttype
        raise NotImplementedError 
        
    def addStatement(self, statement ):
        '''add the specified statement to the model'''
        raise NotImplementedError 

    def addStatements(self, statements):
        '''add the specified statements to the model'''
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

def removeDupStatementsFromSortedList(aList, asQuad=False, pred=None, 
                                                limit=None, offset=None):
    def removeDups(x, y):
        if pred and not pred(y):
            return x
        if (not x or (asQuad and x[-1][:] != y[:]) #include scope in comparison
            or (not asQuad and x[-1] != y)): 
            x.append(y)
        return x
    aList = reduce(removeDups, aList, [])
    if 'offset' is not None:
        aList = aList[offset:]
    if 'limit' is not None:
        aList = aList[:limit]
    return aList

class MemModel(Model):
    '''
    simple in-memory module
    '''
    def __init__(self,defaultStatements=None, **kw):
        self.by_s = {}
        self.by_p = {}
        self.by_o = {}
        self.by_c = {}
        if defaultStatements:
            for stmt in defaultStatements:
                self.addStatement(stmt)                                

    def size(self):
        return len(self.by_s)
            
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=False, hints=None):
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
        
        if not fc:
            if fs:                
                stmts = self.by_s.get(subject,[])            
            elif fo:
                stmts = self.by_o.get(object, [])
            elif fp:
                stmts = self.by_p.get(predicate, [])
            else:
                #get all
                stmts = utils.flattenSeq(self.by_s.itervalues(), 1)
                if fot:
                    stmts = [s for s in stmts if s.objectType == objecttype]
                else:
                    stmts = list(stmts)
                stmts.sort()
                return removeDupStatementsFromSortedList(stmts,asQuad,
                                                    limit=limit,offset=offset)
        else:            
            by_cAnds = self.by_c.get(context)
            if not by_cAnds:
                return []
            if fs:                
                stmts = by_cAnds.get(subject,[])
            else:
                stmts = utils.flattenSeq(by_cAnds.itervalues(), 1)
                
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
                     
    def addStatement(self, stmt ):
        '''add the specified statement to the model'''            
        if stmt in self.by_c.get(stmt[4], {}).get(stmt[0], []):
            return #statement already in
        self.by_s.setdefault(stmt[0], []).append(stmt)
        self.by_p.setdefault(stmt[1], []).append(stmt)
        self.by_o.setdefault(stmt[2], []).append(stmt)
        self.by_c.setdefault(stmt[4], {}).setdefault(stmt[0], []).append(stmt)
        
    def removeStatement(self, stmt ):
        '''removes the statement'''
        stmts = self.by_s.get(stmt.subject)
        if not stmts:
            return
        try:
            stmts.remove(stmt)
        except ValueError:
            return        
        self.by_p[stmt.predicate].remove(stmt)
        self.by_o[stmt.object].remove(stmt)
        try:
            self.by_c[stmt.scope][stmt.subject].remove(stmt)
        except (ValueError,KeyError):
            #this can happen since scope isn't part of the stmt's key
            for subjectDict in self.by_c.values():
                stmts = subjectDict.get(stmt.subject,[])
                try:
                    stmts.remove(stmt)
                except ValueError:
                    pass
                else:
                    return            
        
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
                      objecttype=None,context=None, asQuad=False, hints=None):
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
        self.models[0].addStatement( statement )
        
    def removeStatement(self, statement ):
        '''removes the statement'''
        self.models[0].removeStatement( statement)

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
    
    def commit(self, **kw):
        for model in self.models:
            model.commit(**kw)

    def rollback(self):
        for model in self.models:
            model.rollback()
                            
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=False, hints=None):
        return self.models[0].getStatements(subject, predicate, object,
                                            objecttype,context, asQuad)
                     
    def addStatement(self, statement ):
        for model in self.models:
            model.addStatement( statement )
        
    def removeStatement(self, statement ):
        for model in self.models:
            model.removeStatement( statement )

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
        subset = MemModel(stmts)
        MirrorModel.__init__(self, subset, model)

    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=False, hints=None):        
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

    def __init__(self, *args, **kw):
        super(TransactionModel, self).__init__(*args, **kw)
        self.autocommit = False
    
    def commit(self, **kw):        
        if not self.queue:
            return     
        for stmt in self.queue:
            if stmt[0] is Removed:
                super(TransactionModel, self).removeStatement( stmt[1] )
            else:
                super(TransactionModel, self).addStatement( stmt[0] )
        super(TransactionModel, self).commit(**kw)

        self.queue = []
        
    def rollback(self):
        #todo: if self.autocommit: raise exception
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
                      objecttype=None,context=None, asQuad=False, hints=None):
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
                            if not asQuad or stmt.scope == statements[i].scope:
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
        #print 'addStmt', statement
        if self.queue is None: 
            self.queue = []        
        try:
            i = 0
            while 1:
                i = self.queue.index( (Removed, statement), i)
                if self.queue[i][1].scope == statement.scope:
                    del self.queue[i]
                    return
                i+=1
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
            i = 0
            while 1:
                i = self.queue.index((statement,), i)
                if self.queue[i][0].scope == statement.scope:
                    del self.queue[i]
                    return
                i+=1
        except ValueError:
            self.queue.append( (Removed, statement) )

class TransactionMemModel(TransactionModel, MemModel): pass

class NTriplesFileModel(MemModel):
    def __init__(self, source='', defaultStatements=(), context='',
                                             incrementHook=None, **kw):
        self.path, stmts, format = _loadRDFFile(source, defaultStatements,
                                        context, incrementHook=incrementHook)
        MemModel.__init__(self, stmts)    

    def commit(self, **kw):
        outputfile = file(self.path, "w+", -1)
        stmts = self.getStatements(asQuad=True)
        writeTriples(stmts, outputfile)
        outputfile.close()
        
class _IncrementalNTriplesFileModelBase(object):
    '''
    Incremental save changes to an NTriples "transaction log"
    Use in a class hierarchy for Model where self has a path attribute
    and TransactionModel is preceeds this in the MRO.
    '''    
    loadNtriplesIncrementally = True
        
    def commit(self, **kw):                
        import os.path, time
        if os.path.exists(self.path):
            outputfile = file(self.path, "a+")
            def unmapQueue():
                for stmt in self.queue:
                    if stmt[0] is Removed:
                        yield Removed, stmt[1]
                    else:
                        yield stmt[0]
                        
            comment = kw.get('source','')
            if isinstance(comment, (list, tuple)):                
                comment = comment and comment[0] or ''
            if getattr(comment, 'getAttributeNS', None):
                comment = comment.getAttributeNS(RDF_MS_BASE, 'about')
                
            outputfile.write("#begin " + comment + "\n")            
            writeTriples( unmapQueue(), outputfile)            
            outputfile.write("#end " + time.asctime() + ' ' + comment + "\n")
            outputfile.close()
        else: #first time
            super(_IncrementalNTriplesFileModelBase, self).commit()

class IncrementalNTriplesFileModel(TransactionModel, _IncrementalNTriplesFileModelBase, NTriplesFileModel): pass

def _loadRDFFile(path, defaultStatements,context='', incrementHook=None):
    '''
    If location doesn't exist create a new model and initialize it
    with the statements specified in defaultModel
    '''
    if os.path.exists(path):
        from rx import Uri
        uri = Uri.OsPathToUri(path)
        stmts = parseRDFFromURI(uri, scope=context,
                                options=dict(incrementHook=incrementHook))
    else:
        stmts = defaultStatements

    #we only support writing to a NTriples file 
    if not path.endswith('.nt'):
        base, ext = os.path.splitext(path)
        path = base + '.nt'
        if ext == '.rdf':
            format = 'rdfxml'
        else:
            format = 'unsupported'
    else:
        format = 'ntriples'
    
    return path,stmts,format

try:
    import RDF #import Redland RDF
    #import RDF.RDF; RDF.RDF._debug = 1
    
    def node2String(node):
        if node is None:
            return ''
        elif node.is_blank():
            return BNODE_BASE + node.blank_identifier
        elif node.is_literal():
            literal = node.literal_value['string']
            if not isinstance(literal, unicode):
                return unicode(literal, 'utf8')
            else:
                return literal
        else:
            return unicode(node.uri)

    def URI2node(uri): 
        if isinstance(uri, unicode):
            uri = uri.encode('utf8')
        if uri.startswith(BNODE_BASE):
            label = uri[BNODE_BASE_LEN:]
            return RDF.Node(blank=label)
        else:
            return RDF.Node(uri_string=uri)

    def object2node(object, objectType):
        if objectType == OBJECT_TYPE_RESOURCE:
            return URI2node(object)
        else:
            if isinstance(object, unicode):
                object = object.encode('utf8')
            if isinstance(objectType, unicode):
                objectType = objectType.encode('utf8')
                
            kwargs = { 'literal':object }
            if objectType and objectType != OBJECT_TYPE_LITERAL:
                if objectType.find(':') > -1:
                    kwargs['datatype'] = RDF.Uri(objectType)
                    kwargs['language'] = None
                elif len(objectType) > 1: #must be a language id
                    kwargs['language'] = objectType                    
            return RDF.Node(**kwargs)            
        
    def statement2Redland(statement):
        object = object2node(statement.object, statement.objectType)
        return RDF.Statement(URI2node(statement.subject),
                             URI2node(statement.predicate), object)

    def redland2Statements(redlandStatements, defaultScope=''):
        '''convert result of find_statements or find_statements_context to Statements'''
        for result in redlandStatements:
            if isinstance(result, tuple):
                stmt, context = result
            else:
                stmt = result
                context = None

            if stmt.object.is_literal():
                language = stmt.object.literal_value.get('language')
                if language:
                    objectType = language
                else:
                    datatype = stmt.object.literal_value.get('datatype')
                    if datatype:
                        objectType = str(datatype)
                    else:
                        objectType = OBJECT_TYPE_LITERAL
            else:
                objectType = OBJECT_TYPE_RESOURCE
            yield Statement(node2String(stmt.subject), node2String(stmt.predicate),                            
                            node2String(stmt.object), objectType=objectType,
                            scope=node2String(context) or defaultScope)
        
    class RedlandModel(Model):
        '''
        wrapper around Redland's RDF.Model
        '''
        def __init__(self, redlandModel):
            self.model = redlandModel

        def commit(self):
            self.model.sync()

        def rollback(self):
            pass
                    
        def getStatements(self, subject=None, predicate=None, object=None,
                          objecttype=None,context=None, asQuad=False, hints=None):
            ''' Return all the statements in the model that match the given arguments.
            Any combination of subject and predicate can be None, and any None slot is
            treated as a wildcard that matches any value in the model.'''
            if subject:
                subject = URI2node(subject)            
            if predicate:
                predicate = URI2node(predicate)
            if object is not None:
                if objecttype is None:
                    #ugh... we need to do two separate queries
                    objecttypes = (OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL)
                else:
                    objecttypes = (objecttype,)
            else:
                objecttypes = (None,)

            redlandStmts = []
            for objecttype in objecttypes:
                if object is not None:
                    redlandObject = object2node(object, objecttype)
                else:
                    redlandObject = None
            
                if context or not asQuad:
                    if context:
                        redlandContext = URI2node(context)
                    else:
                        redlandContext = None
                
                    redlandStmts.append(self.model.find_statements(
                                    RDF.Statement(subject, predicate, redlandObject),
                                        context=redlandContext) )
                    defaultContext = context
                else:
                    #search across all contexts                
                    redlandStmts.append(self.model.find_statements_context(
                                RDF.Statement(subject, predicate, redlandObject)) )
                    defaultContext = ''

            statements = list( utils.flattenSeq([redland2Statements(rstmts, defaultContext)
                                         for rstmts in redlandStmts]) )
            #statements = list( redland2Statements(redlandStmts, defaultContext))
            statements.sort()
            return removeDupStatementsFromSortedList(statements, asQuad, **(hints or {}))
                         
        def addStatement(self, statement):
            '''add the specified statement to the model'''
            if statement.scope:
                context = URI2node(statement.scope)
            else:
                context = None            
            self.model.add_statement(statement2Redland(statement),
                                      context=context)

        def removeStatement(self, statement):
            '''removes the statement'''
            if statement.scope:
                context = URI2node(statement.scope)
            else:
                context = None            
            self.model.remove_statement(statement2Redland(statement),
                                        context=context)

    class RedlandHashBdbModel(TransactionModel, RedlandModel):
        def __init__(self, source='', defaultStatements=(),**kw):
            if os.path.exists(source + '-sp2o.db'):
                storage = RDF.HashStorage(source,
                                    options="hash-type='bdb',contexts='yes'")
                model = RDF.Model(storage)
            else:
                # Create a new BDB store
                storage = RDF.HashStorage(source,
                        options="new='yes',hash-type='bdb',contexts='yes'")
                model = RDF.Model(storage)                
                for stmt in defaultStatements:
                    if stmt.scope:
                        context = URI2node(stmt.scope)
                    else:
                        context = None
                    model.add_statement( statement2Redland(stmt),context=context)
                model.sync()
            super(RedlandHashBdbModel, self).__init__(model)

    class RedlandHashMemModel(TransactionModel, RedlandModel):
        def __init__(self, source='dummy', defaultStatements=(),**kw):
            # Create a new hash memory store
            storage = RDF.HashStorage(source,
                    options="new='yes',hash-type='memory',contexts='yes'")
            model = RDF.Model(storage)
            super(RedlandHashMemModel, self).__init__(model)
            for stmt in defaultStatements:
                self.addStatement(stmt)
            model.sync()
            

except ImportError:
    log.debug("Redland not installed")

try:
    import rdflib
    from rdflib.Literal import Literal
    from rdflib.BNode import BNode
    from rdflib.URIRef import URIRef
    
    def statement2rdflib(statement):
        if statement.objectType == OBJECT_TYPE_RESOURCE:            
            object = RDFLibModel.URI2node(statement.object)
        else:
            kwargs = {}
            if statement.objectType.find(':') > -1:
                kwargs['datatype'] = statement.objectType
            elif len(statement.objectType) > 1: #must be a language id
                kwargs['lang'] = statement.objectType
            object = Literal(statement.object, **kwargs)            
        return (RDFLibModel.URI2node(statement.subject),
                RDFLibModel.URI2node(statement.predicate), object)

    def rdflib2Statements(rdflibStatements, defaultScope=''):
        '''RDFLib triple to Statement'''
        for (subject, predicate, object) in rdflibStatements:
            if isinstance(object, Literal):                
                objectType = object.language or object.datatype or OBJECT_TYPE_LITERAL
            else:
                objectType = OBJECT_TYPE_RESOURCE            
            yield Statement(RDFLibModel.node2String(subject),
                            RDFLibModel.node2String(predicate),
                            RDFLibModel.node2String(object),
                            objectType=objectType, scope=defaultScope)

    class RDFLibModel(Model):
        '''
        wrapper around rdflib's TripleStore
        '''

        def node2String(node):
            if isinstance(node, BNode):
                return BNODE_BASE + unicode(node[2:])
            else:
                return unicode(node)
        node2String = staticmethod(node2String)
        
        def URI2node(uri): 
            if uri.startswith(BNODE_BASE):
                return BNode('_:'+uri[BNODE_BASE_LEN:])
            else:
                return URIRef(uri)
        URI2node = staticmethod(URI2node)

        def object2node(object, objectType):
            if objectType == OBJECT_TYPE_RESOURCE:            
                return URI2node(object)
            else:
                kwargs = {}
                if objectType.find(':') > -1:
                    kwargs['datatype'] = objectType
                elif len(objectType) > 1: #must be a language id
                    kwargs['lang'] = objectType
                return Literal(object, **kwargs)                                
        object2node = staticmethod(object2node)
        
        def __init__(self, tripleStore):
            self.model = tripleStore

        def commit(self):
            pass

        def rollback(self):
            pass
                    
        def getStatements(self, subject = None, predicate = None, object=None,
                          objecttype=None, asQuad=False, hints=None):
            ''' Return all the statements in the model that match the given arguments.
            Any combination of subject and predicate can be None, and any None slot is
            treated as a wildcard that matches any value in the model.'''
            if subject:
                subject = self.URI2node(subject)
            if predicate:
                predicate = self.URI2node(predicate)
            if object is not None:
                object = object2node(object, objectType)
            statements = list( rdflib2Statements( self.model.triples((subject, predicate, object)) ) )
            statements.sort()
            return removeDupStatementsFromSortedList(statements, asQuad, **(hints or {}))
                         
        def addStatement(self, statement ):
            '''add the specified statement to the model'''            
            self.model.add( statement2rdflib(statement) )

        def removeStatement(self, statement ):
            '''removes the statement'''
            self.model.remove( statement2rdflib(statement))

    class RDFLibFileModel(RDFLibModel):
        def __init__(self,source='', defaultStatements=(), context='', **kw):
            ntpath, stmts, format = _loadRDFFile(source, defaultStatements,context)
            if format == 'unsupported':                
                self.format = 'nt'
                self.path = ntpath
            else:
                self.format = (format == 'ntriples' and 'nt') or (
                               format == 'rdfxml' and 'xml') or 'error'
                assert self.format != 'error', 'unexpected format'
                self.path = source
                
            from rdflib.TripleStore import TripleStore                                    
            RDFLibModel.__init__(self, TripleStore())
            for stmt in stmts:
                self.addStatement( stmt )             
    
        def commit(self):
            self.model.save(self.path, self.format)

    class TransactionalRDFLibFileModel(TransactionModel, RDFLibFileModel): pass
        
except ImportError:
    log.debug("rdflib not installed")
