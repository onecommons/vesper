#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import os.path
import rdflib
from rdflib.Literal import Literal
from rdflib.BNode import BNode
from rdflib.URIRef import URIRef

from vesper.data.base import * # XXX
from vesper.data.store.basic import loadFileStore

def statement2rdflib(statement):
    if statement.objectType == OBJECT_TYPE_RESOURCE:            
        object = URI2node(statement.object)
    else:
        kwargs = {}
        if statement.objectType.find(':') > -1:
            kwargs['datatype'] = statement.objectType
        elif len(statement.objectType) > 1: #must be a language id
            kwargs['lang'] = statement.objectType
        object = Literal(statement.object, **kwargs)            
    return (URI2node(statement.subject),
            URI2node(statement.predicate), object, URI2node(statement.scope))

def rdflib2Statements(rdflibStatements):
    '''RDFLib triple to Statement'''
    for (subject, predicate, object, context) in rdflibStatements:
        if isinstance(object, Literal):
            objectType = object.language or object.datatype or OBJECT_TYPE_LITERAL
        else:
            objectType = OBJECT_TYPE_RESOURCE            
        yield Statement(node2String(subject),
                        node2String(predicate),
                        node2String(object),
                        node2String(objectType), scope=node2String(context))

def URI2node(uri): 
    if uri.startswith(BNODE_BASE):
        return BNode('_:'+uri[BNODE_BASE_LEN:])
    else:
        if ':' not in uri:
            uri = 'name:'+ uri
        return URIRef(uri)

def node2String(node):
    if isinstance(node, BNode):
        return BNODE_BASE + unicode(node)
    else:
        val = unicode(node)
        if not isinstance(node, Literal) and val.startswith('name:'):
            return val[5:]
        else:
            return val

def object2node(object, objectType):
    if isinstance(object, ResourceUri):
        return URI2node(object.uri)
    if objectType == OBJECT_TYPE_RESOURCE:            
        return URI2node(object)

    kwargs = {}
    if objectType and objectType.find(':') > -1:
        kwargs['datatype'] = objectType
    elif objectType and len(objectType) > 1: #must be a language id
        kwargs['lang'] = objectType
    return Literal(object, **kwargs)

class RDFLibStore(Model):
    '''
    wrapper around rdflib's TripleStore
    '''
    
    def __init__(self, graph):
        if not graph.context_aware:
            raise RuntimeError("RDFLib Graph must be context aware") 
        self.graph = graph
        self.txnState = TxnState.BEGIN

    def commit(self):
        self.txnState = TxnState.BEGIN

    def rollback(self):
        self.txnState = TxnState.BEGIN
                
    def getStatements(self, subject = None, predicate = None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        ''' Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.'''
        if subject:
            subject = URI2node(subject)
        if predicate:
            predicate = URI2node(predicate)
        if object is not None:
            object = object2node(object, objecttype)
        if context is not None:
            context = URI2node(context)

        def _getRdfLibStatements(s, p, o, context):
            for (s, p, o), cg in self.graph.store.triples((s, p, o), context=context):
                if context is not None:
                    #triple() returns all the contexts the statement appears 
                    #even when selecting triples that only appear in the given 
                    #context, but we just want the statements in that context
                    yield s, p, o, context
                else:
                    for ctx in cg:
                        yield s, p, o, ctx
                    
        statements = list( rdflib2Statements( _getRdfLibStatements(subject, predicate, object, context) ) )
        statements.sort()
        return removeDupStatementsFromSortedList(statements, asQuad, **(hints or {}))
                     
    def addStatement(self, statement ):
        '''add the specified statement to the model'''
        s, p, o, c = statement2rdflib(statement)
        self.graph.store.add((s, p, o), context=c, quoted=False)
        self.txnState = TxnState.DIRTY

    def removeStatement(self, statement ):
        '''removes the statement'''
        s, p, o, c = statement2rdflib(statement)
        self.graph.store.remove((s, p, o), context=c)
        self.txnState = TxnState.DIRTY

class RDFLibFileModel(RDFLibStore):
    def __init__(self,source='', defaultStatements=(), context='', **kw):    
        stmts, format, fsize, mtime = loadFileStore(source, context)
        if stmts is None:
            stmts = defaultStatements
        
        #try to save in source format 
        if format == 'ntriples':
            self.format = 'nt'
            self.path = source
        elif format == 'rdfxml':
            self.format = 'xml'
            self.path = source
        else:
            #unsupported format, save as ntriples (to new file)
            self.format = 'nt'
            base, ext = os.path.splitext(path)
            self.path = base + '.nt'

        import rdflib        
        RDFLibStore.__init__(self, rdflib.ConjunctiveGraph())
        self.addStatements( stmts )             

    def commit(self):
        if self.txnState == TxnState.DIRTY:
            self.graph.serialize(self.path, self.format)
        self.txnState = TxnState.BEGIN

class TransactionalRDFLibFileModel(TransactionModel, RDFLibFileModel): pass
