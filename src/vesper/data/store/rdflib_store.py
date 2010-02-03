import rdflib
from rdflib.Literal import Literal
from rdflib.BNode import BNode
from rdflib.URIRef import URIRef

from vesper.data.base import * # XXX

def statement2rdflib(statement):
    if statement.objectType == OBJECT_TYPE_RESOURCE:            
        object = RDFLibStore.URI2node(statement.object)
    else:
        kwargs = {}
        if statement.objectType.find(':') > -1:
            kwargs['datatype'] = statement.objectType
        elif len(statement.objectType) > 1: #must be a language id
            kwargs['lang'] = statement.objectType
        object = Literal(statement.object, **kwargs)            
    return (RDFLibStore.URI2node(statement.subject),
            RDFLibStore.URI2node(statement.predicate), object)

def rdflib2Statements(rdflibStatements, defaultScope=''):
    '''RDFLib triple to Statement'''
    for (subject, predicate, object) in rdflibStatements:
        if isinstance(object, Literal):                
            objectType = object.language or object.datatype or OBJECT_TYPE_LITERAL
        else:
            objectType = OBJECT_TYPE_RESOURCE            
        yield Statement(RDFLibStore.node2String(subject),
                        RDFLibStore.node2String(predicate),
                        RDFLibStore.node2String(object),
                        objectType=objectType, scope=defaultScope)

class RDFLibStore(Model):
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
                      objecttype=None, asQuad=True, hints=None):
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

class RDFLibFileModel(RDFLibStore):
    def __init__(self,source='', defaultStatements=(), context='', **kw):    
        ntpath, stmts, format = loadRDFFile(source, defaultStatements,context)
        
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
            self.path = ntpath

        from rdflib.TripleStore import TripleStore                                    
        RDFLibStore.__init__(self, TripleStore())
        self.addStatements( stmts )             

    def commit(self):
        self.model.save(self.path, self.format)

class TransactionalRDFLibFileModel(TransactionModel, RDFLibFileModel): pass
