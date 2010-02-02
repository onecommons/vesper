from vesper.data.base import * # XXX

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
    
class RedlandStore(Model):
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
                      objecttype=None,context=None, asQuad=True, hints=None):
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

class RedlandHashBdbStore(TransactionModel, RedlandStore):
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
        super(RedlandHashBdbStore, self).__init__(model)

class RedlandHashMemStore(TransactionModel, RedlandStore):
    def __init__(self, source='dummy', defaultStatements=(),**kw):
        # Create a new hash memory store
        storage = RDF.HashStorage(source,
                options="new='yes',hash-type='memory',contexts='yes'")
        model = RDF.Model(storage)
        super(RedlandHashMemStore, self).__init__(model)
        for stmt in defaultStatements:
            self.addStatement(stmt)
        model.sync()
