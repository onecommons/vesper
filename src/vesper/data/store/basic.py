from vesper.data.base import * # XXX

class MemStore(Model):
    '''
    simple in-memory module
    '''
    updateAdvisory = True
    
    def __init__(self,defaultStatements=None, **kw):
        self.by_s = {}
        self.by_p = {}
        self.by_o = {}
        self.by_c = {}
        if defaultStatements:            
            self.addStatements(defaultStatements)

    def size(self):
        return len(self.by_s)
            
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
        if not isinstance(stmt, BaseStatement):
            stmt = Statement(*stmt)
        assert isinstance(stmt.object, (str, unicode)), 'bad object %r, objectType %s' % (stmt.object, stmt.objectType)
        if stmt in self.by_c.get(stmt[4], {}).get(stmt[0], []):
            return False#statement already in
        self.by_s.setdefault(stmt[0], []).append(stmt)
        self.by_p.setdefault(stmt[1], []).append(stmt)
        self.by_o.setdefault(stmt[2], []).append(stmt)
        self.by_c.setdefault(stmt[4], {}).setdefault(stmt[0], []).append(stmt)
        return True
        
    def removeStatement(self, stmt ):
        '''removes the statement'''
        stmts = self.by_s.get(stmt[0])
        if not stmts:
            return False
        try:
            stmts.remove(stmt)
        except ValueError:
            return False  
        self.by_p[stmt[1]].remove(stmt)
        self.by_o[stmt[2]].remove(stmt)
        try:
            self.by_c[stmt[4]][stmt[0]].remove(stmt)
        except (ValueError,KeyError):
            #this can happen since scope isn't part of the stmt's key
            for subjectDict in self.by_c.values():
                stmts = subjectDict.get(stmt[0],[])
                try:
                    stmts.remove(stmt)
                except ValueError:
                    pass
                else:
                    return True
        return True

class TransactionMemStore(TransactionModel, MemStore): pass

class FileStore(MemStore):
    '''
    Reads the file into memory and write out 
    '''

    def __init__(self, source='', defaultStatements=(), context='',
                            incrementHook=None, serializeOptions=None, **kw):
        self.initialContext = context
        self.defaultStatements = defaultStatements
        ntpath, stmts, format = loadFileStore(source, defaultStatements,
                                        context, incrementHook=incrementHook)
        if self.canWriteFormat(format):
            self.path = source
            self.format = format
        else:
            #source is in a format we can read by not write
            #so output ntriples if we need to write
            self.path = ntpath
            self.format = 'ntriples'
        self.serializeOptions = serializeOptions
        MemStore.__init__(self, stmts)    

    def canWriteFormat(self, format):
        return canWriteFormat(format)

    def commit(self, **kw):
        from vesper.data.transactions import TxnFileFactory
        try:
            #use TxnFileFactory so serializations errors don't corrupt file
            tff = TxnFileFactory(self.path)
            outputfile = tff.create('t')
            stmts = self.getStatements()
            serializeRDF_Stream(stmts, outputfile, self.format, options=self.serializeOptions)
            outputfile.close()
        except:
            tff.abortTransaction(None)
            tff.finishTransaction(None, False)
            raise
        else:                        
            tff.commitTransaction(None)
            tff.finishTransaction(None, True)

    def rollback(self):
        #reload file
        #XXX only reloading if dirty would be nice
        ntpath, stmts, format = loadFileStore(self.path, self.defaultStatements, self.initialContext)
        MemStore.__init__(self, stmts)
        
class TransactionFileStore(TransactionModel, FileStore): pass
        
class IncrementalNTriplesFileStoreBase(FileStore):
    '''
    Incremental save changes to an NTriples "transaction log"
    Use in a class hierarchy for Model where self has a path attribute
    and TransactionModel preceeds this in the MRO.
    '''    
    loadNtriplesIncrementally = True
    changelist = None

    def canWriteFormat(self, format):
        #only these formats support incremental output
        return format in ('ntriples', 'ntjson')

    def _getChangeList(self):
        changelist = self.changelist
        if changelist is None:
            changelist = self.changelist = []
        return changelist
        
    def addStatement(self, statement):
        '''add the specified statement to the model''' 
        added = super(IncrementalNTriplesFileStoreBase, self).addStatement(statement)
        changelist = self._getChangeList()
        #added is None if updateAdvisory == False
        if (added is None or added) and changelist is not None:
            changelist.append( (statement,) )
        return added

    def removeStatement(self, statement ):
        '''add the specified statement to the model'''               
        removed = super(IncrementalNTriplesFileStoreBase, self).removeStatement(statement)
        #removed is None if updateAdvisory == False
        changelist = self._getChangeList()
        if (removed is None or removed) and changelist is not None:
            changelist.append( (Removed, statement) )
        return removed

    def commit(self, **kw):                
        if os.path.exists(self.path):
            outputfile = file(self.path, "a+")
            changelist = self._getChangeList()
            def unmapQueue():
                for stmt in changelist:
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
            super(IncrementalNTriplesFileStoreBase, self).commit()
        self.changelist = []

    def rollback(self):        
        self.changelist = []

class IncrementalNTriplesFileStore(TransactionModel, IncrementalNTriplesFileStoreBase):
    
    def _getChangeList(self):
        return self.queue

def loadFileStore(path, defaultStatements,context='', incrementHook=None):
    '''
    If location doesn't exist create a new model and initialize it
    with the statements specified in defaultModel
    '''
    extmap = { '.nt' : 'ntriples',
      '.nj' : 'ntjson', 
      '.rdf' : 'rdfxml',
      '.json' : 'pjson',
      '.mjson' : 'mjson',
      '.yaml' : 'yaml',
    }
    #try to guess from extension
    base, ext = os.path.splitext(path)
    format = extmap.get(ext, 'unknown')        

    if os.path.exists(path):
        from vesper.utils import Uri
        uri = Uri.OsPathToUri(path)
        if incrementHook:
            options = dict(incrementHook=incrementHook)
        else:
            options = {}
        stmts, format = parseRDFFromURI(uri, type=format, scope=context,
                                options=options, getType=True)
    else:
        stmts = defaultStatements
        
    #some stores only support writing to a NTriples file 
    if not path.endswith('.nt'):
        base, ext = os.path.splitext(path)
        path = base + '.nt'
    
    return path, stmts,format

