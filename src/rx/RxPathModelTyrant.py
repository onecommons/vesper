'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    See RxPathDOM.py for more notes and todos.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from rx.RxPathModel import *

try:
    from hashlib import md5 # python 2.5 or greater
except ImportError:
    from md5 import new as md5

# used to detect if defaultStatements have been run
TYRANT_METADATA_KEY = "RHIZOME_TYRANT_STORE_METADATA"

# requires version from http://github.com/ericflo/pytyrant/tree/master
import pytyrant

def make_statement(d):
    return Statement(d['subj'], d['pred'],  d['obj'], d['type'], d['scope'])

def make_key(s):
    "Generate a unique hex key for a Statement"
    if not isinstance(s, tuple):
        s = tuple(s)
    m = md5()
    m.update(str(s))
    return m.hexdigest()

def safe_statement(s):
    "Ensure all strings in a Statement are Python 'str' objects, not 'unicode'"
    return Statement( *(to_str(f) for f in s) )

def to_str(s):
    "Convert any unicode strings to utf-8 encoded 'str' types"
    if isinstance(s, str):
        return s
    elif isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return str(s)

class TyrantModel(Model):
    def __init__(self, source, port=1978, defaultStatements=None):
        # accept a host string containing a port number
        if ':' in source:
            (source, port) = source.split(':')
            port = int(port)
        print "creating a TyrantModel for %s %d" % (source, port)
        self.tyrant = pytyrant.PyTableTyrant.open(source, port)
        if not TYRANT_METADATA_KEY in self.tyrant:
            import datetime, uuid
            print "initializing new tyrant database!"
            self.tyrant[TYRANT_METADATA_KEY] = {
                'version':'rhizome2 0.01',
                'uuid':str(uuid.uuid4()),
                'created':str(datetime.datetime.now())
            }
            if defaultStatements:
                self.addStatements(defaultStatements)

    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        """
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        """
        q = {}
        if subject != None:
            q['subj__streq'] = to_str(subject)
        if predicate != None:
            q['pred__streq'] = to_str(predicate)
        if object != None:
            q['obj__streq'] = to_str(object)
        if objecttype != None:
            q['type__streq'] = to_str(objecttype)
        if context != None:
            q['context_streq'] = to_str(context)

        try:
            tmp = [make_statement(x) for x in self.tyrant.search.filter(**q).items()]
            tmp.sort() # XXX make tokyo tyrant do this
            return removeDupStatementsFromSortedList(tmp, asQuad, **(hints or {}))
            #return tmp

        except Exception, e:
            # XXX items() throws an exception on empty results from filter
            #print "exception!"
            #print e
            return []
    
    def addStatement(self, statement):
        '''add the specified statement to the model'''
        statement = safe_statement(statement)
        #print "adding:", statement
        key = make_key(statement)
        self.tyrant[key] = {'subj':statement.subject,
                            'pred':statement.predicate,
                            'obj':statement.object,
                            'type':statement.objectType,
                            'scope':statement.scope}

    def removeStatement(self, statement):
        '''removes the statement'''
        #print "removing:", statement
        key = make_key(statement)
        del self.tyrant[key]

class TransactionTyrantModel(TransactionModel, TyrantModel):
    '''
    Use this class when creating a 4Suite Model using a driver that is not transactional
    (in particular, the Memory driver).
    '''
