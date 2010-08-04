#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    RDF-related unit tests
"""
import unittest, os, os.path, glob, tempfile
import cStringIO
from pprint import *

from vesper.data.base import *
from vesper.data.store.basic import *

from vesper.data.base import graph

import time
from vesper.data.base.utils import parseTriples, canWriteFormat
from vesper.utils import pprintdiff
    
class RDFStaticTestCase(unittest.TestCase):
    '''
    Some tests that don't rely on creating a store.
    '''

    def testNtriples(self):        
        #test character escaping 
        s1 = r'''bug: File "g:\_dev\rx4rdf\rx\Server.py", '''
        n1 = r'''_:x1f6051811c7546e0a91a09aacb664f56x142 <http://rx4rdf.sf.net/ns/archive#contents> "bug: File \"g:\\_dev\\rx4rdf\\rx\\Server.py\", ".'''
        [(subject, predicate, object, objectType, scope)] = [x for x in parseTriples([n1])]
        self.failUnless(s1 == object)
        #test xml:lang support
        n2 = r'''_:x1f6051811c7546e0a91a09aacb664f56x142 <http://rx4rdf.sf.net/ns/archive#contents> "english"@en-US.'''
        [(subject, predicate, object, objectType, scope)] = [x for x in parseTriples([n2])]
        self.failUnless(object=="english" and objectType == 'en-US')
        #test datatype support
        n3 = r'''_:x1f6051811c7546e0a91a09aacb664f56x142 <http://rx4rdf.sf.net/ns/archive#contents>'''\
        ''' "1"^^http://www.w3.org/2001/XMLSchema#int.'''
        [(subject, predicate, object, objectType, scope)] = [x for x in parseTriples([n3])]
        self.failUnless(object=="1" and objectType == 'http://www.w3.org/2001/XMLSchema#int')

        sio = cStringIO.StringIO()
        writeTriples( [Statement('test:s', 'test:p', u'\x10\x0a\\\u56be',
                                 OBJECT_TYPE_LITERAL)], sio, 'ascii')
        self.failUnless(sio.getvalue() == r'<test:s> <test:p> "\u0010\n\\\u56BE" .'
                        '\n')                      

        #test URI validation when writing triples
        out = cStringIO.StringIO()
        self.failUnlessRaises(RuntimeError, lambda:
            writeTriples( [Statement(BNODE_BASE+'foo bar', 'http://foo bar', 
                'http://foo bar')], out) )
        writeTriples( [Statement(BNODE_BASE+'foobar', 'http://foo', 
                'http://foo bar')], out)         
        self.failUnlessRaises(RuntimeError, lambda:
            writeTriples( [Statement(BNODE_BASE+'foobar', 'http://foo', 
                'http://foo bar',OBJECT_TYPE_RESOURCE)], out) )

    def testStatement(self):
        #we do include scope as part of the Statements key        
        st1 = Statement('test:s', 'test:p', 'test:o', 'R', 'test:c')
        st2 = Statement('test:s', 'test:p', 'test:o', 'R', '')
        self.failUnless(st2 not in [st1])
        self.failUnless(st1 not in {st2:1})

        self.failUnless(Statement('s', 'o', 'p') == Statement('s', 'o', 'p'))
        self.failUnless(Statement('s', 'o', 'p','L') == Statement('s', 'o', 'p'))        
        self.failUnless(Statement('s', 'o', 'p',scope='C1') != Statement('s', 'o', 'p', scope='C2'))
        self.failUnless(Statement('s', 'o', 'p','L','C') != Statement('s', 'o', 'p'))
        self.failUnless(not Statement('s', 'o', 'p','L','C') == Statement('s', 'o', 'p'))
        self.failUnless(Statement('s', 'p', 'a') < Statement('s', 'p', 'b'))

    def testTriple(self):
        #we don't include scope as part of the Triple key
        st1 = Triple('test:s', 'test:p', 'test:o', 'R', 'test:c')
        st2 = Triple('test:s', 'test:p', 'test:o', 'R', '')
        self.failUnless(st2 in [st1] and [st2].index(st1) == 0)
        self.failUnless(st1 in {st2:1}  )

        self.failUnless(Triple('s', 'o', 'p') == Statement('s', 'o', 'p'))
        self.failUnless(Triple('s', 'o', 'p','L') == Statement('s', 'o', 'p'))        

        self.failUnless(Triple('s', 'o', 'p') == Triple('s', 'o', 'p'))
        self.failUnless(Triple('s', 'o', 'p','L') == Triple('s', 'o', 'p'))        

        self.failUnless(Statement('s', 'o', 'p') == Triple('s', 'o', 'p'))
        self.failUnless(Statement('s', 'o', 'p','L') == Triple('s', 'o', 'p'))        

        self.failUnless(Triple('s', 'o', 'p',scope='C1') == Triple('s', 'o', 'p', scope='C2'))
        self.failUnless(Triple('s', 'o', 'p','L','C') == Triple('s', 'o', 'p'))
        self.failUnless(not Triple('s', 'o', 'p','L','C') != Triple('s', 'o', 'p'))
        self.failUnless(Triple('s', 'p', 'a') < Triple('s', 'p', 'b'))

    def testSerialize(self):
        model = r'''<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#Contents> .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#sha1-digest> "ndKxl8RGnTmr3u/omnJxVdGnWgXuA=" .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#hasContent> " llll"@en-US .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#content-length> "5"^^http://www.w3.org/2001/XMLSchema#int .
_:_a <http://rx4rdf.sf.net/ns/wiki#name> _:_a .
_:_b <http://rx4rdf.sf.net/ns/wiki#name> _:_b .
#ssss!graph context:add:context:txn:blah
_:_a <http://rx4rdf.sf.net/ns/wiki#name> _:_a .
'''        
        stmts = MemStore(parseRDFFromString(model,'test:', 'ntriples')).getStatements()
        for stype in ['ntriples', 'ntjson', 'pjson', 'mjson', 'yaml', 'rdfxml']:
            if not canWriteFormat(stype):
                print 'warning, can not test serializing %s, dependent library not installed' % stype
                continue
            #print 'stype', stype
            options = {}
            if stype == 'mjson':
                options = dict(blobmax=30)
            json = serializeRDF(stmts, stype, options=options)
            if stype in ['pjson', 'mjson', 'yaml']:
                #don't add the order preserving RDF
                options = dict(addOrderInfo=False)
            else:
                options = {}
            newstmts = list(parseRDFFromString(json,'', stype, options=options))
            stmts.sort()
            newstmts.sort()
            #print 'stmts'
            #print stmts
            #print 'newstmts'
            #print newstmts
            #print 'lenght', len(stmts), len(newstmts)
            self.failUnless(stmts == newstmts, 
                        stype+' failed: '+ pprintdiff(stmts, newstmts ))
    
class RDFSchemaTestCase(unittest.TestCase):
    ''' tests models with:
            bNodes
            literals: empty (done for xupdate), xml, text with invalid xml characters, binary
            advanced rdf: rdf:list, containers, datatypes, xml:lang
            circularity 
            empty element names (_)
            multiple rdf:type
            RDF Schema support
        diffing and merging models
    '''

    testHistory = ''#split' #'single', 'split' or '' (for no graph manager)
    graphManagerClass = graph.MergeableGraphManager
    graphManagerClass = graph.NamedGraphManager

    def setUp(self):        
        if DRIVER == '4Suite':
            self.loadModel = self.loadFtModel
        elif DRIVER == 'RDFLib':
            self.loadModel = self.loadRdflibModel
        elif DRIVER == 'Redland':
            self.loadModel = self.loadRedlandModel
        elif DRIVER == 'Mem':
            self.loadModel = self.loadMemStore
        elif DRIVER == 'Bdb':
            self.loadModel = self.loadBdbModel
        elif DRIVER == 'Tyrant':
            from basicTyrantTest import start_tyrant_server
            self.tyrant = start_tyrant_server()
            self.loadModel = self.loadTyrantModel
        else:
            raise "unrecognized driver: " + DRIVER
        #from rx import RxPath
        #RxPath.useQueryEngine = True
        
    def loadFtModel(self, source, type='nt'):
        if type == 'rdf':
            #assume relative file
            model, self.db = Util.DeserializeFromUri('file:'+source, scope='')
        else:
            model, self.db = DeserializeFromN3File( source )
        #use TransactionFtModel because we're using 4Suite's Memory
        #driver, which doesn't support transactions
        return TransactionFtModel(model)

    def loadRedlandModel(self, source, type='nt'):
        if type == 'rdf':
            assert False, 'Not Supported'
        else:            
            for f in glob.glob('RDFDomTest*.db'):
                if os.path.exists(f):
                    os.unlink(f)
            if isinstance(source, (str, unicode)):
                stream = file(source, 'r+')
            else:
                stream = source
            stmts = NTriples2Statements(stream)
            return RedlandHashMemModel("RDFDomTest", stmts)
            #return RedlandHashBdbStore("RDFDomTest", stmts)

    def loadRdflibModel(self, source, type='nt'):
        if type == 'nt':
            type = 'ntriples'
        elif type == 'rdf':
            type = 'rdfxml'        

        from vesper.data.store.rdflib_store import RDFLibFileModel
        
        if isinstance(source, (str, unicode)):
            data = parseRDFFromURI('file:'+source,type)
        else:
            data = parseRDFFromString(source.read(),'test:', type)
            
        dest = tempfile.mktemp()        
        return RDFLibFileModel(dest, data)

    def loadMemStore(self, source, type='nt'):
        if type == 'nt':
            type = 'ntriples'
        elif type == 'rdf':
            type = 'rdfxml'        
        if isinstance(source, (str, unicode)):
            return TransactionMemStore(parseRDFFromURI('file:'+source,type))
        else:
            return TransactionMemStore(parseRDFFromString(source.read(),'test:', type))

    def loadTyrantModel(self, source, type='nt'):
        from vesper.data.store.tyrant import TransactionTyrantStore
        
        if type == 'nt':
            type = 'ntriples'
        elif type == 'rdf':
            type = 'rdfxml'

        if isinstance(source, (str, unicode)):
            data = parseRDFFromURI('file:'+source,type)
        else:
            data = parseRDFFromString(source.read(),'test:', type)

        port = self.tyrant['port']
        model = TransactionTyrantStore('localhost', port)
        model.addStatements(data)
        return model

    def loadBdbModel(self, source, type='nt'):
        from vesper.data.store.bdb import TransactionBdbStore
        
        if type == 'nt':
            type = 'ntriples'
        elif type == 'rdf':
            type = 'rdfxml'

        if isinstance(source, (str, unicode)):
            data = parseRDFFromURI('file:'+source,type)
        else:
            data = parseRDFFromString(source.read(),'test:', type)

        for f in glob.glob('RDFDomTest*.bdb'):
            if os.path.exists(f):
                os.unlink(f)
        
        model = TransactionBdbStore('RDFDomTest.bdb', data)
        return model

    def getModel(self, source, type='nt', useSchema=True):
        model = self.loadModel(source, type)

        modelUri =generateBnode()
        if self.testHistory:
            if self.testHistory == 'single':
                revmodel = None
            else:
                assert self.testHistory == 'split'
                revmodel = TransactionMemStore()
            graphManager = self.graphManagerClass(model, revmodel, modelUri)
            model = graphManager
        else:
            graphManager = None

        if useSchema:
            from vesper.data.base import schema
            schemaClass = schema.RDFSSchema        
            schema = schemaClass(model)
            if isinstance(schema, Model):
                model = schema
                schema.findCompatibleStatements = True

        return model
       
    def tearDown(self):
        if DRIVER == 'Tyrant':
            from basicTyrantTest import stop_tyrant_server
            stop_tyrant_server(self.tyrant)
            self.tyrant = None

    def testSubtype(self):        
        model = '''_:C <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:D.
_:C <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:F.
_:B <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:D.
_:B <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:E.
_:A <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:B.
_:A <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:C.
_:O1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:C.
_:O2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:B.
_:O2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:F.
_:O3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:B.
_:O4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:A.
_:O4 <http://rx4rdf.sf.net/ns/archive#contents> "".
'''
        model = self.getModel(cStringIO.StringIO(model) )
        def getcount(obj, p=0):
            stmts = model.getStatements(
                predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
                object=obj, objecttype=OBJECT_TYPE_RESOURCE)
            if p: print 'getcount for', obj, set(s[0] for s in stmts), stmts
            return len(set(s[0] for s in stmts))

        self.assertEquals(getcount('bnode:A'), 1)        
        self.assertEquals(getcount('bnode:D'), 4)
        self.assertEquals(getcount('bnode:F'), 3)

        ### domain
        #add a domain rule and a property to trigger it        
        model.addStatements([
        Statement('test:prop', RDF_SCHEMA_BASE+u'domain', 'bnode:A', 'R'),
        Statement('bnode:O5', 'test:prop', 'test'),
        ])
        self.assertEquals(getcount('bnode:A'), 2)
        self.assertEquals(getcount('bnode:D'), 5)

        #already has this type, so adding prop on this resource shouldn't change anything
        model.addStatements([
        Statement('bnode:O4', 'test:prop', 'test2'),
        ])
        self.assertEquals(getcount('bnode:A'), 2)
        self.assertEquals(getcount('bnode:D'), 5)
        #neither should removing it
        model.removeStatements([
        Statement('bnode:O4', 'test:prop', 'test2'),
        ])
        self.assertEquals(getcount('bnode:A'), 2)
        self.assertEquals(getcount('bnode:D'), 5)

        #add a subproperty rule and an resource with that property
        model.addStatements([
        Statement('test:subprop', RDF_SCHEMA_BASE+u'subPropertyOf', 'test:prop', 'R'),
        Statement('bnode:D6', 'test:subprop', 'test'),
        ])
        #subproperty should trigger entailments too
        self.assertEquals(getcount('bnode:A'), 3)
        self.assertEquals(getcount('bnode:D'), 6)

        #remove the rule, the entailments should be removed too
        model.removeStatements([
        Statement('test:prop', RDF_SCHEMA_BASE+u'domain', 'bnode:A', 'R'),
        ])
        #XXX adding test:subprop break removals
        #self.assertEquals(getcount('bnode:A'), 1)
        #self.assertEquals(getcount('bnode:D'), 4)
        
        ### range
        #add a range rule and a property to trigger it
        model.addStatements([
        Statement('test:propR', RDF_SCHEMA_BASE+u'range', 'bnode:A', 'R'),
        Statement('bnode:O6', 'test:propR', 'bnode:O5', 'R'),
        ])
        self.assertEquals(getcount('bnode:A'), 2)
        self.assertEquals(getcount('bnode:D'), 5)

        #already has this type, so adding prop on this resource shouldn't change anything
        model.addStatements([
        Statement('bnode:O7', 'test:propR', 'bnode:O4', 'R'),
        ])
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)
        #neither should removing it
        model.removeStatements([
        Statement('bnode:O7', 'test:propR', 'bnode:O4', 'R'),
        ])
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)

        #add a subproperty rule and an resource with that property
        model.addStatements([
        Statement('test:subpropR', RDF_SCHEMA_BASE+u'subPropertyOf', 'test:propR', 'R'),
        Statement('bnode:R1', 'test:subpropR', 'bnode:R2', 'R'),
        ])
        #subproperty should trigger entailments too
        self.failUnless(getcount('bnode:A') == 3)
        self.failUnless(getcount('bnode:D') == 6)

        #remove the rule, the entailments should be removed too
        model.removeStatements([
        Statement('test:propR', RDF_SCHEMA_BASE+u'range', 'bnode:A', 'R'),
        ])
        #XXX adding test:subpropR break removals
        #self.failUnless(getcount('bnode:A') == 1)
        #self.failUnless(getcount('bnode:D') == 4)


    def testSubproperty(self):        
        model = '''<http://rx4rdf.sf.net/ns/archive#C> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#D>.
<http://rx4rdf.sf.net/ns/archive#C> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#F>.
<http://rx4rdf.sf.net/ns/archive#B> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#D>.
<http://rx4rdf.sf.net/ns/archive#B> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#E>.
<http://rx4rdf.sf.net/ns/archive#A> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#B>.
<http://rx4rdf.sf.net/ns/archive#A> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://rx4rdf.sf.net/ns/archive#C>.
_:O1 <http://rx4rdf.sf.net/ns/archive#C> "".
_:O2 <http://rx4rdf.sf.net/ns/archive#B> "".
_:O2 <http://rx4rdf.sf.net/ns/archive#F> "".
_:O3 <http://rx4rdf.sf.net/ns/archive#B> "".
_:O4 <http://rx4rdf.sf.net/ns/archive#A> "".
'''
        startmodel = model = self.getModel(cStringIO.StringIO(model) )

        def getcount(pred):
            stmts = startmodel.getStatements(predicate=pred)
            return len(stmts)
        
        a='http://rx4rdf.sf.net/ns/archive#'
        self.assertEquals(getcount(a+'A'), 1)        
        self.assertEquals(getcount(a+'D'), 4)
        self.assertEquals(getcount(a+'F'), 3)

        #modify the schema and make sure inferences are updated properly

        #remove the statement that A is a subproperty of 
        model.removeStatement(
            Statement('http://rx4rdf.sf.net/ns/archive#A', 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf', 
                    'http://rx4rdf.sf.net/ns/archive#C', OBJECT_TYPE_RESOURCE))
        
        self.assertEquals(getcount(a+'F'), 2)
        
        stmt = Statement("http://rx4rdf.sf.net/ns/archive#E", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
                         "http://rx4rdf.sf.net/ns/archive#F", objectType=OBJECT_TYPE_RESOURCE)
        model.addStatements([stmt]) 
        self.assertEquals(getcount(a+'F'), 5)        
        
class GraphTestCase(RDFSchemaTestCase):
    testHistory = 'single'#, 'split' or '' (for no graph manager)
    graphManagerClass = graph.NamedGraphManager

class MergeableGraphTestCase(RDFSchemaTestCase):
    testHistory = 'single'#, 'split' or '' (for no graph manager)
    graphManagerClass = graph.MergeableGraphManager

class SplitGraphTestCase(RDFSchemaTestCase):
    testHistory = 'split' #'single', 'split' or '' (for no graph manager)
    graphManagerClass = graph.NamedGraphManager

class MergeableSplitGraphTestCase(RDFSchemaTestCase):
    testHistory = 'split' #'single', 'split' or '' (for no graph manager)
    graphManagerClass = graph.MergeableGraphManager
                        
DRIVER = 'Mem'

if DRIVER == '4Suite':
    from Ft.Rdf import Util
    from Ft.Rdf.Statement import Statement as FtStatement
    from Ft.Rdf.Model import Model as Model4Suite
    #this function is no longer used by RxPath
    def DeserializeFromN3File(n3filepath, driver=Memory, dbName='', create=0, defaultScope='',
                            modelName='default', model=None):
        if not model:
            if create:
                db = driver.CreateDb(dbName, modelName)
            else:
                db = driver.GetDb(dbName, modelName)
            db.begin()
            model = Model4Suite(db)
        else:
            db = model._driver
            
        if isinstance(n3filepath, ( type(''), type(u'') )):
            stream = file(n3filepath, 'r+')
        else:
            stream = n3filepath
            
        #bNodeMap = {}
        #makebNode = lambda bNode: bNodeMap.setdefault(bNode, generateBnode(bNode))
        makebNode = lambda bNode: BNODE_BASE + bNode
        for stmt in parseTriples(stream,  makebNode):
            if stmt[0] is Removed:            
                stmt = stmt[1]
                scope = stmt[4] or defaultScope
                model.remove( FtStatement(stmt[0], stmt[1], stmt[2], '', scope, stmt[3]) )
            else:
                scope = stmt[4] or defaultScope
                model.add( FtStatement(stmt[0], stmt[1], stmt[2], '', scope, stmt[3]) )                
        #db.commit()
        return model, db

def profilerRun(testname, testfunc):
    import hotshot, hotshot.stats
    global prof
    prof = hotshot.Profile(testname+".prof")
    try:
        testfunc() #prof.runcall(testfunc)
    except:
        import traceback; traceback.print_exc()
    prof.close()

    stats = hotshot.stats.load(testname+".prof")
    stats.strip_dirs()
    stats.sort_stats('cumulative','time')
    #stats.sort_stats('time','calls')
    stats.print_stats(100)            

if __name__ == '__main__':
    import sys
    import logging
    logging.root.setLevel(logging.DEBUG)
    logging.basicConfig()

    #import os, os.path
    #os.chdir(os.path.basename(sys.modules[__name__ ].__file__))    
    if sys.argv.count('--driver'):
        arg = sys.argv.index('--driver')
        DRIVER = sys.argv[arg+1]
        del sys.argv[arg:arg+2]

    profile = sys.argv.count('--prof')
    if profile:
        del sys.argv[sys.argv.index('--prof')]

    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        unittest.main()
    else:
        tc = RDFSchemaTestCase(test)
        tc.setUp()
        testfunc = getattr(tc, test)
        if profile:
            profilerRun(test, testfunc)
        else:
            testfunc() #run test


