"""
    Rx4RDF unit tests

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import unittest, os, os.path, glob, tempfile
import cStringIO
from pprint import *

from vesper.data.RxPath import *
from vesper.data.store.basic import *

from vesper.data import RxPathGraph

import time
from vesper.data.RxPathUtils import _parseTriples as parseTriples
from vesper.utils import pprintdiff
    
class RDFDomTestCase(unittest.TestCase):
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
    graphManagerClass = RxPathGraph.MergeableGraphManager
    graphManagerClass = RxPathGraph.NamedGraphManager

    model1 = r'''#test
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://rx4rdf.sf.net/ns/archive#created-on> "1057790527.921" .
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://rx4rdf.sf.net/ns/archive#has-expression> <urn:sha:XPmK/UXVwPzgKryx1EwoHtTMe34=> .
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://rx4rdf.sf.net/ns/archive#last-modified> "1057790527.921" .
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://rx4rdf.sf.net/ns/wiki#name> "HomePage" .
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://rx4rdf.sf.net/ns/wiki#summary> "l" .
<http://4suite.org/rdf/banonymous/5c79e155-5688-4059-9627-7fee524b7bdf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#NamedContent> .
<urn:sha:XPmK/UXVwPzgKryx1EwoHtTMe34=> <http://rx4rdf.sf.net/ns/archive#content-length> "13" .
<urn:sha:XPmK/UXVwPzgKryx1EwoHtTMe34=> <http://rx4rdf.sf.net/ns/archive#hasContent> "            kkk &nbsp;" .
<urn:sha:XPmK/UXVwPzgKryx1EwoHtTMe34=> <http://rx4rdf.sf.net/ns/archive#sha1-digest> "XPmK/UXVwPzgKryx1EwoHtTMe34=" .
<urn:sha:XPmK/UXVwPzgKryx1EwoHtTMe34=> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#Contents> .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#NamedContent> .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://rx4rdf.sf.net/ns/wiki#name> "test" .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://rx4rdf.sf.net/ns/archive#created-on> "1057790874.703" .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://rx4rdf.sf.net/ns/archive#has-expression> <urn:sha:jERppQrIlaay2cQJsz36xVNyQUs=> .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://rx4rdf.sf.net/ns/archive#last-modified> "1057790874.703" .
<http://4suite.org/rdf/banonymous/5e3bc305-0fbb-4b67-b56f-b7d3f775dde6> <http://rx4rdf.sf.net/ns/wiki#summary> "lll" .
<urn:sha:jERppQrIlaay2cQJsz36xVNyQUs=> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#Contents> .
<urn:sha:jERppQrIlaay2cQJsz36xVNyQUs=> <http://rx4rdf.sf.net/ns/archive#sha1-digest> "jERppQrIlaay2cQJsz36xVNyQUs=" .
<urn:sha:jERppQrIlaay2cQJsz36xVNyQUs=> <http://rx4rdf.sf.net/ns/archive#hasContent> "        kkkk    &nbsp;" .
<urn:sha:jERppQrIlaay2cQJsz36xVNyQUs=> <http://rx4rdf.sf.net/ns/archive#content-length> "20" .
'''

    model2 = r'''<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#Contents> .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#sha1-digest> "ndKxl8RGTmr3uomnJxVdGnWgXuA=" .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#hasContent> " llll"@en-US .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#content-length> "5"^^http://www.w3.org/2001/XMLSchema#int .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#NamedContent> .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://rx4rdf.sf.net/ns/wiki#name> "HomePage" .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://rx4rdf.sf.net/ns/archive#created-on> "1057802436.437" .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://rx4rdf.sf.net/ns/archive#has-expression> <urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://rx4rdf.sf.net/ns/archive#last-modified> "1057802436.437" .
<http://4suite.org/rdf/banonymous/cc0c6ff3-e8a7-4327-8cf1-5e84fc4d1198> <http://rx4rdf.sf.net/ns/wiki#summary> "ppp" .'''

    loopModel = r'''<http://loop.com#r1> <http://loop.com#prop> <http://loop.com#r1>.
<http://loop.com#r2> <http://loop.com#prop> <http://loop.com#r3>.
<http://loop.com#r3> <http://loop.com#prop> <http://loop.com#r2>.'''
    
    model1NsMap = { 'rdf' : RDF_MS_BASE, 
                    'rdfs' : RDF_SCHEMA_BASE,
                    'bnode' : "bnode:",
                    'wiki' : "http://rx4rdf.sf.net/ns/wiki#",
                    'a' : "http://rx4rdf.sf.net/ns/archive#" }

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

    def RDFDoc(self, model, nsMap):    
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

        schemaClass = RDFSSchema
        schema = schemaClass(model)
        if isinstance(schema, Model):
            model = schema
            schema.findCompatibleStatements = False

        return model
        
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
        #ugh can't figure out how to close an open store!
        #if hasattr(self,'rdfDom'):
        #    del self.rdfDom.model.model._storage
        #    import gc; gc.collect()             

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
        dest = tempfile.mktemp()
        if type == 'rdf':
            type = 'xml'
        return initRDFLibModel(dest, source, type)

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

    def getModel(self, source, type='nt'):
        model = self.loadModel(source, type)
        self.nsMap = {u'http://rx4rdf.sf.net/ns/archive#':u'arc',
               u'http://www.w3.org/2002/07/owl#':u'owl',
               u'http://purl.org/dc/elements/1.1/#':u'dc',
               }
        return self.RDFDoc(model, self.nsMap)
       
    def tearDown(self):
        if DRIVER == 'Tyrant':
            from basicTyrantTest import stop_tyrant_server
            stop_tyrant_server(self.tyrant)
            self.tyrant = None

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

    def testSerialize(self):
        model = r'''<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#Contents> .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#sha1-digest> "ndKxl8RGnTmr3u/omnJxVdGnWgXuA=" .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#hasContent> " llll"@en-US .
<urn:sha:ndKxl8RGTmr3uomnJxVdGnWgXuA=> <http://rx4rdf.sf.net/ns/archive#content-length> "5"^^http://www.w3.org/2001/XMLSchema#int .
_:1 <http://rx4rdf.sf.net/ns/wiki#name> _:1 .
_:1 <http://rx4rdf.sf.net/ns/wiki#name> _:2 .
'''
        model = self.loadModel(cStringIO.StringIO(model), 'nt')
        stmts = model.getStatements()
        for stype in ['ntriples', 'ntjson', 'sjson', 'mjson']:
            #print 'stype', stype
            options = {}
            if stype == 'mjson':
                options = dict(blobmax=30)
            json = serializeRDF(stmts, stype, options=options)
            if stype in ['sjson', 'mjson']:
                #print stype
                #print json
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
            self.failUnless(stmts == newstmts, pprintdiff(stmts, newstmts ))

    def XXXtestSubtype(self):        
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
        startmodel = model = self.getModel(cStringIO.StringIO(model) )
        #we're testing the model directly so set this True:
        model.findCompatibleStatements = True               
        if isinstance(model.models[0], RxPathGraph.NamedGraphManager):
            model = model.models[0].managedModel
        else:
            model = model.models[0]
        #print 'all',  model.getStatements()
        def getcount(obj):
            stmts = startmodel.getStatements(
                predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
                object=obj)
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
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)

        #already has this type, so adding prop on this resource shouldn't change anything
        model.addStatements([
        Statement('bnode:O4', 'test:prop', 'test2'),
        ])
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)
        #neither should removing it
        model.removeStatements([
        Statement('bnode:O4', 'test:prop', 'test2'),
        ])
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)

        #add a subproperty rule and an resource with that property
        model.addStatements([
        Statement('test:subprop', RDF_SCHEMA_BASE+u'subPropertyOf', 'test:prop', 'R'),
        Statement('bnode:D6', 'test:subprop', 'test'),
        ])
        #subproperty should trigger entailments too
        self.failUnless(getcount('bnode:A') == 3)
        self.failUnless(getcount('bnode:D') == 6)

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
        self.failUnless(getcount('bnode:A') == 2)
        self.failUnless(getcount('bnode:D') == 5)

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


    def XXXtestSubproperty(self):        
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
        #we're testing the model directly so set this True:
        model.findCompatibleStatements = True

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
        addStatements(model, [stmt]) #XXX
        self.assertEquals(getcount(a+'F'), 5)
        
        #now let rollback those changes and redo the queries --
        #the results should now be the same as the first time we ran them
        rdfDom.rollback()  #XXX
        self.assertEquals(getcount(a+'F'), 3)
        
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

class GraphRDFDomTestCase(RDFDomTestCase):
    testHistory = 'single'#, 'split' or '' (for no graph manager)
    graphManagerClass = RxPathGraph.NamedGraphManager

class MergeableGraphRDFDomTestCase(RDFDomTestCase):
    testHistory = 'single'#, 'split' or '' (for no graph manager)
    graphManagerClass = RxPathGraph.MergeableGraphManager

class SplitGraphRDFDomTestCase(RDFDomTestCase):
    testHistory = 'split' #'single', 'split' or '' (for no graph manager)
    graphManagerClass = RxPathGraph.NamedGraphManager

class MergeableSplitGraphRDFDomTestCase(RDFDomTestCase):
    testHistory = 'split' #'single', 'split' or '' (for no graph manager)
    graphManagerClass = RxPathGraph.MergeableGraphManager
                        
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
        tc = RDFDomTestCase(test)
        tc.setUp()
        testfunc = getattr(tc, test)
        if profile:
            profilerRun(test, testfunc)
        else:
            testfunc() #run test


