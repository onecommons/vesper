import unittest
from pprint import pprint,pformat

from vesper.sjson import *
from vesper.utils import pprintdiff

def assert_json_match(expected, result, dosort=False):
    if dosort and isinstance(expected, list):
        result.sort()
        expected.sort()
    result = json.dumps(result,sort_keys=True)
    expected = json.dumps(expected, sort_keys=True)
    assert result == expected, pprint((result, '!=', expected))

def assert_stmts_match(expected_stmts, result_stmts):
    assert set(result_stmts) == set(expected_stmts), pprintdiff(
                        result_stmts,expected_stmts)

    if not RxPath.graph_compare(expected_stmts, result_stmts):
        print 'graph_compare failed'
        print pprintdiff(ge._hashtuple(), gr._hashtuple())
        #print 'expected _:2', RxPath.Graph(expected_stmts).vhash('_:2')
        #print 'expected _:1', RxPath.Graph(expected_stmts).vhash('_:1')
        #print 'result _:1', RxPath.Graph(result_stmts).vhash('_:1')
        #print 'result _:2', RxPath.Graph(result_stmts).vhash('_:2')
        assert False

def assert_json_and_back_match(src, backagain=True, expectedstmts=None, includesharedrefs=False, intermediateJson=None, serializerNameMap=None):
    if isinstance(src, (str,unicode)):
        test_json = json.loads(src)
        if not test_json.get('sjson'):
            test_json = [test_json]
    else:
        test_json = src
    result_stmts = Parser(generateBnode='counter').to_rdf( test_json )
    #print 'results_stmts'
    #pprint( result_stmts)
    if expectedstmts is not None:
        assert_stmts_match(expectedstmts, result_stmts)
    
    result_json = Serializer(nameMap=serializerNameMap).to_sjson( result_stmts)
    if serializerNameMap is None:
        result_json = result_json['data']
    #pprint( result_json )
    if intermediateJson:
        test_json = intermediateJson
    assert_json_match(result_json, test_json)
    if backagain:
        assert_stmts_and_back_match(result_stmts,serializerNameMap=serializerNameMap)

def assert_stmts_and_back_match(stmts, expectedobj = None, serializerNameMap=None, addOrderInfo=True):
    result = Serializer(nameMap=serializerNameMap).to_sjson( stmts )
    #print 'serialized', result
    if expectedobj is not None:
        if serializerNameMap is None:
            compare = result['data']
        else:
            compare = result
        assert_json_match(expectedobj, compare, True)
    
    result_stmts = Parser(generateBnode='counter', addOrderInfo=addOrderInfo).to_rdf( result )
    assert_stmts_match(stmts, result_stmts)

class SjsonTestCase(unittest.TestCase):
    def testAll(self):
        test()
        
def test():
              
    dc = 'http://purl.org/dc/elements/1.1/'
    r1 = "http://example.org/book#1";     
    r2 = "http://example.org/book#2"; 
    stmts = [
    Statement(r1, dc+'title', u"SPARQL - the book",OBJECT_TYPE_LITERAL,''),
    Statement(r1, dc+'description', u"A book about SPARQL",OBJECT_TYPE_LITERAL,''),
    Statement(r2, dc+'title', u"Advanced SPARQL",OBJECT_TYPE_LITERAL,''),
    ]
    
    expected =[{'http://purl.org/dc/elements/1.1/description': 'A book about SPARQL',
            'http://purl.org/dc/elements/1.1/title': 'SPARQL - the book',
            'id': 'http://example.org/book#1'},
            {'http://purl.org/dc/elements/1.1/title': 'Advanced SPARQL',
            'id': 'http://example.org/book#2'}]
    
    assert_stmts_and_back_match(stmts, expected)
        
    stmts.extend( 
        [Statement("http://example.org/book#2", 'test:sequelto' , 'http://example.org/book#1', OBJECT_TYPE_RESOURCE),]
    )

    expected = [{"http://purl.org/dc/elements/1.1/title": "Advanced SPARQL",
    "id": "http://example.org/book#2",
    "test:sequelto": {
        "http://purl.org/dc/elements/1.1/description": "A book about SPARQL",
        "http://purl.org/dc/elements/1.1/title": "SPARQL - the book",
        "id": "http://example.org/book#1"
        }
    }]
    assert_stmts_and_back_match(stmts, expected)
    assert_stmts_and_back_match(stmts, addOrderInfo=False)

    src = '''
    { "id" : "atestid",
       "foo" : { "id" : "bnestedid", "prop" : "value" }
    }'''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "atestid2",
       "foo" : { "id" : "bnestedid", "prop" : "@ref" }
    }'''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "testid", 
    "foo" : ["1","3"],
     "bar" : [],
     "baz" : {  "id": "_:j:e:object:testid:1", 
                "nestedobj" : { "id" : "anotherid", "prop" : "value" }}
    } 
    '''
    assert_json_and_back_match(src)
    
    src = '''
    { "id" : "testid",
    "baz" : { "id": "_:j:e:object:testid:1", 
               "nestedobj" : { "id" : "anotherid", "prop" : "value" }},
    "foo" : ["1","3"],
     "bar" : []
    } 
    '''
    assert_json_and_back_match(src)

    #test nested lists and dups
    src = '''
    { "id" : "testid",
    "foo" : [1,  1, ["nested1",
                       { "id": "nestedid", "nestedprop" : [ "nested3" ] },
                    "nested2"], 1,
            3],
    "bar" : [ [] ],
    "one" : [1]
    }
    '''
    assert_json_and_back_match(src)

    #test numbers and nulls
    src = '''
    { "id" : "test",
    "float" : 1.0,
      "integer" : 2,
      "null" : null,
      "list" : [ 1.0, 2, null, 0, -1],
      "created": 1262662188016
    }
    '''
    assert_json_and_back_match(src)

    #test circular references
    src = '''
    { "id" : "test",
      "circular" : "@test",
      "not a reference" : "test",
      "circularlist" : ["@test", "@test"],
      "circularlist2" : [["@test"],["@test", "@test"]]
    }
    '''
    assert_json_and_back_match(src)

    #test a custom ref pattern 
    #and then serialize with the same pattern
    #they should match
    src = r'''
    { "sjson" : "%s",
    "namemap" : { "refs" : "ref:(\\w+)"},
    "data" :[{ "id" : "test",
     "circular" : "ref:test",
     "not a reference" : "@test",
      "circularlist" : ["ref:test", "ref:test"],
      "circularlist2" : [["ref:test"],["ref:test", "ref:test"]]
        }]
    }
    ''' % VERSION
    serializerNameMap={ "refs" : "ref:(\\w+)"}
    assert_json_and_back_match(src, serializerNameMap=serializerNameMap)

    #add statements that are identical to the ones above except they have
    #different object types (they switch a resource (object reference) for literal
    #and vice-versa)
    stmts.extend( 
        [Statement("http://example.org/book#2", 'test:sequelto' , 
            'http://example.org/book#1', OBJECT_TYPE_LITERAL),
         Statement(r2, dc+'title', u"Advanced SPARQL",OBJECT_TYPE_RESOURCE,''),
        ]
    )
    
    assert_stmts_and_back_match(stmts, addOrderInfo=False)
    
    src = dict(namemap = dict(id='itemid', namemap='jsonmap'),
    itemid = 1,
    shouldBeARef = '@hello', #default ref pattern is @(URIREF)
    value = dict(jsonmap=dict(id='anotherid', refs=''), #disable matching
            anotherid = 2,
            #XXX fix assert key != self.ID, (key, self.ID) when serializing
            #id = 'not an id', #this should be treated as a regular property
            innerobj = dict(anotherid = 3, shouldBeALiteral='@hello2'),
            shouldBeALiteral='@hello')
    )
    #expect different output because we don't use the namemaps when serializing
    #and because ids are coerced to strings
    intermediateJson = [{"id": "1", "shouldBeARef": "@hello", 
            "value": {"id": "2", 
                "innerobj": {"id": "3", 
                            "shouldBeALiteral": {"type": "literal", "value": "@hello2"}
                            }, 
                "shouldBeALiteral": {"type": "literal", "value": "@hello"}
                }
            }]
    assert_json_and_back_match(src, intermediateJson=intermediateJson)
    
    #############################################
    ################ scope/context tests
    #############################################
    
    src = [{"id": "1",
      "context" : "context1",
      "prop1": 1,
      "prop2": ["@a_ref", "a value"]
    }]
    stmts = [('1', 'prop1', u'1', 'http://www.w3.org/2001/XMLSchema#integer', 'context1'), StatementWithOrder('1', 'prop2', 'a value', 'L', 'context1', (1,)), StatementWithOrder('1', 'prop2', 'a_ref', 'R', 'context1', (0,)), ('1', 'sjson:schema#propseq', 'bnode:j:proplist:1;prop2', 'R', 'context1'), ('bnode:j:proplist:1;prop2', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#_1', 'a_ref', 'R', 'context1'), ('bnode:j:proplist:1;prop2', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#_2', 'a value', 'L', 'context1'), ('bnode:j:proplist:1;prop2', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq', 'R', 'context1'), ('bnode:j:proplist:1;prop2', u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'sjson:schema#propseqtype', 'R', 'context1'), ('bnode:j:proplist:1;prop2', 'sjson:schema#propseqprop', 'prop2', 'R', 'context1'), ('bnode:j:proplist:1;prop2', 'sjson:schema#propseqsubject', '1', 'R', 'context1')]    
    assert Parser().to_rdf(src) == stmts
    assert_json_and_back_match(src)

    src = [{"id": "1",
      "context" : "context1",
      "prop1": 1,
      "prop2": ["a_ref", 
                { 'context' : 'context2', 'prop3' : None, "id": "_:j:e:object:1:1"}
               ],
     'prop4' : { 'type' : 'literal', 'value' : 'hello', 'context' : 'context3'}
    }]
    assert_json_and_back_match(src)

    src = [{
      "sjson": "0.9", 
      "namemap": {
        "refs": "@(URIREF)"
      },
      'context' : 'scope1'
    },
    {
      'id' : 'id1',
      'prop1': 1,
      'prop2': "@ref"
    },
    {
      "sjson": "0.9", 
      'context' : ''
    },    
    {
      'id' : 'id1', #note: same id
      'prop1': 1,
      'prop2': "@ref"
    },    
    ]
    assert_json_and_back_match(src, False, 
    [('id1', 'prop1', u'1', 'http://www.w3.org/2001/XMLSchema#integer', 'scope1'),
     ('id1', 'prop2', 'ref', 'R', 'scope1'),
     ('id1', 'prop1', u'1', 'http://www.w3.org/2001/XMLSchema#integer', ''),
      ('id1', 'prop2', 'ref', 'R', '')     
     ],    
    intermediateJson=[{"id": "id1", "prop1": [1, {"context": "scope1", 
        "datatype": "http://www.w3.org/2001/XMLSchema#integer", 
        "type": "typed-literal", 
        "value": "1"}], 
      "prop2": [ "@ref", {"context": "scope1", "type": "uri", "value": "ref"}]
      }]
    )

    src = [{
       'id' : 'resource1',
       "value" : "not in a scope",
      "type":         
        {
          "type": "uri", 
          "context": "context:add:context:txn:http://pow2.local/;0A00001;;", 
          "value": "post"
        }
      , 
      "content-Type":         
        {
          "type": "literal", 
          "context": "context:add:context:txn:http://pow2.local/;0A00001;;", 
          "value": "text/plain"
        }      
     }]
    #XXX note intermediateJson: not ideal but correct   
    assert_json_and_back_match(src, intermediateJson=[{
    "content-Type": "text/plain", 
    "context": "context:add:context:txn:http://pow2.local/;0A00001;;", 
    "id": "resource1", 
    "type": "@post", 
    "value": {"context": "", "type": "literal", "value": "not in a scope"}}]) 

    #test duplicate statements but in different scopes
    src = [{ 'id' : 'id1', 
       "value" : "not in a scope",
      "type": [
        "@post", 
        {
          "type": "uri", 
          "context": "context:add:context:txn:http://pow2.local/;0A00001;;", 
          "value": "post"
        }
      ], 
      "content-Type": [
        "text/plain", 
        {
          "type": "literal", 
          "context": "context:add:context:txn:http://pow2.local/;0A00001;;", 
          "value": "text/plain"
        }
      ]
     }]
    assert_json_and_back_match(src) 
        
    print 'tests pass'


if __name__  == "__main__":
    test()
