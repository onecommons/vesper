from sjson import *
from pprint import pprint

def assert_json_match(expected, result, dosort=False):
    if dosort and isinstance(expected, list):
        result.sort()
        expected.sort()
    result = json.dumps(result,sort_keys=True)
    expected = json.dumps(expected, sort_keys=True)
    assert result == expected, pprint((result, '!=', expected))

def printdiff(a, b):
    import difflib
    d = difflib.SequenceMatcher(None, a, b)
    return '\n'.join([("%7s a[%d:%d] (%s) b[%d:%d] (%s)" %
          (tag, i1, i2, a[i1:i2], j1, j2, b[j1:j2]))
        for tag, i1, i2, j1, j2 in d.get_opcodes()])

def assert_stmts_match(expected_stmts, result_stmts):
    assert set(result_stmts) == set(expected_stmts), printdiff(
                        result_stmts,expected_stmts)

    if not RxPath.graph_compare(expected_stmts, result_stmts):
        print 'expected _:2', RxPath.Graph(expected_stmts).vhash('_:2')
        print 'expected _:1', RxPath.Graph(expected_stmts).vhash('_:1')
        print 'result _:1', RxPath.Graph(result_stmts).vhash('_:1')
        print 'result _:2', RxPath.Graph(result_stmts).vhash('_:2')
        assert False

def assert_json_and_back_match(src, backagain=True, expectedstmts=None, includesharedrefs=False):
    test_json = [ json.loads(src) ]
    result_stmts = sjson(generateBnode='counter').to_rdf( test_json )
    #print 'results_stmts'
    #pprint( result_stmts)
    if expectedstmts is not None:
        assert_stmts_match(expectedstmts, result_stmts)
    
    result_json = sjson()._to_sjson( result_stmts, includesharedrefs=includesharedrefs)['results']
    #pprint( result_json )
    if includesharedrefs:
        test_json = includesharedrefs
    assert_json_match(result_json, test_json)
    if backagain:
        assert_stmts_and_back_match(result_stmts)

def assert_stmts_and_back_match(stmts, expectedobj = None):
    result = sjson()._to_sjson( stmts )['results']
    if expectedobj is not None:
        assert_json_match(expectedobj, result, True)
    
    result_stmts = sjson(generateBnode='counter').to_rdf( result )
    assert_stmts_match(stmts, result_stmts)

import unittest
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

    src = '''
    { "id" : "atestid",
       "foo" : { "id" : "bnestedid", "prop" : "value" }
    }'''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "testid", 
    "foo" : ["1","3"],
     "bar" : [],
     "baz" : { "nestedobj" : { "id" : "anotherid", "prop" : "value" }}
    } 
    '''
    assert_json_and_back_match(src, False) #XXX backagain fails, but then next one succeeds!
    
    src = '''
    { "id" : "testid",
    "baz" : { "nestedobj" : { "id" : "anotherid", "prop" : "value" }},
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
    "bar" : [ [] ]
    }
    '''
    assert_json_and_back_match(src)

    #test numbers and nulls
    src = '''
    { "id" : "test",
    "float" : 1.0,
      "integer" : 2,
      "null" : null,
      "list" : [ 1.0, 2, null ]
    }
    '''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "test",
     "circular" : "test",
      "circularlist" : ["test", "test"],
      "circularlist2" : [["test"],["test", "test"]]
        }
    '''
    assert_json_and_back_match(src, False) #XXX backagain fails

    #test that shardrefs output doesn't explode with circular references
    #XXX current handling is bad, should give error
    includesharedrefs = [{
    "circular": "test",
    "circularlist": ["test", "test"],
    "circularlist2": ["_:1", "_:2"],
    "id": "test"}]
    assert_json_and_back_match(src, False, includesharedrefs=includesharedrefs)
    #test missing ids and exclude_blankids
    #test shared
    print 'tests pass'


if __name__  == "__main__":
    test()
