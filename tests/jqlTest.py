import jql, sjson
from jql.jqlAST import *
from rx import RxPath
import sys, pprint
from rx.utils import flatten

import jql.jqlAST
import jql.engine

#aliases for convenience
jc = JoinConditionOp
cs = ConstructSubject
def cp(name, *args, **kw):
    #print 'cp', name, args
    if not args:
        #no name
        return ConstructProp(None, name, **kw)
    if isinstance(name, str):        
        kw['nameFunc']=Constant(name)
    return ConstructProp(None, *args, **kw)

def modelFromJson(model):
    model = sjson.sjson(generateBnode='counter').to_rdf(model)
    model = RxPath.MemModel(model)
    model.bnodePrefix = '_:'
    return model

'''
todo: query tests:

*joins:
outer joins (maybe())
semi-join (in)
anti-join (not in)
* unions (or)
* intersect (not)

* construction:
id keys only (not objects)
'''

class Test(object):
    def __init__(self, attrs):
        self.__dict__.update(attrs)

class Suite(object):
    defaults = dict(ast=None, rows=None, result=None, skip=False,
                skipParse=False, model=None, name=None, query=None, group=None)

    def __init__(self):
        self.tests = []

    def __call__(self, query=None, results=None, **kw):
        '''
        optional arguments:
        rows: test the tupleset result matches this
        results : test the result of query execution matches this
        name: name the test
        '''
        defaults = self.defaults.copy()
        defaults.update(self.__dict__)
        defaults.update(query=query, results=results)
        defaults.update(kw)
        t = Test(defaults)
        self.tests.append(t)
        return t

    def __iter__(self):
        for t in self.tests:
            yield t

t = Suite()
skip = Suite()

###################################
########### basic tests ###########
###################################
t.model = modelFromJson([
        { "parent":"1", "child":"2", 'id': '_:2'},
        { "parent":"1", "child":"3", 'id': '_:1'},
        { "id" : "1"},
        { "id" : "2", "foo" : "bar"},
        { "id" : "3", "foo" : "bar"}
    ])

t.group = 'smoke'
t('''
[*]
''',
[['bar'], ['bar'], ['1', '2'], ['1', '3']]
)

#XXX: AssertionError: cant find 0 in SimpleTupleset 0xd6c650 for group by '#0' [ColInfo('', <type 'object'>), ColInfo('#0', MutableTupleset[])]
skip('''
{
groupby('id', display=merge)
}
''')

t('{*}',
[{'foo': 'bar', 'id': '3'},
 {'foo': 'bar', 'id': '2'},
 {'child': '2', 'id': '_:2', 'parent': '1'},
 {'child': '3', 'id': '_:1', 'parent': '1'}])

t('''{ * where (foo > 'bar') }''')

#   [{'1': '2', 'id': '_:2'}, {'1': '3', 'id': '_:1'}])

t("{ 'parent' : child }",
[{'parent': '2', 'id': '_:2'}, {'parent': '3', 'id': '_:1'}])
#   [{'parent': '2', 'id': '_:2'}, {'parent': '3', 'id': '_:1'}])

t("{ parent : child }",
   [{'1': '2', 'id': '_:2'}, {'1': '3', 'id': '_:1'}])

t(
''' { id : ?childid,
        *
       where( {child = ?childid 
           })
    }
''', [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}])


t(
''' { id : ?parentid,
      'derivedprop' : id * 2,
      'children' : { ?childid,
                   *
                   where( {child = ?childid and
                        parent = ?parentid
                       })
                 }
    }
''',skipParse=0,
results = [{'children': [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}],
  'derivedprop': 2.0,
  'id': '1'}],
ast=Select(Construct([ 
    cs('id', 'parentid'),    
    cp('derivedprop',  qF.getOp('mul', Project(0), Constant(2))),
    cp('children', Select(Construct([
            cs('id', 'childid'),
            cp(Project('*')) 
        ])))
    ]),
    Join(
 jc(
    Join(
    jc(
    Join(
    Filter(Eq('parent',Project(PROPERTY)), objectlabel='parent'),
    Filter(Eq('child',Project(PROPERTY)), objectlabel='child'),
    name='@1'
    ),
    Eq(Project('child'), Project(SUBJECT)) ), name='childid'
    ),
 Eq(Project('parent'), Project(SUBJECT)) ), name='parentid'
 )
),
#expected rows: id, (child, parent)
skiprows=[['1',
    [
      ['3', '_:1', '_:1'], ['2', '_:2', '_:2']
    ]
]]
)

t(
''' { ?parentid,
      'children' : { ?childid,
                   foo,
                   where( foo = 'bar' and 
                         {child = ?childid and
                        parent = ?parentid                    
                       })
                 }
    }
''',
[{'children': [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}],
  'id': '1'}],
  skipParse=0,
skipast=Select( #XXX fix
  Construct([
    cs('id', 'parentid'),
    cp('children', Select(Construct([
            cs('id', 'childid'),
            cp('foo', Project('foo')) #find all props
        ])))
    ]),
 Join( #row  : (subject, (subject, foo, ("child", ("child", "parent"))))
  jc(
    Join( #row : subject, foo, ("child", ("child", "parent"))
     Filter(Eq(Project(OBJECT),'bar'), Eq(Project(PROPERTY),'foo'), objectlabel='foo'),
     jc(Join( #row : subject, ("child", "parent")
       Filter(Eq('parent',Project(PROPERTY)), objectlabel='parent'),
       Filter(Eq('child', Project(PROPERTY)), objectlabel='child'),
       name = '@1'
       ),'child')
    , name='childid'),
    'parent'),  #this can end up with child cell as a list
    name='parentid'
)
),
#expected results (id, (child, foo), parent)
skiprows=[['1',
    [
       ['3', [['bar']], '3', '_:1', '_:1'],
       ['2', [['bar']], '2', '_:2', '_:2']
    ]
]]
)

t('''
{ id : ?childid,
      *, 
      'parent' : { id : ?parentid,
                   where( 
                   {child = ?childid and
                        parent = ?parentid
                    })
                 }
    }
''', [{'foo': 'bar', 'id': '3', 'parent': {'id': '1'}},
 {'foo': 'bar', 'id': '2', 'parent': {'id': '1'}}]
 )

t.group = 'parse'

t('''
{ 'blah' : foo }
''',
[{'blah': 'bar', 'id': '3'}, {'blah': 'bar', 'id': '2'}]
)

syntaxtests = [
#test qnames:
'''{ rdfs:comment:* where(rdfs:label='foo')}''',
'''
[rdfs:comment where(rdfs:label='foo')]
'''
]

#XXX fix failing queries!
failing = [
#triggers listconstruct not forcelist, so different semantics than {'blah' : foo}
#and leads to AssertionError: pos 0 but not a Filter: <class 'jql.jqlAST.Join'>
'''
{ 'blah' : [foo] }
'''
,
'''{ 'ok': */1 }''',
#throws join in filter not yet implemented:
'''{* where (foo = { id = 'd' }) }''',

#XXX there's ambguity here: construct vs. join (wins)
# throws AssertionError: pos 0 but not a Filter: <class 'jql.jqlAST.Join'>
"{foo: {*} }", 
#XXXAssertionError: pos 0 but not a Filter: <class 'jql.jqlAST.Join'>
'''
{
id : ?artist,
foo : { id : ?join },
"blah" : [ {*} ]
where( {
    ?id == 'http://foaf/friend' and
    topic_interest = ?ss and
    foaf:topic_interest = ?artist.foo.bar #Join(
  })
GROUPBY(foo)
}
''',
#jql.QueryException: only equijoin supported for now:
"{*  where (foo = ?var/2 and {id = ?var and foo = 'bar'}) }",
]

for s in syntaxtests:
    t(s)

#XXX test broken, AST seems wrong
#XXX there's ambguity here: construct vs. forcelist (see similar testcase above)
skip("{'foo': [*]}", 
ast = Select( Construct([
  ConstructProp('foo', Project('*'),
        PropShape.uselist, PropShape.uselist)
      ]), Join())
)

#expect equivalent asts:
t('{*,}', ast=jql.buildAST('{*}'))

#XXX this ast looks wrong:
t('''{ *, 
    where(type=bar OR foo=*)
    }''', ast=jql.buildAST("{ * where(type=bar or foo=*) }")) 

#expect parse errors:
t("{*/1}", ast='error')  

#logs ERROR:parser:Syntax error at '}'
t("{*  where (foo = ?var/2 and {id = ?var and foo = 'bar'} }", ast='error')

#XXX filters to test:
'''
    foo = (?a or ?b)
    foo = (a or b)
    foo = (?a and ?b)
    foo = (a and b)
    foo = {c='c'}
    foo = ({c='c'} and ?a)
'''

#xxx fix when namespace and type support is better
SUBPROPOF = u'http://www.w3.org/2000/01/rdf-schema#subPropertyOf'
SUBCLASSOF = u'http://www.w3.org/2000/01/rdf-schema#subClassOf'
RDF_SCHEMA_BASE = u'http://www.w3.org/2000/01/rdf-schema#'
TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

t.model = modelFromJson([
#{ 'ns': nsMap },

{ "id" : "Tag",
   SUBCLASSOF : RDF_SCHEMA_BASE+'Class'
},
{
'id' : "subsumedby",
 SUBPROPOF : SUBCLASSOF,
 RDF_SCHEMA_BASE+'domain' : 'Tag',
 RDF_SCHEMA_BASE+'range' : 'Tag'
},
{ "id" : "actions",
  TYPE : "Tag"
},
{ "id" : "todo",
  "subsumedby" : "actions"
},
{ "id" : "toread",
   "label" : "to read",
   "subsumedby" : "actions"
},
{ "id" : "projects",
  TYPE : "Tag"
},
{'id' : 'commons',
   "subsumedby" : "projects"
},
{'id':'rhizome',
 "subsumedby" : "projects"
},
{
'subject': 'commons',
'content' : 'some text about the commons'
},
{
'subject': 'commons',
'content' : 'some more text about the commons'
},
{
'subject': 'rhizome',
'content' : 'some text about rhizome'
}

])

t.group = 'groupby'
# XXX * is broken: need evalProject not to assume id is SUBJECT
skip('''{
*,  
groupby('subject', display=merge)
}
''')

t('''{
subject, 
content
groupby('subject')
}
''', 
[{'content': ['some more text about the commons',
              'some text about the commons'],
  'subject': 'commons'},
  {'content': 'some text about rhizome', 'subject': 'rhizome'}
]
)

#leave the groupby property out of the display list
t('''{
content
groupby(subject)
}
''', [{'content': ['some more text about the commons',
              'some text about the commons']},
 {'content': 'some text about rhizome'}]
 )

t.group = 'recurse'

t('''{
id : ?parent,
'contains' : [{ where(subsumedby = ?parent)}]
}
''',
[{'contains': [{'id': 'commons'}, {'id': 'rhizome'}], 'id': 'projects'},
 {'contains': [{'id': 'toread'}, {'id': 'todo'}], 'id': 'actions'}])

t('''
{ id : ?tag, * 
where (?tag in ('foo', 'commons'))
}
''',
[{'id': 'commons', 'subsumedby': 'projects'}])

t('''
{ *
where (id = ?tag and ?tag in ('foo', 'commons'))
}
''',
[{'id': 'commons', 'subsumedby': 'projects'}])

t('''
{ 
where (id not in ('foo', 'commons') and subsumedby)
}''',
[{'id': 'toread'},
 {'id': 'todo'},
 {'id': 'rhizome'}])

#XXX * not handle properly, matching property named '*', should match any value 
skip('''{ 
where (subsumedby = *)
}''',
[{'id': 'toread'},
 {'id': 'todo'},
 {'id': 'rhizome'},
 {'id': 'commons'},])

#throws: jql.QueryException: unlabeled joins not yet supported "tag"
skip('''
    {
    *
     where (subjectof= ?tag and
          {
            ?tag in recurse('commons', 'subsumedby')
           }
        )
    }
    ''',[])

#test recurse()
skip(ast=
Select(Construct([cp(Project('*'))]),
Join(
    JoinConditionOp(
      Filter(In(Project(0), jql.jqlAST.qF.getOp('recurse',Constant('commons'),Constant('subsumedby'))), subjectlabel='#0')
          ),               
     JoinConditionOp(
      Join(
        JoinConditionOp(
          Filter(Eq('subject',Project(1)), subjectlabel='#0', objectlabel='subject')
                       )
        ,name='@1') 
      ,'subject')
    )
)
)

skip(ast=
Select(Construct([cp(Project('*'))]),
Join(
    JoinConditionOp(
          Filter(Eq('subject',Project(1)), subjectlabel='#0', objectlabel='subject')          
    ),
    JoinConditionOp(
      Join(
        JoinConditionOp(
            Filter(In(Project(0), jql.jqlAST.qF.getOp('recurse',Constant('commons'),Constant('subsumedby'))), subjectlabel='#0')
                       )
        ,name='@1') 
        ,'subject')
,name='@1')
)
)

t('''{ *, 'blah' : [{ *  where(id = ?tag and ?tag = 'dd') }]
 where ( subject = ?tag) }
''',
[])

t('''{ * 
 where ({id = ?tag and ?tag = 'dd'} and subject = ?tag) }
''',
[])

t('''{ content,  
    'blah' : [{ *  where(id = ?tag and ?tag = 'commons') }]
 where ( subject = ?tag) }
''',
[
 {'blah': [{'id': 'commons', 'subsumedby': 'projects'}],
  'content': 'some more text about the commons',
  'id': '_:2'},
  
{'blah': [{'id': 'commons', 'subsumedby': 'projects'}],
  'content': 'some text about the commons',
  'id': '_:1'},
  ]
)

t('''{ content, 
 where ({id = ?tag and ?tag = 'commons'} and subject = ?tag) }
''',
[
 {'content': 'some more text about the commons', 'id': '_:2'},
 {'content': 'some text about the commons', 'id': '_:1'},
]
)

#find all the entries that implicitly or explicitly are tagged 'projects'
t('''
    {
    * 
     where (
          { id = ?tag and
            'projects' in recurse(?tag, 'subsumedby')
           }
           and subject= ?tag
        )
    }
    ''',
[{'content': 'some more text about the commons',
  'id': '_:2',
  'subject': 'commons'},
 {'content': 'some text about the commons', 'id': '_:1', 'subject': 'commons'},
 {'content': 'some text about rhizome', 'id': '_:3', 'subject': 'rhizome'}]
 )

#find all the entries that implicitly or explicitly are tagged 'commons'
t( '''
    {
    *
     where (subject= ?tag and
          { id = ?tag and
            ?tag in recurse('commons', 'subsumedby')
           }
        )
    }
    ''',
[{'content': 'some more text about the commons',
  'id': '_:2',
  'subject': 'commons'},
 {'content': 'some text about the commons', 'id': '_:1', 'subject': 'commons'}]
)

#throws jql.QueryException: only equijoin supported for now
skip( '''
    { *
     where (subjectof= ?tag and
          { id = ?start and id = 'commons' }
          and
          {
           id = ?tag and id in recurse(?start, 'subsumedby')
           }
        )
    }
    ''',[])

#XXX error:   AssertionError: pos 0 but not a Filter: <class 'jql.jqlAST.Join'>
# in JoinConditionOp.__init__
skip('''
{ ?a
    where (
        {id=?a and ?a = 1} and {id=?b and ?b = 2}
        and ?b = ?a
    )
}
'''
)

t('''
{ id : ?a,
  'foo' : { id : ?b where (?b > '1')}
    where (?a = '2')
}
'''
)

t('''{*}''')

#XXX test circularity
t.group = 'depth'
t('''{*
DEPTH 1
}''')

t.model = modelFromJson([
     { 'id' : '1',
       'values' :  {
          'prop1' : 'foo',
          'prop2' : 3,
          'prop3' : None,
          'prop4' : True,          
       }
     },
     { 'id' : '2',
       'values' :  {
          'prop1' : 'bar',
          'prop2' : None,
          'prop3' : False,
          'prop4' : '',
          'prop5' : 0,
       }
     },
    { 'id' : '3',
       'values' : ['', 0, None, False]
    },
    { 'id' : '4',
       'values' : [1,'1',1.1]
    },
    ]
)

t.group = 'types'

#XXX shouldn't including anoymous children in results
t('''{*}''',
[
     { 'id' : '1',
       'values' :  {
          'prop1' : 'foo',
          'prop2' : 3,
          'prop3' : None,
          'prop4' : True,          
       }
     },
    { 'id' : '3',
       'values' : ['', 0, None, False]
    },
     { 'id' : '2',
       'values' :  {
          'prop1' : 'bar',
          'prop2' : None,
          'prop3' : False,
          'prop4' : '',
          'prop5' : 0,
       }
     },
    { 'id' : '4',
       'values' : [1,'1',1.1]
    },
#shouldn't be here:
{'prop1': 'bar', 'prop2': None, 'prop3': False, 'prop4': '', 'prop5': 0},
 {'prop1': 'foo', 'prop2': 3, 'prop3': None, 'prop4': True}]
)

t('''{
values
}''',[{'id': '1',
  'values': {'prop1': 'foo', 'prop2': 3, 'prop3': None, 'prop4': True}},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2',
  'values': {'prop1': 'bar',
             'prop2': None,
             'prop3': False,
             'prop4': '',
             'prop5': 0}},
 {'id': '4', 'values': [1, '1', 1.1]}]
 )

#XXX
skip(name='labeled but no where',
#the nested construct will not have a where clause
#but we have a joincondition referencing that object
query='''
{ 'inner' : { id : ?foo }
  where ( ?foo = bar) }
''')

#test multivalued properties without any associated json list info
t.group = 'multivalue'

t.model = RxPath.MemModel([
 ('1', 'multivalued', 'b', 'R', ''),
 ('1', 'multivalued', 'a', 'R', ''),
 ('1', 'multivalued', '0', 'http://www.w3.org/2001/XMLSchema#integer', ''),
 ('1', 'multivalued', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil', 'R', ''),
 ('1', 'singlevalue', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil', 'R', ''),
])

t('{*}',
[{'id': '1', 'multivalued': [0, 'a', 'b', []], 'singlevalue': []}]
)

t('{ "multivalued" : multivalued }',
[{'id': '1', 'multivalued': [0, 'a', 'b', []]}])

t('{ multivalued }',
[{'id': '1', 'multivalued': [0, 'a', 'b', []]}])

t.group = 'lists'

t.model = modelFromJson([
     {
     'id' : '1',
     'listprop' : [ 'b', 'a', [[]], [{ 'foo' : 'bar'}, 'another'] ],      
     #include enough items to expose lexigraphic sort bugs
     'listprop2' : [-1, 0, 1,2,3,4,5,6,7,9,10,11],
     'listprop3' : [1],
     'listprop4' : [],
     },
     {
     'id' : '2',
     'listprop' : [ 'b', [-1, 0, 1,2,3,4,5,6,7,9,10,11], [], 'a', 1],
     'listprop2' : [ [ ['double nested'] ]],
     'listprop3' : [],
     'listprop4' : [[]],
     },     
    ])

t('{*}',
[[[]],
 [{'foo': 'bar'}, 'another'],
 ['double nested'],
 [['double nested']],
 [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11],
 {'id': '1',
  'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']],
  'listprop2': [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11],
  'listprop3': [1],
  'listprop4': []},
 {'id': '2',
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1],
  'listprop2': [[['double nested']]],
  'listprop3': [],
  'listprop4': [[]]},
 {'foo': 'bar'}
])

t('{ "listprop" : listprop}',
[{'id': '1', 'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']]},
 {'id': '2',
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1]}]
)

t('{ listprop, listprop2, listprop3, listprop4 }',
[{'id': '1',
  'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']],
  'listprop2': [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11],
  'listprop3': [1],
  'listprop4': []},
 {'id': '2',
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1],
  'listprop2': [[['double nested']]],
  'listprop3': [],
  'listprop4': [[]]}]
)

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(['--quiet'])

import logging
logging.basicConfig() 

def main(cmdargs=None):
    from optparse import OptionParser
    
    usage = "usage: %prog [options] [group name] [number]"
    parser = OptionParser(usage)
    for name, default in [('printmodel', 0), ('printast', 0), ('explain', 0),
        ('printdebug', 0), ('printrows', 0), ('quiet',0)]:
        parser.add_option('--'+name, dest=name, default=default, 
                                                action="store_true")
    (options, args) = parser.parse_args(cmdargs)
    options.num = -1
    options.group = None
    if args and args[0] != 'null':
        if len(args) > 1:
            options.group = args[0]
            options.num = int(args[1])
        else:            
            try:                        
                options.num = int(args[0])
            except:
                options.group = args[0]
    
    count = 0
    skipped = 0
    currentgroup = None
    groupcount = 0
    lastmodelid = None
    for (i, test) in enumerate(flatten(t)):
        if test.group != currentgroup:
            currentgroup = test.group
            groupcount = 0
        else:
            groupcount += 1
        if options.group and options.group != currentgroup:
            skipped += 1
            continue
        
        if options.num > -1:
            if options.group:
                if groupcount != options.num:
                    skipped += 1
                    continue
            elif i != options.num:
                skipped += 1
                continue
                
        if test.skip:
            skipped += 1
            continue
        count += 1

        if test.name:
            name = test.name
        elif test.group:
            name = '%s %d' % (test.group, groupcount)
        else:
            name = "%d" % i        

        if not options.quiet:
            print '*** running test:', name
            print 'query', test.query

        if options.printmodel and id(test.model) != lastmodelid:
            lastmodelid = id(test.model)
            print 'model'
            pprint.pprint(list(test.model))

        if test.ast:
            if not test.skipParse and test.query:
                testast = jql.buildAST(test.query)
                #jql.rewriteAST(testast)
                if not options.quiet: print 'comparing ast'
                if test.ast == 'error': #expect an error
                    assert testast is None, (
                        'not expecting an ast for test %d: %s' % (i,testast))
                else: 
                    assert testast == test.ast, (
                            'unexpected ast for test %d: %s \n %s'
                            % (i, findfirstdiff(testast, test.ast), testast))
                ast = testast
            else:
                ast = test.ast
        else:
            ast = jql.buildAST(test.query)
            assert ast, "ast is None, parsing failed"

        if options.printast:
            print "ast:"
            pprint.pprint(ast)

        if options.printrows or test.rows is not None:
            if ast:
                evalAst = ast.where            
                testrows = list(jql.evalAST(evalAst, test.model))
            else:
                testrows = None
        if options.printrows:
            print 'labels', evalAst.labels
            print 'rows:'
            pprint.pprint(testrows)        
        if test.rows is not None:
            assert test.rows== testrows,  ('unexpected rows for test %d' % i)

        if options.explain:
            print "explain plan:"
            explain = sys.stdout
        else:
            explain = None

        if not options.quiet: print "construct " + (options.printdebug and '(with debug)' or '')
        if ast:
            testresults = list(jql.evalAST(ast, test.model, explain=explain,
                                                    debug=options.printdebug))
        else:
            testresults = None
        if not options.quiet:        
            print "Construct Results:"
            pprint.pprint(testresults)

        if test.results is not None:
            assert test.results == testresults,  ('unexpected results for test %d' % i)#, [(k, type(k)) for k in testresults[0]])

    print '***** %d tests passed, %d skipped' % (count, skipped)

if __name__ == "__main__":
    main()