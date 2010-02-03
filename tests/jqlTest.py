'''
Basic JQL tests -- see jqltester module for usage
'''

import sys
sys.path.append('.')
from jqltester import *

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

#XXX: consider that unreferenced anonymous object are left out of results
#XXX: consider that an id'd object with no properties is left out

t.group = 'smoke'

t('{*}',
[ {'foo': 'bar', 'id': '3'},
  {'foo': 'bar', 'id': '2'},
   {'child': '2', 'id': '_:2', 'parent': '1'}, 
  {'child': '3', 'id': '_:1', 'parent': '1'},
])

t('''
[*]
''',
[['bar'], ['bar'], ['1', '2'], ['1', '3']]
)

t("{id}", [ {'id': '3'}, {'id': '2'}, {'id': '_:2'}, {'id': '_:1'}, ])

t("{}", [{}]) 

t("(foo)",['bar', 'bar']) 

t("(id)",['3', '2', '_:2', '_:1'])

t("('constant')", ['constant'])

#XXX AssertionError: pos 0 but not a Filter: <class 'jql.jqlAST.Join'>
skip('''
{ "staticprop" : ["foo"] }
''',[{ "staticprop" : ["foo"] }])

t('''
{ "staticprop" : "foo" }
''', 
[{'staticprop': 'foo'}])

t('''{ * where ( foo > 'bar') }''', [])

t('''{ * where ( id = @id) }''', 
[{'foo':'bar', 'id':'2'}], 
                bindvars={'id':'2'})

t('''{ * where ( child = @child) }''', 
[{'child': '2', 'id': '_:2', 'parent': '1'}], 
        bindvars={'child':'2'})

t("{ id, 'parent' : child }",
[{'parent': '2', 'id': '_:2'}, {'parent': '3', 'id': '_:1'},])

t("{ parent : child, id }",
   [{'1': '2', 'id': '_:2'}, {'1': '3', 'id': '_:1'},])

t.group = 'joins'

t(
''' { ?childid,
        *
       where( {child = ?childid 
           })
    }
''', [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}])


t(
''' { ?parentid,        
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
  }],
ast=Select(Construct([     
    cp('derivedprop',  qF.getOp('mul', Project(0), Constant(2))),
    cp('children', Select(Construct([            
            cp(Project('*')),
            cs('id', 'childid'),
        ]))),
    cs('id', 'parentid'),
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
      id,
      'children' : { ?childid,
                   id, foo,
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
    cp('children', Select(Construct([            
            cp('foo', Project('foo')), #find all props
            cs('id', 'childid'),
        ]))),
    cs('id', 'parentid'),
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
{ ?childid,
      *, 
      'parent' : { ?parentid, id,
                   where( 
                   {child = ?childid and
                        parent = ?parentid
                    })
                 }
    }
''', [{'foo': 'bar', 'id': '3', 'parent': {'id': '1'}},
 {'foo': 'bar', 'id': '2', 'parent': {'id': '1'}}]
 )

t.group = 'orderby'

t('''{ * orderby(child) }''',
[{'foo': 'bar', 'id': '3'}, #note: nulls go first
 {'foo': 'bar', 'id': '2'},
 {'child': '2', 'id': '_:2', 'parent': '1'},
 {'child': '3', 'id': '_:1', 'parent': '1'}]
)

t('''{ * orderby(id desc) }''',
[{'child': '2', 'id': '_:2', 'parent': '1'},
 {'child': '3', 'id': '_:1', 'parent': '1'},
 {'foo': 'bar', 'id': '3'},
 {'foo': 'bar', 'id': '2'}]
)

res = [{'child': '3', 'id': '_:1', 'parent': '1'},
 {'child': '2', 'id': '_:2', 'parent': '1'},
 {'foo': 'bar', 'id': '2'},
 {'foo': 'bar', 'id': '3'}]
 
t('''{ * orderby(child desc, id) }''', res)
t('''{ * orderby(child desc, id asc) }''', res)

t.group = 'parse'

t('''
{ 'id' : ID, 'blah' : foo }
''',
[{'blah': 'bar', 'id': '3'}, {'blah': 'bar', 'id': '2'}]
)

t(r'''
("unicode\x0alinefeed")
''', ["unicode\nlinefeed"])

t(r'''
("unicode\u000alinefeed")
''', ["unicode\nlinefeed"])

syntaxtests = [
#test qnames:
'''{ rdfs:comment:* where(rdfs:label='foo')}''', #propname : *
'''
[rdfs:comment where(rdfs:label='foo')]
''',
'''
(rdfs:comment where(rdfs:label='foo'))
''',
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
?artist,
foo : { ?join, id },
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
t('{*,}', ast=jql.buildAST('{*}')[0])

#XXX this ast looks wrong:
skip('''{ *, 
    where(type=bar OR foo=*)
    }''', ast=jql.buildAST("{ * where(type=bar or foo=*) }")[0]) 

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

#XXX some more (maybe redundent) tests:
'''
{foo : bar} #construct foo, where foo = bar
{"foo" : "bar"} #construct "foo" : "bar"
{"foo" : bar}  #construct foo, value of bar property (on this object)
#construct foo where value of baz on another object
{foo : ?child.baz
    where ({ id=?child, bar="dd"})
}

#construct foo with a value as the ?child object but only matching baz property
{'foo' : {  ?child, * }
    where ( foo = ?child.baz)
}

#same as above, but only child id as the value
{'foo' : ?child
    where ( foo = ?child.baz)
}
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
  "type" : "Tag"
},
{ "id" : "todo",
  "subsumedby" : "actions",
    "type" : "Tag"
},
{ "id" : "toread",
   "label" : "to read",
   "subsumedby" : "actions",
    "type" : "Tag"
},
{ "id" : "projects",
  "type" : "Tag"
},
{'id' : 'commons',
  "label" : "commons",
   "subsumedby" : "projects",
    "type" : "Tag"
},
{'id':'rhizome',
 "subsumedby" : "projects",
  "type" : "Tag"
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

#XXX: AssertionError: cant find 0 in SimpleTupleset 0xd6c650 for group by '#0' [ColInfo('', <type 'object'>), ColInfo('#0', MutableTupleset[])]
skip('''
{
groupby('id', display=merge)
}
''')

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
[{'content': ['some text about the commons', 
              'some more text about the commons'],
  'subject': 'commons'},
 {'content': 'some text about rhizome', 
  'subject': 'rhizome'}
]
)

#leave the groupby property out of the display list
t('''{
content
groupby(subject)
}
''', [{'content': ['some text about the commons',
              'some more text about the commons',
              ]},
 {'content': 'some text about rhizome'}]
 )

t('''{
 subject, 
 'count' : count(content)
 groupby('subject')
 }
 ''', 
 [{'count': 2, 'subject': 'commons'}, {'count': 1, 'subject': 'rhizome'}])

t('''{
  subject, 
  'count' : count(*), 
  'count2': count(subject)
  groupby(subject)
  }
  ''',
[{'count': 2, 'count2': 2, 'subject': 'commons'}, {'count': 1, 'count2': 1, 'subject': 'rhizome'}])

#XXX re-enable!
#expression needs to be evaluated on each item
#XXX consolidate filters -- multiple references to the same property create 
#duplicate filters and joins
skip('''
{
key,
'valTimesTypeDoubled' : val*type*2, #(-2*1, -4*1, 2*2, 4*2)*2
'sumOfType1' : sum(if(type==1, val, 0)), #-2 + -4 = -6
'sumOfType2' : sum(if(type==2, val, 0)),  # 2 + 4  = 6
'differenceOfSums' : sum(if(type==1, val, 0)) - sum(if(type==2, val, 0))
groupby(key) 
}
''',
[{'key': 1,
  'sumOfType1': -6,
  'sumOfType2': 6,
  'differenceOfSums': -12.0,
  'valTimesTypeDoubled': [-4.0, -8.0, 8.0, 16.0]},
 {'key': 10,
  'sumOfType1': -60,
  'sumOfType2': 60,
  'differenceOfSums': -120.0,
  'valTimesTypeDoubled': [-40.0, -80.0, 80.0, 160.0]}
],  
model = [dict(key=key, type=type, 
  val=(type%2 and -1 or 1) * key * val)
for key in 1,10 for type in 1,2 for val in 2,4]
)


t.group = 'recurse'

t('''{
?parent,
id,
'contains' : [{ id where(subsumedby = ?parent)}]
}
''',
[{'contains': [{'id': 'commons'}, {'id': 'rhizome'}], 'id': 'projects'},
 {'contains': [{'id': 'toread'}, {'id': 'todo'}], 'id': 'actions'}])

t('''
{ ?tag, * 
where (?tag in ('foo', 'commons'))
}
''',
[{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}])

t('''
{ *
where (id = ?tag and ?tag in ('foo', 'commons'))
}
''',
[{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}])

t('''
{ id
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

 #the nested construct will not have a where clause
 #but we have a joincondition referencing that object
t('''
 { 'inner' : { ?foo, id }
   where ( ?foo = subject) }
 ''',
 [{'inner': {'id': 'commons'}},
  {'inner': {'id': 'commons'}},
  {'inner': {'id': 'rhizome'}}]
 , name='labeled but no where'
 )

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
      Filter(In(Project(0), qF.getOp('recurse',Constant('commons'),Constant('subsumedby'))), subjectlabel='#0')
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
            Filter(In(Project(0), qF.getOp('recurse',Constant('commons'),Constant('subsumedby'))), subjectlabel='#0')
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
{'blah': [{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}],
  'content': 'some text about the commons',
  },

 {'blah': [{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}],
  'content': 'some more text about the commons',
  },  
  ]
)

t('''{ content, 
 where ({id = ?tag and ?tag = 'commons'} and subject = ?tag) }
''',
[
{'content': 'some text about the commons'},
 {'content': 'some more text about the commons'}, 
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
[
 {'content': 'some text about the commons', 'subject': 'commons'},
{'content': 'some more text about the commons',
  'subject': 'commons'},
 {'content': 'some text about rhizome', 'subject': 'rhizome'}]
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
[{'content': 'some text about the commons',
  'subject': 'commons'},
 {'content': 'some more text about the commons', 'subject': 'commons'}]
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
{ ?a,
  'foo' : { ?b where (?b > '1')}
    where (?a = '2')
}
'''
)

t('''{*}''')

t.group = 'not'

t('''
{ *
where (type = 'Tag' and not subsumedby)
}
''',
[{'id': 'actions', 'type': 'Tag'}, {'id': 'projects', 'type': 'Tag'}])

t('''
{ *
where (not subsumedby and type = 'Tag')
}
''',
[{'id': 'actions', 'type': 'Tag'}, {'id': 'projects', 'type': 'Tag'}])

t('''
{ *
where (not subsumedby)
}
''',
[{'content': 'some text about the commons', 'subject': 'commons'},
 {'content': 'some more text about the commons', 'subject': 'commons'},
 {'content': 'some text about rhizome', 'subject': 'rhizome'},
 {'id': 'actions', 'type': 'Tag'},
 {u'http://www.w3.org/2000/01/rdf-schema#domain': 'Tag',
  u'http://www.w3.org/2000/01/rdf-schema#range': 'Tag',
  u'http://www.w3.org/2000/01/rdf-schema#subPropertyOf': u'http://www.w3.org/2000/01/rdf-schema#subClassOf',
  'id': 'subsumedby'},
 {u'http://www.w3.org/2000/01/rdf-schema#subClassOf': u'http://www.w3.org/2000/01/rdf-schema#Class',
  'id': 'Tag'},
 {'id': 'projects', 'type': 'Tag'}]
)

t('''
{ *
where (type = 'Tag' and not not subsumedby)
}
''',
[{'id': 'toread', 'label': 'to read', 'subsumedby': 'actions', 'type': 'Tag'},
 {'id': 'todo', 'subsumedby': 'actions', 'type': 'Tag'},
 {'id': 'commons',
  'label': 'commons',
  'subsumedby': 'projects',
  'type': 'Tag'},
 {'id': 'rhizome', 'subsumedby': 'projects', 'type': 'Tag'}]
)

#XXX test circularity
t.group = 'depth'
t('''{*
DEPTH 1
}''')

t('''{*
DEPTH 10
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

t('{*}', 
[{'id': '1',
  'values': {'id': '_:j:e:object:1:1',
             'prop1': 'foo',
             'prop2': 3,
             'prop3': None,
             'prop4': True}},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2',
  'values': {'id': '_:j:e:object:2:2',
             'prop1': 'bar',
             'prop2': None,
             'prop3': False,
             'prop4': '',
             'prop5': 0}},
 {'id': '4', 'values': [1, '1', 1.1000000000000001]}],
forUpdate=True)

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
 ]
)

#make sure rows with null values aren't filtered out when proper 
t('''{ prop3 }''',
[{'prop3': None}, {'prop3': False}]) 

#XXX shouldn't match null values for next two but it currently does
skip('''{  prop3 where (prop3) }''', [])
skip('''{  * where (prop3) }''',[])

#XXX implement exists()
skip('''{ prop3 where( exists(prop3) ) }''',
[{'prop3': False}, {'prop3': None}]) 

skip('''{  * where ( exists(prop3) ) }''',
[{'prop1': 'bar', 'prop2': None, 'prop3': False, 'prop4': '', 'prop5': 0},
 {'prop1': 'foo', 'prop2': 3, 'prop3': None, 'prop4': True}])

#model = firstmodel
skip('''
{
'foo_or_child' : foo or child
where (exists(foo) or exists(child)) 
}
''', 
[{'id': '2', 'foo_or_child': 'bar'}, {'id': '_:1', 'foo_or_child': '1'}])


t('''{
id,
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

#test multivalued properties without any associated json list info
t.group = 'multivalue'

t.model = vesper.data.store.basic.MemStore([
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
[{ 'multivalued': [0, 'a', 'b', []]}])

t('{ multivalued }',
[{ 'multivalued': [0, 'a', 'b', []]}])

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
[
 {'id': '1',
  'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']],
  'listprop2': [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11],
  'listprop3': [1],
  'listprop4': []},
 {'id': '2',
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1],
  'listprop2': [[['double nested']]],
  'listprop3': [],
  'listprop4': [[]]}
])

t('{ "listprop" : listprop}',
[{'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']]},
 {
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1]}]
)

t('{ listprop, listprop2, listprop3, listprop4 }',
[{
  'listprop': ['b', 'a', [[]], [{'foo': 'bar'}, 'another']],
  'listprop2': [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11],
  'listprop3': [1],
  'listprop4': []},
 {
  'listprop': ['b', [-1, 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11], [], 'a', 1],
  'listprop2': [[['double nested']]],
  'listprop3': [],
  'listprop4': [[]]}]
)


#XXX t.group = 'exclude'
#html5 json microdata representation:
#issue: @ in type's value, need a way to control that (by prop)
'''
{
?parent
id, type,
'properties' : { 
   *, exclude(id, type) 
   where (id=?parent)
  }
}
'''

t.group = 'outer'

#XXX join from foo property masks outer join 
skip('''{
    foo 
    where(maybe foo = 1)
    }
''')

t.model = modelFromJson([
        { "label": "tag 1", 'id': 'tag1'},
        { "parent":"1", "child":"3", 'id': '_:1'},
        { "id" : "2", "type" : "post", "tags" : "tag1"},
        { "id" : "3", "type" : "post"}
    ])

t('''
{   id, 
    'tags' : {id where (id=?tag)}
    where (maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None},
 {'id': 'tag1', 'tags': None},
  {'id': '_:1', 'tags': None},
 {'id': '2', 'tags': {'id': 'tag1'}},
], unordered=True
)
 
#construct an array but note that nulls appear instead an empty list
t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None},
 {'id': 'tag1', 'tags': None},
 {'id': '_:1', 'tags': None},
 {'id': '2', 'tags': ['tag1']}]
, unordered=True)


#construct an array but also force a list so empty list appears instead of null
t('''
{   ?post, id, 
    'tags' : [[id where (id=?tag)]]
    where (maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': []},
 {'id': 'tag1', 'tags': []},
 {'id': '_:1', 'tags': []},
 {'id': '2', 'tags': ['tag1']}
]
, unordered=True)

#force a list so an empty list appears instead of a null
t('''
{   ?post, id, 
    'tags' : [{id where (id = ?tag)}]
    where (maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': []},
 {'id': 'tag1', 'tags': []},
 {'id': '2', 'tags': [{'id': 'tag1'}]},
 {'id': '_:1', 'tags': []}
]
, unordered=True)

t('''
{   *,
    'tags' : {id where (id=?tag)}   
    where (type='post' and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': {'id': 'tag1'}, 'type': 'post'}]
)

t('''
{   *, 
    'tags' : [id where (id=?tag)]
    where (maybe tags = ?tag and type='post')
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': ['tag1'], 'type': 'post'}]
, unordered=True)

t('''
{   *,
    'tags' : {* where (id=?tag)}   
    where (type='post' and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': {'id': 'tag1', 'label': 'tag 1'}, 'type': 'post'}]
)

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where ((maybe tags = ?tag) and type='post')
}
''',
[{'id': '3', 'tags': None}, {'id': '2', 'tags': ['tag1']}]
, unordered=True)


#XXX throws jql.QueryException: reference to unknown label(s): tag
#'tags' : ?tag should be treated like 'tags' : {?tag}  
skip('''
{ 
    'tags' : ?tag 
    where (tags = ?tag)
}
'''
)

t.group = 'onetomany'
t.model = modelFromJson([
        { "label": "tag 1", 'id': 'tag1'},
        { "label": "tag 2", 'id': 'tag2'},
        { "parent":"1", "child":"3", 'id': '_:1'},
        { "id" : "2", "type" : "post", "tags" : "tag1"},
        { "id" : "2", "type" : "post", "tags" : "tag2"},
        { "id" : "3", "type" : "post"}
    ])

t.skip = True #XXX these need to be fixed!

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (type='post' and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None},
 {'id': 'tag1', 'tags': None},
 {'id': '_:1', 'tags': None},
 {'id': '2', 'tags': ['tag1']}]
, unordered=True)


t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (tags = ?tag and type='post')
}''',[])

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

    def XXXtestSerializationClassOveride(self):
        '''
        test that query results always use the user specified list and dict classes
        '''
        #broken: evalProject and _setConstructProp don't create user-specified list shape
        
        #some hacky classes so we can call set() on the query results
        class hashabledict(dict):
            def __hash__(self):
                #print self
                return hash(tuple(sorted(self.items())))

            def __repr__(self):
                return 'HashableList'+super(hashablelist, self).__repr__()
        
        class hashablelist(list):
            def __hash__(self):
                return hash(tuple(self))

            def __repr__(self):
                return 'HashableList'+super(hashablelist, self).__repr__()

        try:
            save =jql.QueryContext.shapes
            jql.QueryContext.shapes = { dict:hashabledict, list:hashablelist}            
            set(jql.getResults("{*}", t.model).results)            
        finally:
            jql.QueryContext.shapes = save
                    
if __name__ == "__main__":
    import sys
    try:
        sys.argv.remove("--unittest")        
    except:
        main(t)
    else:
        unittest.main()