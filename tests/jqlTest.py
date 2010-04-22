#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
Basic JQL tests -- see jqltester module for usage
'''

import sys
sys.path.append('.')
from jqltester import *

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

#id keys only
t("{id}", [ {'id': '3'}, {'id': '2'}, {'id': '_:2'}, {'id': '_:1'}, ])

t("{id : foo MERGEALL}", [{'2': 'bar', '3': 'bar'}])

t('''
{
 id : *
 MERGEALL
}
''',
[{'2': {'foo': 'bar', 'id': '2'},
  '3': {'foo': 'bar', 'id': '3'},
  '_:1': {'child': {'foo': 'bar', 'id': '3'}, 'id': '_:1', 'parent': '1'},
  '_:2': {'child': {'foo': 'bar', 'id': '2'}, 'id': '_:2', 'parent': '1'}}]
)

t("{}", [{}]) 

t("(foo)",['bar', 'bar']) 

t("(id)",['3', '2', '_:2', '_:1'])

t("('constant')", ['constant'])

t('''
{ "staticprop" : ["foo"] }
''',[{ "staticprop" : ["foo"] }])

t('''
{ "staticprop" : "foo" }
''', 
[{'staticprop': 'foo'}])

t('''{ * where ( foo > 'bar') }''', [])

t('''{ * where ( id = :id) }''', 
[{'foo':'bar', 'id':'2'}], 
                bindvars={'id':'2'})

t('''{ * where ( child = :child) }''', 
[{'child': '2', 'id': '_:2', 'parent': '1'}], 
        bindvars={'child':'2'})

t("{ id, 'parent' : child }",
[{'parent': '2', 'id': '_:2'}, {'parent': '3', 'id': '_:1'},])

t("{ parent : child, id }",
   [{'1': '2', 'id': '_:2'}, {'1': '3', 'id': '_:1'},])

t("[:b1, :b2]", 
[['1', '2']],
bindvars={'b1':'1', 'b2':'2'})

#use [[]] because { 'emptylist' : []} => {'emptylist': None}]
t("{ 'emptylist' : [[]]}",
[{'emptylist': []}]
)

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
skipast=Select(Construct([   #XXX
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
        Eq(Project('child'), Project(SUBJECT)) 
       ), name='childid'
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

t('''{ 'foo' : ?bar.baz.id }''')

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
#test comments
'''{
*
// a comment
}
''',
'''{
*
/* a comment */
}
''',
'''{
*
# a comment!!
}
''',
#test qnames:
'''{ <rdfs:comment>:* where(<rdfs:label>='foo')}''', #propname : *
'''
[<rdfs:comment> where(<rdfs:label>='foo')]
''',
'''
(<rdfs:comment> where(<rdfs:label>='foo'))
''',
#force list
'''
{ 'blah' : [foo] }
''',
'''{* where (foo = { id = 'd' }) }'''
]

#XXX fix failing queries!
failing = [
#  File "/_dev/rx4rdf/vesper/src/vesper/query/rewrite.py", line 236, in _getASTForProject
#    op.addLabel(project.varref)
#AttributeError: 'JoinConditionOp' object has no attribute 'addLabel'
'''{ 'foo' : ?bar.baz.biff }''',

'''{ 'ok': */1 }''',

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
    <foaf:topic_interest> = ?artist.foo.bar #Join(
  })
GROUPBY(foo)
}
''',
#jql.QueryException: only equijoin supported for now:
"{*  where (foo = ?var/2 and {id = ?var and foo = 'bar'}) }",
# = is non-associative so this is illegal -- at least have better error msg
"""
{ * where (?a = b = ?c)}
"""
]

for s in syntaxtests:
    t(s)

#XXX test broken, AST seems wrong
#XXX there's ambguity here: construct vs. forcelist (see similar testcase above)
skip("{'foo': [*]}", 
skipast = Select( Construct([
  ConstructProp('foo', Project('*'),
        PropShape.uselist, PropShape.uselist)
      ]), Join())
)

#expect equivalent asts:
t('{*,}', ast='{*}')

#XXX this ast looks wrong:
skip('''{ *, 
    where(type=bar OR foo=*)
    }''', ast="{ * where(type=bar or foo=*) }")

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
,{'id':'post1','tags' : ['commons', 'toread']}
])

t.group = 'namemap'

#XXX need to serialize based on namemap
t('''{
* 
where (<rdfs:range> = 'Tag')
orderby (<rdfs:range>)
namemap = {
 "props" : { 
      'rdf:' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      'rdfs:': 'http://www.w3.org/2000/01/rdf-schema#'
    }
}
}''',
[{u'http://www.w3.org/2000/01/rdf-schema#domain': 'Tag',
  u'http://www.w3.org/2000/01/rdf-schema#range': 'Tag',
  u'http://www.w3.org/2000/01/rdf-schema#subPropertyOf': u'http://www.w3.org/2000/01/rdf-schema#subClassOf',
  'id': 'subsumedby'}]
)

t.group = 'groupby'

#XXX return empty result
skip('''
{
id
groupby(id, display=merge)
}
''')

# XXX * is broken: need evalProject not to assume id is SUBJECT
skip('''{
*,  
groupby(subject, display=merge)
}
''')

t('''{
subject, 
content
groupby(subject)
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
 groupby(subject)
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

#expression needs to be evaluated on each item
#XXX consolidate filters -- multiple references to the same property create 
#duplicate filters and joins

groupbymodel = [dict(key=key, type=type, 
  val=(type%2 and -1 or 1) * key * val)
for key in 1,10 for type in 1,2 for val in 2,4]

t('''
{
key,
'sum' : sum(val),
'val' : val, 
'valTimesTypeDoubled' : val*type*2, #(-2*1, -4*1, 2*2, 4*2)*2
'sumOfType1' : sum(if(type==1, val, 0)), #-2 + -4 = -6
'sumOfType2' : sum(if(type==2, val, 0)),  # 2 + 4  = 6
'differenceOfSums' : sum(if(type==1, val, 0)) - sum(if(type==2, val, 0))
groupby(key) 
}
''',
[{'key': 1, 
  'sum': 4,  
  'val': [-2, -4, 2, 4],
  'sumOfType1': -6,
  'sumOfType2': 6,
  'differenceOfSums': -12.0,
  'valTimesTypeDoubled': [-4.0, -8.0, 8.0, 16.0]},
 {'key': 10,
  'sum': 40,
  'val': [-20, -40, 20, 40],
  'sumOfType1': -60,
  'sumOfType2': 60,
  'differenceOfSums': -120.0,
  'valTimesTypeDoubled': [-40.0, -80.0, 80.0, 160.0]}
],  
model = groupbymodel
)

t('''{key, type, val groupby(key)}''',
model = [{'key': 1, 'type': [1, 1, 2, 2], 'val': [-2, -4, 2, 4]},
 {'key': 10, 'type': [1, 1, 2, 2], 'val': [-20, -40, 20, 40]}]
)

#no group by

groupbymodel2 = [{ 'id': 1, 'type': [1, 1, 2, 2], 'val': [2, 4]},
 {'id' : 10, 'type': [1, 1, 2, 2], 'val': [20, 40]},
 {'id': 2, 'val' : 1},
 # XXX functions and operator that expect numbers explode
 {'id': 3, 'val' : None} 
 ]

t('''
{
id, val, type,
'sumOfVal' : sum(val),#-2 + -4 = -6
'valTimesTypeDoubled' : if(val, val, 'null'), #(-2*1, -4*1, 2*2, 4*2)*2
'valIfType': if(type==2, val, 3),
'sumOfValIfType' : sum(if(type==2, val, 3)),  # 2 + 4  = 6

#XXX differenceOfSums raises
#self.addFunc('sub', lambda a, b: float(a)-float(b), NumberType)
#TypeError: float() argument must be a string or a number
#'differenceOfSums' : sum(if(type==1, val, 0)) - sum(if(type==2, val, 0))
}
''',
[{'id': '1',
  'sumOfVal': 66,
  'sumOfValIfType': 78,
  'type': [1, 1, 2, 2],
  'val': [2, 4],
  'valIfType': [3, 2, 3, 4],
  'valTimesTypeDoubled': [2, 4, 2, 4]},
 {'id': '10',
  'sumOfVal': 66,
  'sumOfValIfType': 78,
  'type': [1, 1, 2, 2],
  'val': [20, 40],
  'valIfType': [3, 20, 3, 40],
  'valTimesTypeDoubled': [20, 40, 20, 40]},
 {'id': '1',
  'sumOfVal': 66,
  'sumOfValIfType': 78,
  'type': [1, 1, 2, 2],
  'val': [2, 4],
  'valIfType': [3, 2, 3, 4],
  'valTimesTypeDoubled': [2, 4, 2, 4]},
 {'id': '10',
  'sumOfVal': 66,
  'sumOfValIfType': 78,
  'type': [1, 1, 2, 2],
  'val': [20, 40],
  'valIfType': [3, 20, 3, 40],
  'valTimesTypeDoubled': [20, 40, 20, 40]}]
, model = groupbymodel2
)

t('''
[count(val), count(*), sum(val), avg(val)]
''', 
[[5, 4, 67, 13.4]],
model = groupbymodel2
)

t('''
[count(val), count(*), sum(val), avg(val)]
''', [[8, 8, 4, 0]],
model=groupbymodel)

t('''
[id, count(val), sum(val)]
''', 
[['_:j:t:object:1', 8, 4],
 ['_:j:t:object:2', 8, 4],
 ['_:j:t:object:3', 8, 4],
 ['_:j:t:object:4', 8, 4],
 ['_:j:t:object:5', 8, 4],
 ['_:j:t:object:6', 8, 4],
 ['_:j:t:object:7', 8, 4],
 ['_:j:t:object:8', 8, 4],
 ['_:j:t:object:1', 8, 4],
 ['_:j:t:object:2', 8, 4],
 ['_:j:t:object:3', 8, 4],
 ['_:j:t:object:4', 8, 4],
 ['_:j:t:object:5', 8, 4],
 ['_:j:t:object:6', 8, 4],
 ['_:j:t:object:7', 8, 4],
 ['_:j:t:object:8', 8, 4]],
model=groupbymodel)

t('''
[val, count(val), sum(val)]
''', 
[[-2, 8, 4],
 [-4, 8, 4],
 [2, 8, 4],
 [4, 8, 4],
 [-20, 8, 4],
 [-40, 8, 4],
 [20, 8, 4],
 [40, 8, 4],
 [-2, 8, 4],
 [-4, 8, 4],
 [2, 8, 4],
 [4, 8, 4],
 [-20, 8, 4],
 [-40, 8, 4],
 [20, 8, 4],
 [40, 8, 4]],
model=groupbymodel)

t.group = 'in'

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
 #, name='labeled but no where'
 )

t.group = 'nestedconstruct'

#XXX throws: vesper.query.QueryException: only equijoins currently supported
skip('''
    {
    *
     where (subjectof= ?tag and
          {
            ?tag in follow('commons', subsumedby)
           }
        )
    }
    ''',[])

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

t("""{
'blah' : [{* where (id=?tag)}] 
where (tags = ?tag)
}
""",
[{'blah': []}],
model=modelFromJson([{
  "tags" : []
}])
)

t.group = 'follow'

t("""
{ id, 'top' : follow(id, subsumedby, true, true)
}
""", [{'id': 'anotherparent', 'top': None},
 {'id': 'noparents', 'top': None},
 {'id': 'parentsonly', 'top': 'noparents'},
 {'id': 'hasgrandparents3', 'top': ['noparents', 'anotherparent']},
 {'id': 'hasgrandparents2', 'top': 'noparents'},
 {'id': 'hasgrandparents1', 'top': 'noparents'}], 
 model=modelFromJson([
  { "id" : "noparents", 'type' : 'tag'
  },
  { "id" : "anotherparent", 'type' : 'tag'
  },
  { "id" : "parentsonly",
    "subsumedby" : "noparents",
  },
  { "id" : "hasgrandparents1",
    "subsumedby" : "parentsonly",
  },  
  { "id" : "hasgrandparents2",
    "subsumedby" : ["parentsonly","noparents"]
  },
  { "id" : "hasgrandparents3",
    "subsumedby" : ["parentsonly","anotherparent"]
  }  
])
)

t('''
{ ?tag, id, label, 
 "othertags":?posts.tags,
 where ({?tag1 :tagid1 in follow(?tag1, subsumedby)} 
  and {?posts ?tag = tags and tags = ?tag1} 
  and ?tag not in (:tagid1) )}
''',
[{'id': 'toread', 'label': 'to read', 'othertags': ['commons', 'toread']}], 
bindvars = { 'tagid1' : 'commons'})

#find all the entries that implicitly or explicitly are tagged 'projects'
t('''
    {
    * 
     where (
          { id = ?tag and
            'projects' in follow(?tag, subsumedby)
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
            ?tag in follow('commons', subsumedby)
           }
        )
    }
    ''',
[{'content': 'some text about the commons',
  'subject': 'commons'},
 {'content': 'some more text about the commons', 'subject': 'commons'}]
)

#test label instead of id = ?tag, should have same result as previous query
t( '''
    {
    *
     where (subject= ?tag and
          { ?tag, 
            ?tag in follow('commons', subsumedby)
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
           id = ?tag and id in follow(?start, subsumedby)
           }
        )
    }
    ''',[])

#XXX creating bad AST
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
 {'id': 'post1', 'tags': ['commons', 'toread']},
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
'''
{
?parent
id, type,
'properties' : {
   ?parent
   * omit id, omit type
   #* exclude(id, type)   
   # where (id=?parent)
  }
}
'''

#better:
#need parser support for ?label.* at construct-level
'''
{
?parent
id, type,
'properties' : { 
   ?parent.* exclude(id, type)
  }
}
'''

'''
{

foo : [maybe foo]

maybe foo, #omits 'foo' if null
'foo' : maybe foo #'foo' : null

#omits 'foo' if value is null
isnull(prop()) and 'foo'

maybe 'foo' : bar or baz #omits 'foo' if value is null
val and 'foo' : 
}
'''

'''
{
include {*}
exclude(0,2) [when]
exclude(*) [when]
}
'''

#t.group = 'owner'
#idea: add a owner() function that return the owner of the current object (or None if not owned)
#then enable groupby(owner()) a very useful construct for embedded objects
#also allow owner(?label), enabling {?embedded ... where({?owner id = owner(?embedded) and ...}) }

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
    
#bug: when the label name is the same as the property name
#property references construct the label value (the id) instead of the property value
#e.g. this returns [{'tags': '2'}] instead of [{'tags': 'tag1'}]
skip('''
{ ?tags 
  tags
}
''', [{'tags': 'tag1'}])

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

t.group = 'onetomany'

#XXX throws jql.QueryException: reference to unknown label(s): tag
#'tags' : ?tag should be treated like 'tags' : {?tag}  
skip('''
{ 
    'tags' : ?tag 
    where (tags = ?tag)
}
'''
)

#XXX error: inner construct not joining on outer
#we need to make isConstant check in evalSelect exclude references to outer joins
#in construct props
skip('''
{
?tag, label,
'attributes' : { 'itemid' : ?tag.id }
where (label = 'tag 1')
}
''')

#XXX fails: File "/_dev/rx4rdf/vesper/src/vesper/query/rewrite.py", line 446, in addAlias
#   assert join is pred.parent.parent.parent, pred.parent.parent.parent
# pred.parent.parent.parent is none
skip('''
{
?tag, label,
'attributes' : { ?inner 'itemid' : ?tag where (?inner = ?tag)}
where (label = 'tag 1')
}
''')

#fails, erroneously returns: 
#{'attributes': [{'itemid': None},
#                 {'itemid': None},
#                 {'itemid': None},
#                 {'itemid': None}],
#  'label': 'tag 1'}]
skip('''
{
?tag, label,
'attributes' : { 'itemid' : ?tag }
where (label = 'tag 1')
}
''')

#XXX fails
#toplevel join is missing
#vesper.query._query.QueryException: construct: could not find subject label "tag" in (ColInfo('subject', <type 'unicode'>),
#ColInfo('predicate', <type 'unicode'>), ColInfo('object', <type 'unicode'>), ColInfo('objecttype', <type 'unicode'>),
#ColInfo('context', <type 'object'>), ColInfo('listpos', <type 'unicode'>))
skip('''
{
?tag, label,
'attributes' : { 'itemlabel' : ?tag.label }
where (label = 'tag 1')
}
''')

t('''
    { ?tag, id,
    'shared' : ?posts.tags
     where({ ?posts ?tag = tags} and ?tag = 'tag1')
    }
''',
[{'id': 'tag1', 'shared': 'tag1'}]
)

t.model = modelFromJson([
        { "label": "tag 1", 'id': 'tag1'},
        { "label": "tag 2", 'id': 'tag2'},
        { "parent":"1", "child":"3", 'id': '_:1'},
        { "id" : "2", "type" : "post", "tags" : "tag1"},
        { "id" : "2", "type" : "post", "tags" : "tag2"},
        { "id" : "3", "type" : "post"}
    ])

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (tags = ?tag and type='post')
}''',
[{'id': '2', 'tags': [['tag1'], ['tag2']]}])


t('''
{   ?post,  
    id : [id where (id=?tag)]
    where (tags = ?tag and type='post')
}''',
[{'2': [['tag1'], ['tag2']]}]
)

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (type='post' and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None}, {'id': '2', 'tags': [['tag1'], ['tag2']]}]
)

t('''
{   ?post, id, 
    'tags' : {* where (id=?tag)}
    where (tags = ?tag and type='post')
}''',
[{'id': '2',
  'tags': [{'id': 'tag1', 'label': 'tag 1'},
           {'id': 'tag2', 'label': 'tag 2'}]}]
)

t('''
{   ?post, id, 
    'tags' : {* where (id=?tag and label="tag 1")}
    where (tags = ?tag and type='post')
}''',
[{'id': '2',
  'tags': {'id': 'tag1', 'label': 'tag 1'}
  }
])

#same as previous query but using a join expression instead of 
#a nested construct. The join is treated as a label.
t('''
{   ?post, id, 
    'tags' : { id=?tag and label="tag 1"}
    where (tags = ?tag and type='post')
}''',
[{'id': '2', 'tags': 'tag1'}])

#XXX results should be the same as query label 
#but can't find label 'tag', it doesn't look set on the join
skip('''
{   ?post, id, 
    'tags' : ?tag
    where (tags = ?tag and type='post'
       and { id=?tag and label="tag 1"}
    )
}''',
[{'id': '2', 'tags': 'tag1'}])

t.group= 'semijoins' #(and antijoins)
'''
#XXX throws only equijoins supported
{
* 
where (tags in { id = 'tag1' })
}
'''

skip('''{
* 
where (tags not in { id = 'tag1' })
}
''')

skip('''{
* 
where (tags in { label = 'tag 1' })
}
''')

skip('''{
* 
where (tags not in { label = 'tag 1' })
}
''')

#XXX this is like a label evaluated as a boolean
#which is true if it exists
#so this should return an emty results
#since there are no objects where {a=b and b=2}
skip('''{* where(
  {a=1 and b=2}
)}''')

#{ { } } -- what does that mean? treat the same as { } or flag as error?
skip('''{* where(
  {{a=1 and b=2}}
)}''')


#XXX test labels in property expressions
#?foo 
#?foo or ?bar
#not ?foo
#test equivalent inline joins
t.group = 'ref'

from vesper.data.store.basic import MemStore
from vesper.data.base import Statement, OBJECT_TYPE_LITERAL, OBJECT_TYPE_RESOURCE

t.model = MemStore([
    Statement("subject", 'prop' , 'value', OBJECT_TYPE_LITERAL,''),
    Statement("subject", 'prop' , 'value', 'en-US',''),
    Statement("subject", 'prop' , 'value', 'tag:mydatatype.com:mydatatype',''),
    Statement("subject", 'prop' , 'value', OBJECT_TYPE_RESOURCE,''),    
])

#XXX handle refs
t("""
{ *
where (prop = 'value')
}
""")

t.model=modelFromJson([{'id': '1', 'foo' : "1"}, {'id': '2', 'foo' : 1},
                                                 { 'bar' : ["a", "b"]}])

#XXX handle types
#XXX plus how to query for non-json types and lang tags?
t("""
{ id, foo where (foo = 1)}
""")

t("""
{ id, foo where (foo = '1')}
""")

#this is correct but it'd be nice if there was some way to have the results 
#filter the values in bar's list
t("""
{ bar where (bar = 'a')}
""",
[{'bar': ['a', 'b']}])


#XXX raises error: "tags" projection not found
'''{ ?tag
 * 
 where (?tag = ?item.tags and
   { id = ?item and type = "post"} 
 )
}'''

#XXX id = ?item raise exception, removing it fix things
'''{ ?tag
 * 
 where ( 
   { id = ?item and type = "post" and ?tag = tags }
 )
}'''

#XXX raises construct: could not find subject label "@1"
'''{ ?tag  
     "attributes" : { id where (id=?tag) }, 
     "state" : "closed", 
     "data" : label  
     where (type = "tag")  
   }''' 

#XXX the id = ?post raises 
#File "/_dev/rx4rdf/vesper/src/vesper/query/rewrite.py", line 434, in analyzeJoinPreds
#   assert isinstance(filter, Filter), filter
#filter must be None -- has the parent already been removed?
#(without id = ?post, query is fine)
#{ ?tag, id  where (
#{id=?tag1 and @tagid1 in follow(?tag1, subsumedby)} 
# and {id = ?post and ?tag = tags and tags = ?tag1})}

#XXX turn these into tests:
'''
    this:
    {
    id : ?owner,
    'mypets' : {
          'dogs' : { * where(owner=?owner and type='dog') },
          'cats' : { * where(owner=?owner and type='cat') }
        }
    }

    is equivalent to:
    {
    id : ?owner,

    'mypets' : {
          'dogs' : { * where(id = ?pet and type='dog') },
          'cats' : { * where(id = ?pet and type='cat') }
        }

    where ( {id = ?pet and owner=?owner} )
    }

     
    what about where ( { not id = ?foo or id = ?bar and id = ?baz } )

    also, this:
    {
    'foo' : ?baz.foo
    'bar' : ?baz.bar
    }
    results in joining ?baz together

    here we do not join but use the label to select a value
    { 
      'guardian' : ?guardian,
      'pets' : { * where(owner=?guardian) },
    }

this is similar but does trigger a join on an unlabeled object:
    {
      'guardian' : ?guardian,
      'dogs' : { * where(owner=?guardian and type='dog') },
      'cats' : { * where(owner=?guardian and type='cat') }
    }

join( filter(eq(project('type'), 'dog')),
     filter(eq(project('owner'),objectlabel='guardian')
  jc(
     join( filter(eq(project('type'), 'cat')),
          filter(eq(project('owner'),objectlabel='guardian')
     ),
     Eq(Project('guardian'), Project('guardian'))
)

XXX test multiple labels in one filter, e.g.: { a : ?foo, b : ?bar where (?foo = ?bar) }
XXX test self-joins e.g. this nonsensical example: { * where(?foo = 'a' and ?foo = 'b') }
    '''

'''
XXX join on non-primary key:
{ 
where (date = ?foo.date and {id=?foo and type= 'events'})
}

XXX join on two non-primary keys:
{ 
where (date = ?foo.date and ownerid = ?event.ownerid and {id=?foo and type= 'events'})
}
'''

#XXX test:
# { * where (1=2) }
# { * where (1=1) }
#test that constant evals first and just once and doesn't join on the id
# { * where (foo = 1 and 1=2) } 
# { * where (foo = 1 and 1=1) }
# { * where (foo = 1 and {1=1}) } 
# { * where (foo = 1 and {bar=1 and 1=1}) } 
# { * where (foo = 1 and {bar=1 and {1=1} } )} 
# { * where (foo = 1 or 1=1) }  
#test that ?bar = 1 doesn't join with foo = 1
# {* where (?bar = 1 and foo = 1)}


import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

    def testSerializationClassOveride(self):
        '''
        test that query results always use the user specified list and dict classes
        '''
        #XXX user-defined list used for nested lists
        
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
            save =jql.QueryContext.defaultShapes
            jql.QueryContext.defaultShapes = { dict:hashabledict, list:hashablelist}
            #will raise TypeError: unhashable if a list or dict is in the results:
            set(jql.getResults("{*}", t.model).results)
        except:
            self.fail()
        finally:
            jql.QueryContext.defaultShapes = save
                    
if __name__ == "__main__":
    import sys
    try:
        sys.argv.remove("--unittest")        
    except:
        main(t)
    else:
        unittest.main()