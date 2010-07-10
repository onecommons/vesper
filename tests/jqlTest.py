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
t("{id}", [ {'id': '3'}, {'id': '2'}, {'id': '_:2'}, {'id': '_:1'},]
, useSerializer=True)

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

t("(id)",['@3', '@2', '@_:2', '@_:1'], useSerializer=True)

t("('@constant')", ['@constant'])

t("('@constant')",
[{'datatype': 'json', 'value': '@constant'}], useSerializer=True)

t('''
{ "staticprop" : ["foo"] }
''',[{ "staticprop" : ["foo"] }], useSerializer=True)

t('''
{ "staticprop" : "foo" }
''', 
[{'staticprop': 'foo'}], useSerializer=True)

t('''{ * where foo > @bar }''', [])

t('''{ * where ( id = :id) }''', 
[{'foo':'@bar', 'id':'2'}], 
        bindvars={'id':'@2'}, useSerializer=True)

#XXX bindvar needs to support ref and datatype patterns
t('''{ * where  child = :child }''', 
[{'child': '@2', 'id': '_:2', 'parent': '@1'}], 
        bindvars={'child':'@2'}, useSerializer=True)

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
      'derivedprop' : string(id) * 2,
      'children' : { ?childid,
                   *
                   where( {child = ?childid and
                        parent = ?parentid
                       })
                 }
    }
''',
results = [{'children': [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}],
  'derivedprop': 2.0,
  }]
)

t(
''' { ?parentid,
      id,
      'children' : { ?childid,
                   id, foo,
                   where( foo = @bar and 
                         {child = ?childid and
                        parent = ?parentid                    
                       })
                 }
    }
''',
[{'children': [{'foo': 'bar', 'id': '3'}, {'foo': 'bar', 'id': '2'}],
  'id': '1'}]
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

t('''{ * order by child }''',
[{'foo': 'bar', 'id': '3'}, #note: nulls go first
 {'foo': 'bar', 'id': '2'},
 {'child': '2', 'id': '_:2', 'parent': '1'},
 {'child': '3', 'id': '_:1', 'parent': '1'}]
)

t('''{ * order by id desc }''',
[{'child': '2', 'id': '_:2', 'parent': '1'},
 {'child': '3', 'id': '_:1', 'parent': '1'},
 {'foo': 'bar', 'id': '3'},
 {'foo': 'bar', 'id': '2'}]
)

res = [{'child': '3', 'id': '_:1', 'parent': '1'},
 {'child': '2', 'id': '_:2', 'parent': '1'},
 {'foo': 'bar', 'id': '2'},
 {'foo': 'bar', 'id': '3'}]
 
t('''{ * order by child desc, id }''', res)
t('''{ * order by child desc, id asc }''', res)

t.group = 'parse'

#XXX add real tests for this
t('''{ 'foo' : ?bar.baz.id }''')

t('''
{ 'id' : ID, 'blah' : foo }
''',
[{'blah': '@bar', 'id': '3'}, {'blah': '@bar', 'id': '2'}]
, useSerializer=True)

t('''
{ id, 'blah' : foo }
''',
[{'blah': '@bar', 'id': '3'}, {'blah': '@bar', 'id': '2'}]
, useSerializer=True)

t(u'("\u2019\\x0a")', [u'\u2019\n']) #\u2019 is RIGHT SINGLE QUOTATION MARK

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

'''{ * where foo = <{what a prop}>}''',
#force list
'''
{ 'blah' : [foo] }
''',
'''{* where (foo = { id = 'd' }) }''',

'''{ * where foo = @<{what a ref}>}''',
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
GROUP BY foo
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

#error: need space after ':' or else looks like a bindvar
t('''{ foo:bar }''', ast='error')

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

t('''{
* 
where (<rdfs:range> = @Tag)
order by <rdfs:range>
namemap = {
 "sharedpatterns" : { 
      'rdf:' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      'rdfs:': 'http://www.w3.org/2000/01/rdf-schema#'
    }
}
}''',
[{'id': 'subsumedby',
  'rdfs:domain': '@Tag',
  'rdfs:range': '@Tag',
  'rdfs:subPropertyOf': '@rdfs:subClassOf'}],
 useSerializer=True
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
group by subject
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
group by subject
}
''', [{'content': ['some text about the commons',
              'some more text about the commons',
              ]},
 {'content': 'some text about rhizome'}]
 )

t('''{
 subject, 
 'count' : count(content)
 group by subject
 }
 ''', 
 [{'count': 2, 'subject': 'commons'}, {'count': 1, 'subject': 'rhizome'}])

t('''{
  subject, 
  'count' : count(*), 
  'count2': count(subject)
  group by subject
  }
  ''',
[{'count': 2, 'count2': 2, 'subject': 'commons'}, {'count': 1, 'count2': 1, 'subject': 'rhizome'}])

#expression needs to be evaluated on each item

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
group by key 
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

t('''{key, type, val group by key}''',
model = [{'key': 1, 'type': [1, 1, 2, 2], 'val': [-2, -4, 2, 4]},
 {'key': 10, 'type': [1, 1, 2, 2], 'val': [-20, -40, 20, 40]}]
)

#no group by

groupbymodel2 = [{ 'id': 1, 'type': [1, 1, 2, 2], 'val': [2, 4]},
 {'id' : 10, 'type': [1, 1, 2, 2], 'val': [20, 40]},
 {'id': 2, 'val' : 1},
 {'id': 3, 'val' : None} 
 ]

t('''
{
id, val, type,
'sumOfVal' : sum(val),#-2 + -4 = -6
'valTimesTypeDoubled' : if(val, val, 'null'), #(-2*1, -4*1, 2*2, 4*2)*2
'valIfType': if(type==2, val, 3),
'sumOfValIfType' : sum(if(type==2, val, 3)),  # 2 + 4  = 6

#XXX differenceOfSums evaluates to null for some reason
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

#note: with { 'parents' : id group by children } the id will refer to children object id that's why we need the label reference
t('''
{
?parent
'parents' : ?parent.id, 
'child' : children

group by children 
}
''',
[{'child': 'a', 'parents': ['1', '2']},
 {'child': 'c', 'parents': ['1', '2']},
 {'child': 'b', 'parents': ['1', '2']}]
 , model = modelFromJson([
    { 'id' : '1',
       'name' : '1',
      'children' : ['a', 'b', 'c']
    },
    { 'id' : '2',
      'name' : '2',
      'children' : ['a', 'b', 'c']
    },
    {
    'id':'a',
    'label':'a'
    },
    {
    'id':'b',
    'label':'b'
    },
    {
    'id':'c',
    'label':'c'
    }
  ])
)


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
where ?tag in (@foo, @commons)
}
''',
[{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}])

t('''
{ *
where (id = ?tag and ?tag in (@foo, @commons))
}
''',
[{'id': 'commons',  'label': 'commons', 'subsumedby': 'projects', 'type': 'Tag'}])

t('''
{ id
where (id not in (@foo, @commons) and subsumedby)
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

t('''
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

#XXX throws AssertionError File "/_dev/rx4rdf/vesper/src/vesper/query/engine.py", line 54, in getColumns
#  assert outerjoin or keycell
skip('''{
?parent 
 id : [{* where(subsumedby = ?parent)}]
}''', model =  {
   "subsumedby": None, 
   "id": "1"
 }
)

##XXX returns [{2: [{'id': '1', 'subsumedby': 2}]}] -- 2 isn't a valid id
#but it shouldn't match anything (empty result)
#XXX add support for maybe nestedconstruct
skip('''{
?parent 
 id : [{* where(subsumedby = ?parent)}]
}''', model =  {
   "subsumedby": 2, 
   "id": "1"
 }
)

##XXX returns parent [[{'id': '1', 'subsumedby': []}]]
#treating the empty list as the parent resource
#but it shouldn't match anything (empty result)
skip('''{
?parent 
 "parent" : {* where(subsumedby = ?parent)}
}''', model =  {
   "subsumedby": [], 
   "id": "1"
 }
)

#returns [[{'id': '1', 'subsumedby': []}]]
#XXX should error or at least warn if construct has name expression and result object is a list
skip('''{
?parent 
 id : {* where(subsumedby = ?parent)}
}''', model =  {
   "subsumedby": [], 
   "id": "1"
 }
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
[{'id': 'toread', 'label': 'to read', 'othertags': ['@commons', '@toread']}], 
bindvars = { 'tagid1' : '@commons'}, useSerializer=True) 

#find all the entries that implicitly or explicitly are tagged 'projects'
t('''
    {
    * 
     where (
          { id = ?tag and
            @projects in follow(?tag, subsumedby)
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
            ?tag in follow(@commons, subsumedby)
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
            ?tag in follow(@commons, subsumedby)
           }
        )
    }
    ''',
[{'content': 'some text about the commons',
  'subject': 'commons'},
 {'content': 'some more text about the commons', 'subject': 'commons'}]
)

#XXX results shouldn't be empty, should be same as above
t( '''
    { *
     where (subjectof= ?tag and
          { id = ?start and id = @commons }
          and
          {
           id = ?tag and id in follow(?start, subsumedby)
           }
        )
    }
    ''',[])

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
where (type = @Tag and not subsumedby)
}
''',
[{'id': 'actions', 'type': 'Tag'}, {'id': 'projects', 'type': 'Tag'}])

t('''
{ *
where (not subsumedby and type = @Tag)
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
where (type = @Tag and not not subsumedby)
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

#compare results with and without forUpdate
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

t.group = 'types'


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

t.group = 'filter'
filterModel = modelFromJson([
      { 'id' : '1',
        'values' :  None
      },
      { 'id' : '2',
        'values' : 0
      },
     { 'id' : '3',
        'values' : ['', 0, None, False]
     },
     { 'id' : '4',
        'values' : [1,'1',1.1]
     },
     { 'id' : '5',
       'values' : ''
     },
     { 'id' : '6',
       'values' : 1
     },
     { 'id' : '7',
       'values' : '1'
     },
     { 'id' : '8',
       'values' : { 'datatype' : 'json', 'value' : '1' }
     },
     ]
 )

t.model = filterModel

t('''{ id, values where values = @1 and values = 1 }''',
[{'id': '4', 'values': [1, '1']}
])

#adding "and values" matches all values so all values ends up in the result
t('''{ id, values where values = @1 and values = 1 and values}''',
[{'id': '4', 'values': [1, '1', 1.1]}]
)

t('''{ id, values where values == @1 }''',
[{'id': '4', 'values': ['@1']},
 {'id': '7', 'values': '@1'}], useSerializer=True
)

t('''{ id, values where values != @1 }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2', 'values': 0},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': [1,1.1]},
 {'id': '6', 'values': 1},
 {'id': '8', 'values': '1'}]
)

t('''{ id, values where values == '1' }''',
[{'id': '8', 'values': '1'}], 
useSerializer=True)

t('''{ id, values where values != '1' }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2', 'values': 0},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': [1, '@1', 1.1]},
 {'id': '7', 'values': '@1'},
 {'id': '6', 'values': 1},
], useSerializer=True
)

t('''{ id, values where values == 1 }''',
[{'id': '4', 'values': [1, ]}, {'id': '6', 'values': 1}]
) 

t('''{ id, values where values != 1 }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2', 'values': 0},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': ['1', 1.1]},
 {'id': '7', 'values': '1'},
 {'id': '8', 'values': '1'}
 ]
) 

t('''{ id, values where values = null }''',
[{'id': '1', 'values': None}, {'id': '3', 'values': [None]}]) 

t('''{ id, values where values != null }''',
[{'id': '3', 'values': ['', 0, False]},
 {'id': '2', 'values': 0},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': [1, '1', 1.1]},
 {'id': '7', 'values': '1'},
 {'id': '6', 'values': 1},
 {'id': '8', 'values': '1'}]
) 

t('''{ id, values where values = '' }''',
[{'id': '3', 'values': ['']}, 
{'id': '5', 'values': ''}]
)

t('''{ id, values where values != '' }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': [0, None, False]},
 {'id': '2', 'values': 0},
 {'id': '4', 'values': [1, '1', 1.1]},
 {'id': '7', 'values': '1'},
 {'id': '6', 'values': 1},
 {'id': '8', 'values': '1'}]
) 

t('''{ id, values where values = 0 }''',
[{'id': '3', 'values': [0, False]}, 
 {'id': '2', 'values': 0}]
) 

t('''{ id, values where values != 0 }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': ['', None]},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': [1, '1', 1.1]},
 {'id': '7', 'values': '1'},
 {'id': '6', 'values': 1},
 {'id': '8', 'values': '1'}]
) 

t('''{ id, values where values = '0' }''', []) 

t('''{ id, values where values != '0' }''',
[{'id': '1', 'values': None},
 {'id': '3', 'values': ['', 0, None, False]},
 {'id': '2', 'values': 0},
 {'id': '5', 'values': ''},
 {'id': '4', 'values': [1, '1', 1.1]},
 {'id': '7', 'values': '1'},
 {'id': '6', 'values': 1},
 {'id': '8', 'values': '1'}]
) 

from vesper.data.store.basic import MemStore
from vesper.data.base import Statement, OBJECT_TYPE_LITERAL, OBJECT_TYPE_RESOURCE

t.model = MemStore([
    Statement("subject1", 'prop' , 'value', OBJECT_TYPE_LITERAL,''),
    Statement("subject2", 'prop' , 'value', 'en-US',''),
    Statement("subject3", 'prop' , 'value', 'tag:mydatatype.com:mydatatype',''),
    Statement("subject4", 'prop' , 'value', OBJECT_TYPE_RESOURCE,''),    
])

#XXX handle language tags -- plain strings should match lang tagged strings
#XXX handle datatypes -- how to query for non-json types
t("""
{ *
where (prop = 'value')
}
""", 
[{'id': 'subject1', 'prop': 'value'}]
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

#XXX list order is not preserved
#XXX evaluating on list product is not intuitive here
#XXX shouldn't nested lists be evaluated too? -- currently treated as a bad value
#XXX bad values are collapsing to 0, shouldn't list size be preserved? (e.g. [0,0,0] instead of 0)
t('''{ 
"listpropX2" : listprop * 2,
"listprop2X2" : [listprop2 * 2],
#"listproplistprop2" : listprop + listprop2,
}''',
[{'listprop2X2': [-2.0,
                  0.0,
                  2.0,
                  20.0,
                  22.0,
                  4.0,
                  6.0,
                  8.0,
                  10.0,
                  12.0,
                  14.0,
                  18.0],
  'listpropX2': [0.0, 0.0, 0.0, 0.0]             
 },
 {'listprop2X2': [0.0], 'listpropX2': 0.0
  }])

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
    where (type=@post and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': {'id': 'tag1'}, 'type': 'post'}]
)

t('''
{   *, 
    'tags' : [id where (id=?tag)]
    where (maybe tags = ?tag and type=@post)
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': ['tag1'], 'type': 'post'}]
, unordered=True)

t('''
{   *,
    'tags' : {* where (id=?tag)}   
    where (type=@post and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None, 'type': 'post'},
 {'id': '2', 'tags': {'id': 'tag1', 'label': 'tag 1'}, 'type': 'post'}]
)

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where ((maybe tags = ?tag) and type=@post)
}
''',
[{'id': '3', 'tags': None}, {'id': '2', 'tags': ['tag1']}]
, unordered=True)

#nested constructs that don't have their own join, just reference outer join
t.group = 'dependentconstructs'

t('''
{ 
    'tags' : ?tag 
    where (tags = ?tag)
}
''',
[{'tags': 'tag1'}])

t('''
{
?tag, label,
'attributes' : { 'itemid' : ?tag.id }
where (label = 'tag 1')
}
''',
[{'attributes': {'itemid': 'tag1'}, 'label': 'tag 1'}])

t('''
{
?tag, label,
'attributes' : { ?inner 'itemid' : ?tag where (?inner = ?tag)}
where (label = 'tag 1')
}
''',
[{'attributes': {'itemid': 'tag1'}, 'label': 'tag 1'}]
)

t('''
{
?tag, label,
'attributes' : { 'itemid' : ?tag }
where label = 'tag 1'
}
''',
[{'attributes': {'itemid': 'tag1'}, 'label': 'tag 1'}])

t('''
{
?tag, label,
'attributes' : { 'itemlabel' : ?tag.label }
where label = 'tag 1'
}
''',
[{'attributes': {'itemlabel': 'tag 1'}, 'label': 'tag 1'}])

t.group = 'onetomany'

t(query='''
    { ?tag, id, 
    'shared' : ?posts.tags #other tags that posts with this tag have
     where({ ?posts ?tag = tags} and ?tag = 'tag1')
    }
''',
results=[{'id': 'tag1', 'shared': 'tag1'}]
)

t.model = modelFromJson([
        { "label": "tag 1", 'id': 'tag1'},
        { "label": "tag 2", 'id': 'tag2'},
        { "parent":"1", "child":"3", 'id': '_:1'},
        { "id" : "2", "type" : "post", "tags" : "tag1"},
        { "id" : "2", "type" : "post", "tags" : "tag2"},
        { "id" : "3", "type" : "post"}
    ])

t(query='''
        { ?tag, id, 
        'shared' : ?posts.tags #other tags that posts with this tag have
         where {?posts 
                ?tag = tags} 
            and ?tag = 'tag1'
        }
    ''',
 results=[{'id': 'tag1', 'shared': ['tag1', 'tag2']}]
)

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (tags = ?tag and type=@post)
}''',
[{'id': '2', 'tags': [['tag1'], ['tag2']]}])


t('''
{   ?post,  
    id : [id where (id=?tag)]
    where (tags = ?tag and type=@post)
}''',
[{'2': [['tag1'], ['tag2']]}]
)

t('''
{   ?post, id, 
    'tags' : [id where (id=?tag)]
    where (type=@post and maybe tags = ?tag)
}
''',
[{'id': '3', 'tags': None}, {'id': '2', 'tags': [['tag1'], ['tag2']]}]
)

t('''
{   ?post, id, 
    'tags' : {* where (id=?tag)}
    where (tags = ?tag and type=@post)
}''',
[{'id': '2',
  'tags': [{'id': 'tag1', 'label': 'tag 1'},
           {'id': 'tag2', 'label': 'tag 2'}]}]
)

t('''
{   ?post, id, 
    'tags' : {* where (id=?tag and label="tag 1")}
    where (tags = ?tag and type=@post)
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
    where (tags = ?tag and type=@post)
}''',
[{'id': '2', 'tags': 'tag1'}])

t('''
{   ?post, id, 
    'tags' : ?tag
    where (tags = ?tag and type=@post
       and { id=?tag and label="tag 1"}
    )
}''',
[{'id': '2', 'tags': 'tag1'}])

t.group = 'nojoin'

t('''
{ ?a
    where (
        {id=?a and ?a = 1} and {id=?b and ?b = 2}
        and ?b = ?a
    )
}
''', [])

t('''
{
?posts
'tags' : ?tag.id
where ?posts.tags = ?tag
}
''',
[{'tags': ['tag1', 'tag2']}])

t('''
{
?posts
'tags' : ?tag.id
where tags = ?tag
}
''',
[{'tags': ['tag1', 'tag2']}])

t('''
{
?posts
'tags' : ?tag.id
where {?tag ?posts.tags = ?tag} and type = @post
}
''',
[{'tags': ['tag1', 'tag2']}])

t('''
{
?posts
'tags' : {?tag id where ?posts.tags = id}
where type = @post
}
''',
[{'tags': [{'id': 'tag1'}, {'id': 'tag2'}]}])

t.group = 'crossjoin'

crossjoinResults = [{'alltags': ['tag 1', 'tag 2'], 'id': '3', 'tags': None},
 {'alltags': ['tag 1', 'tag 2'], 'id': '2', 'tags': ['tag1', 'tag2']}]
 
t('''
{
id,
maybe tags,
'alltags' : ?alltags.label
where type = @post 
}
''',crossjoinResults)

#equivlent to previous query
t('''
{
id,
maybe tags,
'alltags' : ?alltags.label
where type = @post and {?alltags label}
}
''', crossjoinResults)

t('''
{
?alltags

id,
'posts' : ?post.id,
label
where {?post type = @post}
}
''',
[{'id': 'tag1', 'label': 'tag 1', 'posts': ['3', '2']},
 {'id': 'tag2', 'label': 'tag 2', 'posts': ['3', '2']}]
)

t.group= 'semijoins' #(and antijoins)

#XXX throws AssertionError: missing label: @1
skip('''
{
* 
where (tags in { id = 'tag1' })
}
''')

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
#test equivalent inline joins'

t('''{ ?tag
 * 
 where (?tag = ?item.tags and
   { id = ?item and type = @post} 
 )
}''',
[{'id': 'tag1', 'label': 'tag 1'}, {'id': 'tag2', 'label': 'tag 2'}]
)

t('''{ ?tag
 * 
 where { id = ?item and type = @post and ?tag = tags }
}''',
[{'id': 'tag1', 'label': 'tag 1'}, {'id': 'tag2', 'label': 'tag 2'}]
)

t('''{ ?tag  
     "attributes" : { id where (id=?tag) }, 
     "state" : "closed", 
     "data" : label  
     where (label = "tag 1")  
   }''',
 [{'attributes': {'id': 'tag1'}, 'data': 'tag 1', 'state': 'closed'}]
)

t('''
{ ?tag, id  where (
{id=?tag1 and @tagid1 in follow(?tag1, subsumedby)} 
 and {id = ?post and ?tag = tags and tags = ?tag1})
}
''',[])

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

t.group = 'temp'

# { 'tags' : ?inner.label where ?outer.tags = ?inner }
t(ast=Select(
    Construct([
            ConstructProp('post-id', Project(SUBJECT)),
            ConstructProp('tag-labels', Project('label')), 
              ConstructProp('tag-ids', Label('inner'))]),
where=Join(
   Filter(Eq(Project(PROPERTY),'tags'), objectlabel='tags'),
   JoinConditionOp(
    Join(
      Filter(Eq(Project(PROPERTY),'label'), objectlabel='label')
    , name='inner'), 
   'inner', 'i', 'tags')
  )
),
results=[{'post-id': '2',
  'tag-ids': ['tag1', 'tag2'],
  'tag-labels': ['tag 1', 'tag 2']}]
)

t('''
{
?a 
id,
foo,
'blah' : {?b bar}
where ?a = ?b and ?a = 1 and {?b type=@post}
}
''')

#txn tests
#XXX ?firsttxn = string(1) needs to be moved to the ?firsttxn.createtime join  
skip('''
{
id where type = @post and ?firsttxn = string(1) #getfirsttxn(id) 

order by ?firsttxn.createdtime
}''')

t('''
{
?parent
'parents' : ?parent.id, 
'child' : children,
'children' : ?parent.children #show all children for the parent
group by children 
}
''')
   

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

    def testSerializationClassOveride(self):
        '''
        test that query results always use the user specified list and dict classes
        '''
        #XXX user-defined list not used for nested lists
        
        #some hacky classes so we can call set() on the query results
        class hashabledict(dict):
            def __hash__(self):
                #print self
                return hash(tuple(sorted(self.items())))

            def __repr__(self):
                return 'HashableDict('+super(hashabledict, self).__repr__()+')'
        
        class hashablelist(list):
            def __hash__(self):
                return hash(tuple(self))

            def __repr__(self):
                return 'HashableList('+super(hashablelist, self).__repr__()+')'

        try:
            save =jql.QueryContext.defaultShapes
            jql.QueryContext.defaultShapes = { dict:hashabledict, list:hashablelist}
            #will raise TypeError: unhashable if a list or dict is in the results:
            set(jql.getResults("{*}", t.model).results)
        except:
            self.fail()
        finally:
            jql.QueryContext.defaultShapes = save
    
    def testPygmentsLexer(self):
        try:
            import pygments
            from pygments.token import Token
        except ImportError:
            print 'skipping testPygmentsLexer, pygments not available'
            return
        from vesper.query.pygmentslexer import JsonqlLexer
        query = '''{
          foo, 
          *,
          "displayname" : displayname+1,
          <afasfd>
           where ?bar = :bindvar and func(@<{what a ref}>, @foo:baz)
           order BY <a prop> ASC
        }'''
        tokens = [(Token.Punctuation, u'{'), (Token.Text, u'\n          '), (Token.Name.Variable, u'foo'), (Token.Punctuation, u','), (Token.Text, u' \n          '), (Token.Name.Variable, u'*'), (Token.Punctuation, u','), (Token.Text, u'\n          '), (Token.Literal.String.Double, u'"displayname"'), (Token.Text, u' '), (Token.Punctuation, u':'), (Token.Text, u' '), (Token.Name.Variable, u'displayname'), (Token.Operator, u'+'), (Token.Literal.Number.Integer, u'1'), (Token.Punctuation, u','), (Token.Text, u'\n          '), (Token.Name.Variable, u'<afasfd>'), (Token.Text, u'\n           '), (Token.Keyword.Reserved, u'where'), (Token.Text, u' '), (Token.Name.Label, u'?bar'), (Token.Text, u' '), (Token.Operator, u'='), (Token.Text, u' '), (Token.Name.Entity, u':bindvar'), (Token.Text, u' '), (Token.Operator.Word, u'and'), (Token.Text, u' '), (Token.Name.Function, u'func'), (Token.Punctuation, u'('), (Token.Literal, u'@<{what a ref}>'), (Token.Punctuation, u','), (Token.Text, u' '), (Token.Literal, u'@foo:baz'), (Token.Punctuation, u')'), (Token.Text, u'\n           '), (Token.Keyword.Reserved, u'order'), (Token.Text, u' '), (Token.Keyword.Reserved, u'BY'), (Token.Text, u' '), (Token.Name.Variable, u'<a prop>'), (Token.Text, u' '), (Token.Keyword.Reserved, u'ASC'), (Token.Text, u'\n        '), (Token.Punctuation, u'}'), (Token.Text, u'\n')]
        self.assertEquals(tokens, list(pygments.lex(query, JsonqlLexer()) ) )
    
if __name__ == "__main__":
    import sys
    try:
        sys.argv.remove("--unittest")        
    except:
        main(t)
    else:
        unittest.main()