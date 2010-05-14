#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import sys
sys.path.append('.')
from jqltester import *

t = Suite()
skip = Suite()

###################################
########### basic tests ###########
###################################

t % '''
jsonQL Reference 
~~~~~~~~~~~~~~~~

jsonQL is languages for querying data that can represented in JSON. A jsonQL implementation provides a mapping from objects in a backend datastore to a collection of JSON objects with properties (for example, each object might correspond to a row in table, with a property for each column). A jsonQL query operates on that mapping in a manner similar to a SQL query except that instead of returning rows it returns JSON data structures based on the pattern specified in the query.

The examples here are based on the following example. You can cut an paste or you can run the admin tool on the sample store. 
'''

t.model = mainmodel = modelFromJson([
{ "type": "post", 
'id' : "post1", 
'contentType' : 'text/plain',
'published' : '', 
'tags' : ['tag:foo'],
'author' : 'user:1',
'contents' : "hello world!"
},

{ "id" : "tag:foo", 
   'type' : 'tag',
   "label" : "foo",
  "subcategoryOf" : "tag:nonsense",
},

{
'id' : 'tag:nonsense',
'type' : 'tag',
'label' : 'Nonsense'
},

{ 'type' : 'user', 
'id' : 'user:1',
'displayname': 'abbey aardvaark', 
 'auth' : [ {
   'service' : 'facebook',
    'facebook_uid' : 394090223,
    'name' : 'abbey aardvaark'
   },   
   {
   'service' : 'google',
   'name' : 'abbey aardvaark',
   "email" : 'aaardvaark@gmail.com',
    "language" : 'en',
    "username" : 'aaardvaark'
   }
  ]
},
{ 'type' : 'user', 
'id' : 'user:2',
'displayname': 'billy billygoat',
},

])

t % printmodel(t.model)

t % '''
Basic Grammar
=============

Below is simplifed representation of the JQL grammar (the formal grammar can be found :doc:`here <grammar>`). We'll go through each element and provide sample queries illustrating each feature of the language. The queries and sample results are based on the sample json used by the [tutorial] (which, btw, might be a better place to start learning about JQL). 

.. productionlist::
 query  : `constructobject` 
        :| `constructarray` 
        :| `constructvalue`
 constructobject : "{" [`label`]
                 :    (`objectitem` | `propertypair` [","])+ 
                 :     [`query_criteria`] 
                 :  "}"
 constructarray  : "[" [`label`]
                 :  (`propertyvalue` [","])+ [`query_criteria`] 
                 : "]"
 constructvalue  : "(" 
                 :    `expression` [`query_criteria`] 
                 : ")"
 objectitem      : | "ID" | "*" | ["["] ["omitnull" | "maybe"] `propertyname` ["]"]
 propertypair    : ["omitnull"] `expression` ":" (`propertyvalue` | [`propertyvalue`])
 propertyvalue   : `expression` | "*" | `nestedconstruct`
 nestedconstruct : `constructarray` | `constructobject`
 propertyname    : NAME | "<" CHAR+ ">"
 query_criteria  : ["WHERE(" `expression` ")"]
                 : ["GROUPBY(" (`expression`[","])+ ")"]
                 : ["ORDERBY(" (`expression` ["ASC"|"DESC"][","])+ ")"]
                 : ["LIMIT" number]
                 : ["OFFSET" number]
                 : ["DEPTH" number]
                 : ["MERGEALL"]
                 : ["NAMEMAP" "=" `jsondict`]
 expression : `expression` "and" `expression`
            : | `expression` "or" `expression`
            : | "maybe" `expression`
            : | "not" `expression`
            : | `expression` `operator` `expression`
            : | `join`
            : | `atom`
            : | "(" `expression` ")"
 operator   : "+" | "-" | "*" | "/" | "%" | "=" | "=="
            : | "<" | "<=" | ">" | "=>" | ["not"] "in"  
 join       : "{" [`label`] `expression` "}"
 atom       : `label` | `bindvar` | `constant` 
            : | `functioncall` | `propertyreference`
 label      : "?"NAME
 bindvar    : ":"NAME
 propertyreference : [`label`"."]`propertyname`["."`propertyname`]+
 functioncall : NAME([`expression`[","]]+ [NAME"="`expression`[","]]+)
 constant : STRING | NUMBER | "true" | "false" | "null"
 comments : "#" CHAR* <end-of-line> 
          : | "//" CHAR* <end-of-line> 
          : | "/*" CHAR* "*/"

Construct Patterns
==================

There are three top level constructions depending on whether you want construct results as JSON objects (dictionaries), arrays (lists) or simple values (such as a string or number).

JQL query consists of a pattern describes a JSON object (dictionary), a list (array) or simple value -- executing query will construct a list of objects that match the pattern. This example returns a list of all the objects that have properties named "displayname" and "type":

'''

t('''{ 
    "displayname" : displayname,
    "type" : type
    }
''', [
        {
            "displayname": "abbey aardvaark",
            "type": "user"            
        }, 
        {
            "displayname": "billy billygoat",
            "type": "user"            
        }
    ]
)

t % '''
Both the property name and value are expressions. In this example, the property names is simply string constants while the property value are property references. In the next example, the property name is a property reference and property value is a
more complex expression. It uses the MERGEALL option to return a single dictionary of login services where the name of the service is the property and the value depends on the type of service. [#f1]_
'''

t("""{
  service : maybe facebook_uid or maybe email
  MERGEALL 
}""",
 [{'facebook': 394090223, 'google': 'aaardvaark@gmail.com'}]
)

t %'''
Abbreviated properties: :token:`objectitem`
-------------------------------------------
When a single property name appears instead of a name-value pair, it is 
treated as a name-value pair where the name is the name of the property and 
the value is a reference to the property. So the following example is 
equivalent to the first query: 
'''
t("{ displayname, type }", [
        {
            "displayname": "abbey aardvaark",
            "type": "user"            
        }, 
        {
            "displayname": "billy billygoat",
            "type": "user"            
        }
    ]
)

t%'''
:token:`constructarray`
-----------------------
You can also construct results as arrays (lists) instead of objects. This query selects the same objects but it formats each result as a list not an object.
'''

t("[displayname, type]", [    
    ['abbey aardvaark', "user"], ['billy billygoat', "user"]
    ]
)

t%'''
:token:`constructvalue`
-----------------------

You can select individual values (strings or numbers) by wrapping an :token:`expression` in parentheses. For example:
'''

t("(displayname)",
[
    "abbey aardvaark", 
    "billy billygoat"
])

t % '''
Property Names and `id`
-----------------------

Name tokens not used elsewhere in the grammar are treated as a reference to object properties.
You can specify properties whose name match reserved keywords or have invalid characters by wrapping the property name with "<" and ">". For example, `<where>` or `<a property with spaces>`.

`id` is a reserved name that always refers to the id of the object, not a property named "id".
Such a property can written as `<id>`.
'''

t.model = modelFromJson([
{
"key" : "1",
"namemap" : { "id" : "key"},
"id" : "a property named id",
"a property with spaces" : "this property name has spaces"
}
])

t("{ 'key' : id, <id>, <a property with spaces>}",
[{'a property with spaces': 'this property name has spaces',  
  'id': 'a property named id',
  'key': '1'}]
)

t%'''
Property wildcard ('*')
-----------------------
The "*" will expand to all properties defined for the object. For example, this query retrieves all objects in the store:
'''
t.model = mainmodel
t("{*}", [{'id': 'tag:nonsense', 'label': 'Nonsense', 'type': 'tag'},
 {'auth': [{'facebook_uid': 394090223,
            'name': 'abbey aardvaark',
            'service': 'facebook'},
           {'email': 'aaardvaark@gmail.com',
            'language': 'en',
            'name': 'abbey aardvaark',
            'service': 'google',
            'username': 'aaardvaark'}],
  'displayname': 'abbey aardvaark',
  'id': 'user:1',
  'type': 'user'},
 {'author': 'user:1',
  'contentType': 'text/plain',
  'contents': 'hello world!',
  'id': 'post1',
  'published': '',
  'tags': ['tag:foo'],
  'type': 'post'},
 {'displayname': 'billy billygoat', 'id': 'user:2', 'type': 'user'},
 {'id': 'tag:foo',
  'label': 'foo',
  'subcategoryOf': 'tag:nonsense',
  'type': 'tag'}]
)

t.group = 'lists'

t%'''
Multiple values and lists
-------------------------
* list construction -- multiple values are represented as lists

Note that the actually semantics of inserting pjson depends on the data store it is being inserted into. For example, 
does inserted a property that already exists on an object might add a new value or replace the current one.
'''

listModel = modelFromJson([
{ "id" : "1",
  "a_list" : ["a", "b"]
},
{ "id" : "1",
  "a_list" : "c"
},
{ "id" : "1",
  "a_list" : None,
  "mixed" : ['a', 'b']
},
{ "id" : "2",
  "mixed" : "c"
},
{ "id" : "3",
  "mixed" : None
}
])
#print listModel.getStatements()

t("{ id, a_list }",
[{'a_list': ['a', 'b', 'c', None], 'id': '1'}]
,model = listModel
)

t % '''
"forcelist" syntax
------------------
You can use wrap the property value with brackets to force the value of a property to always be a list, even when the value just as one value or is `null`. If the value is `null`, an empty list (`[]`) will be used. For example, compare the results of the following two examples which are identical except for the second one's use of "forcelist":
'''

t("{ id, mixed }",
[{'id': '1', 'mixed': ['a', 'b']},
 {'id': '3', 'mixed': None},
 {'id': '2', 'mixed': 'c'}]
,model = listModel
)

t % '''

'''

t("{ id, [mixed] }",
[{'id': '1', 'mixed': ['a', 'b']},
 {'id': '3', 'mixed': []},
 {'id': '2', 'mixed': ['c']}]
,model = listModel
)

t%'''
Null values and optional properties
-----------------------------------

results will only include objects that contain the property referenced in the construct list,
For example, the next example just returns one object because only one has a both a displayname and auth property.
'''
t('{displayname, auth}',
[{'auth': [{'facebook_uid': 394090223,
            'name': 'abbey aardvaark',
            'service': 'facebook'},
           {'email': 'aaardvaark@gmail.com',
            'language': 'en',
            'name': 'abbey aardvaark',
            'service': 'google',
            'username': 'aaardvaark'}],
  'displayname': 'abbey aardvaark'}]
)

t%'''
If property references are modified "maybe" before them then objects without that property will be included in the result. For example:
'''
t('{displayname, maybe auth}',
[{'auth': [{'facebook_uid': 394090223,
            'name': 'abbey aardvaark',
            'service': 'facebook'},
           {'email': 'aaardvaark@gmail.com',
            'language': 'en',
            'name': 'abbey aardvaark',
            'service': 'google',
            'username': 'aaardvaark'}],
  'displayname': 'abbey aardvaark'},
 {'displayname': 'billy billygoat',
 'auth': None}]
)

t % '''
This query still specifies that "auth" property appears in every object in the result -- objects that doesn't have a "auth" property defined have that property value set to null. If you do not want the property included in that case, you can use the the `OMITNULL` modifier instead:
''' 
t('{displayname, omitnull auth}',
[{'auth': [{'facebook_uid': 394090223,
            'name': 'abbey aardvaark',
            'service': 'facebook'},
           {'email': 'aaardvaark@gmail.com',
            'language': 'en',
            'name': 'abbey aardvaark',
            'service': 'google',
            'username': 'aaardvaark'}],
  'displayname': 'abbey aardvaark'},
 {'displayname': 'billy billygoat'}]
)

t % '''
The above examples illustrate using MAYBE and OMITNULL on appreviated properties. 
Specifically `maybe property` is an abbreviation for  `'property' : maybe property`
and `omitnull property` is an abbreviation for `omitnull 'property' : maybe property`.

`omitnull` must appear before the property name and takes effect when any property value expression returns null.
For example, here's a silly query that has a "nullproperty" property with a constant value
but it will never be included in the result because of the "omitnull".
'''

t('{displayname, omitnull "nullproperty" : null}',
[{ 'displayname': 'abbey aardvaark'},
 { 'displayname': 'billy billygoat'}]
)

t%'''
The "forcelist" syntax can be combined with `MAYBE` or `OMITNULL`. For example:
'''

t('{displayname, [maybe auth]}',
[{'auth': [{'facebook_uid': 394090223,
            'name': 'abbey aardvaark',
            'service': 'facebook'},
           {'email': 'aaardvaark@gmail.com',
            'language': 'en',
            'name': 'abbey aardvaark',
            'service': 'google',
            'username': 'aaardvaark'}],
  'displayname': 'abbey aardvaark'},
 {'auth': [], 'displayname': 'billy billygoat'}]
)

t%'''
Sub-queries (nested constructs)
-------------------------------

The value of a property or array item can be another object or list construct instead of an expression. 
If a nested query references an object in the outer query (via `labels`) it will be correlated with the outer query.
If it is independent it will be evaluated for each result, so the result set will equivalent to a cross-join.
'''

t%'''
Filtering (the WHERE() clause)
==============================

..note Note: Unlike SQL the WHERE expression must be in a parentheses.

* property references in construct
* matching lists 
'''

t%'''
joins
=====

object references
-----------------

When a filter expression is surrounded by braces (`{` and `}`) the filter is applied 
separately from the rest of the expression, and is evaluated as an object reference
to the object that met that criteria. These object references have the same semantics 
as label references. The object references can optionally be labeled and are typically 
used to create joins.

labels
------

You can create a reference to an object creating object labels, which look this this syntax: `?identifier`. 

By declaring the variable 

Once an objected labels, you can create joins by referencing that label in an expression.

This is example, value of the contains property will be any object that
'''

t('''
    {
    ?parent, 
    *,
    'contains' : { * where (subsumedby = ?parent)}
    }
''')

'''
You can also declare object name inside  
`{ id = ?foo }`
'''
#document:
# when evaluating join expressions are replaced with a label reference to that join
# labels evaluate to the object id of the object except when evaluating as a boolean, 
# in that case it returns true if the object id exists (e.g a label to an object 
#whose id's value was 0 would still evaluate as true)
#note that following these rules, a join expression at the root of the where filter expression 
#(e.g. "where ({ a=1 })") evaluates to true if there exists an object with "a = 1"

t%"find all tag, include child tags in result"
t('''
    {
    ?parent, 
    *,
    'contains' : { where(subsumedby = ?parent)}
    }
''')

t%'''
`maybe` and outer joins
-----------------------
'''

t%'''
object references and anonymous objects
=======================================

If an object is anonymous it will be expanded, otherwise an object reference object will be output. This behavior can be overridden using the `DEPTH` directive, which will force object references to be expanded, even if objects are duplicated. 

'''

'''
Expressions
===========
'''

'''
Groupby and aggregate Functions
===============================
'''

t.group = 'footnotes'
t%'''
.. rubric:: Footnotes

.. [#f1] Note this simplified example isn't very useful since it will merge all user's logins together. Here's a similar query that  returns the login object per user:
'''

t('''
{ "userid" : id, 
  "logins" : {?login 
              service : maybe facebook_uid or maybe email
              MERGEALL
             }
  where (auth = ?login)  
}
''', 
[{'logins': {'facebook': 394090223, 'google': 'aaardvaark@gmail.com'},
  'userid': 'user:1'}]
, model = mainmodel)

t%'''
.. raw:: html

    <style>
    .example-plaintext { position:absolute; z-index: 2; background-color: lightgray;}
    .close-example-plaintext { float:right; padding-right: 3px; font-size: 11px;}
    .close-example-plaintext:hover { color: #CA7900; cursor: pointer; }
    .toolbar { background-color: lightgray; float:right; 
        border:1px solid;
        padding: 1px;
        text-decoration:underline;
    }
    .toolbar:hover { color: #CA7900; cursor: pointer; }
    </style>
    <script>
    $().ready(function(){
      $('.example-plaintext+.highlight-javascript pre').prepend("<span class='toolbar'>copy</span");
      $('.toolbar').click(function() {
        $(this).parents('.highlight-javascript').prev().slideDown('fast').find('textarea').focus();
      });
      $('.close-example-plaintext').click(function() { 
            $(this).parents('.example-plaintext').slideUp('fast').find('textarea').blur(); 
      });
    });
    </script>   

..  colophon: this doc was generated with "python tests/jsonqlDocTest.py --printdoc > doc/source/spec.rst"
'''

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

if __name__ == "__main__":
    main(t) #invoke jqltest.main()