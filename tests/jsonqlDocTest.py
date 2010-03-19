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
Informal Specification
~~~~~~~~~~~~~~~~~~~~~~

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
                 :    (`objectitem` | `objectpair` [","])+ 
                 :     [`query_criteria`] 
                 :  "}"
 constructarray  : "[" [`label`]
                 :  (`arrayitem` [","])+ [`query_criteria`] 
                 : "]"
 constructvalue  : "(" 
                 :    `expression` [`query_criteria`] 
                 : ")"
 objectitem      : `propertyname` | "ID" | "*"
 objectpair      : `expression` ":" (`expression` 
                 : | "*" | `nestedconstruct`)
 arrayitem       : `expression` | "*" | `nestedconstruct`                 
 nestedconstruct : `constructarray` | `constructobject`
 propertyname    : NAME | "<" CHAR+ ">"
 query_criteria  : ["WHERE(" `expression` ")"]
                 : ["GROUPBY(" (`expression`[","])+ ")"]
                 : ["ORDERBY(" (`expression` ["ASC"|"DESC"][","])+ ")"]
                 : ["LIMIT" number]
                 : ["OFFSET" number]
                 : ["DEPTH" number]
                 : ["MERGEALL"]
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
 join       : "{" `expression` "}"
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
:token:`objectitem`
-----------------------
When a single property name appears instead of a name-value pair, it is 
treated as name-value pair where the name is the name of the property and 
the value expression is a reference to property. So the following example is 
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
Property wildcards
------------------
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
Multiple values and nulls
-------------------------
* list construction -- multiple values are represented as lists
* force list, list even when one item and empty lists instead on null
'''

t('''
{ auth
WHERE (type='user')
}
''')

t%'''
Nested constructs
-----------------

cross joins 
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

join expressions
----------------

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
You can also 
Braces "{}" that occur within the where clause indicate that 
where ( { foo = 1 } ) 

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
object references and anonymous objects
=======================================

If an object is anonymous it will be expanded, otherwise the object's id will be output. This behaviour can be overridden using DEPTH directive, which will force object references to be expanded, even if objects are duplicated. 

'''



'''
Expressions
===========
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
..  colophon: this doc was generated with "python tests/jsonqlDocTest.py --printdoc > doc/source/spec.rst"
'''

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

if __name__ == "__main__":
    main(t) #invoke jqltest.main()