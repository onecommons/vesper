import sys
sys.path.append('.')
from jqltester import *

t = Suite()
skip = Suite()

###################################
########### basic tests ###########
###################################
t.model = modelFromJson([

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

t % '''
Overview
~~~~~~~~~

JQL is languages for querying data that can represented in JSON. A JQL implementation provides a mapping from objects in a backend datastore to a collection of JSON objects with properties (for example, each object might correspond to a row in table, with a property for each column). A JQL query operates on that mapping in a manner similar to a SQL query except that instead of returning rows it returns JSON data structures based on the pattern specified in the query.

Below is simplifed representation of the JQL grammar (the formal grammar can be found [here|grammar]). We'll go through each element and provide sample queries illustrating each feature of the language. The queries and sample results are based on the sample json used by the [tutorial] (which, btw, might be a better place to start learning about JQL). 

.. productionlist::
 query: `constructobject` | `constructarray` | `constructvalue`

 constructobject : "{" [`label`]
                 :    (`objectitem` | `objectpair` | "*" [","])+
                 :    [`query_criteria`] 
                 :  "}"

 constructarray  : "[" [`label`]
                 :    (`arrayitem` [","])+
                 :    [`query_criteria`] 
                 : "]"

 constructvalue  : "(" 
                 :    `expression` 
                 :    [`query_criteria`] 
                 : ")"

 arrayitem : `expression` | "*" 
 
 objectitem : `propertyname` | "*"
 
 objectpair : `expression` ":" (`expression` | `constructarray` | `constructobject`)

 propertyname : NAME | "<" CHAR+ ">"
  
 query_criteria : ["WHERE(" `expression` ")"]
                : ["GROUPBY(" (`expression`[","])+ ")"]
                : ["ORDERBY(" (`expression` ["ASC"|"DESC"][","])+ ")"]
                : ["LIMIT" number]
                : ["OFFSET" number]
                : ["DEPTH" number]

Patterns
========

There are three top level constructions depending on whether you want generate JSON objects (dictionaries), arrays (lists) or simple value (such as a string or number).

JQL query consists of a pattern describes a JSON object (dictionary), a list (array) or simple value -- executing query will return a list of instances of that pattern. These basic patterns are:

'''

t('''{ 
    "username" : displayname,
    "type" : type
    }
''', [
        {
            "type": "user", 
            "username": "abbey aardvaark"
        }, 
        {
            "type": "user", 
            "username": "billy billygoat"
        }
    ]
)


t%'''
Create arrays:
This query selects the same objects but it formats each result as a list not an object.
'''

t("[id, displayname]", {
    "results": [
        [
            "user:1", 
            "abbey aardvaark"
        ], 
        [
            "user:2", 
            "billy billygoat"
        ]
    ]
})

t%'''
strings:
'''

t("(displayname)",
[
    "abbey aardvaark", 
    "billy billygoat"
])

'''
We can abbreviate as:

'''

t("{ displayname, type }")


'''
* object properties lists
* abbreviated property lists
* mix and match 

The * will expand to all properties defined for the object. For example, this query retrieves all objects in the store:

Property and array item lists 
=============================



'''

t("{*}")


t%'''
Filtering (where clause)
======

Constructing a JSON object (dictionary) specify 
'''

t%'''
joins
=====

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
Braces "{}" that occurr within the where clause indicate that 
where ( { foo = 1 } ) 

You can also declare object name inside  
`{ id = ?foo }`
'''



t%"find all tag, include child tags in result"
t('''
    {
    id : ?parent, 
    *,
    'contains' : { where(subsumedby = ?parent)}
    }
''')

'''
Objects, id and anonymous objects
=================================

If an object is anonymous it will be expanded, otherwise the object's id will be output. This behaviour can be overridden using DEPTH directive, which will force object references to be expanded, even if objects are duplicated. 
'''

#force an individual property value reference to expand like this: foo.*
'''
Expressions
===========
'''

t%'''
..  colophon: this doc was generated with "python tests/tutorialTest.py --printdoc > doc/source/jsonql.rst"
'''

import unittest
class JQLTestCase(unittest.TestCase):
    def testAll(self):
        main(t, ['--quiet'])

if __name__ == "__main__":
    main(t) #invoke jqltest.main()