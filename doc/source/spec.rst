

Informal Specification
~~~~~~~~~~~~~~~~~~~~~~

jsonQL is languages for querying data that can represented in JSON. A jsonQL implementation provides a mapping from objects in a backend datastore to a collection of JSON objects with properties (for example, each object might correspond to a row in table, with a property for each column). A jsonQL query operates on that mapping in a manner similar to a SQL query except that instead of returning rows it returns JSON data structures based on the pattern specified in the query.

Below is simplifed representation of the JQL grammar (the formal grammar can be found :doc:`here <grammar>`). We'll go through each element and provide sample queries illustrating each feature of the language. The queries and sample results are based on the sample json used by the [tutorial] (which, btw, might be a better place to start learning about JQL). 

.. productionlist::
 query  : `constructobject` 
        :| `constructarray` 
        :| `constructvalue`
 constructobject : "{" [`label`]
                 :    (`objectitem` | `objectpair` | "*" [","])+ 
                 :     [`query_criteria`] 
                 :  "}"
 constructarray  : "[" [`label`]
                 :  (`arrayitem` [","])+ [`query_criteria`] 
                 : "]"
 constructvalue  : "(" 
                 :    `expression` [`query_criteria`] 
                 : ")"
 arrayitem       : `expression` | "*" 
 objectitem      : `propertyname` | "id" | "*"
 objectpair      : `expression` ":" (`expression` 
                 : | `constructarray` | `constructobject`)
 propertyname    : NAME | "<" CHAR+ ">"
 query_criteria  : ["WHERE(" `expression` ")"]
                 : ["GROUPBY(" (`expression`[","])+ ")"]
                 : ["ORDERBY(" (`expression` ["ASC"|"DESC"][","])+ ")"]
                 : ["LIMIT" number]
                 : ["OFFSET" number]
                 : ["DEPTH" number]
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

JQL query consists of a pattern describes a JSON object (dictionary), a list (array) or simple value -- executing query will return a list of instances of that pattern. These basic patterns are:


 >>> model1 = app.createStore('''[{'author': 'user:1',
 ...   'contentType': 'text/plain',
 ...   'contents': 'hello world!',
 ...   'id': 'post1',
 ...   'published': '',
 ...   'tags': ['tag:foo'],
 ...   'type': 'post'},
 ...  {'id': 'tag:foo',
 ...   'label': 'foo',
 ...   'subcategoryOf': 'tag:nonsense',
 ...   'type': 'tag'},
 ...  {'id': 'tag:nonsense', 'label': 'Nonsense', 'type': 'tag'},
 ...  {'auth': [{'facebook_uid': 394090223,
 ...             'name': 'abbey aardvaark',
 ...             'service': 'facebook'},
 ...            {'email': 'aaardvaark@gmail.com',
 ...             'language': 'en',
 ...             'name': 'abbey aardvaark',
 ...             'service': 'google',
 ...             'username': 'aaardvaark'}],
 ...   'displayname': 'abbey aardvaark',
 ...   'id': 'user:1',
 ...   'type': 'user'},
 ...  {'displayname': 'billy billygoat', 'id': 'user:2', 'type': 'user'}]''')

 >>> model1.query('''{ 
 ...     "username" : displayname,
 ...     "type" : type
 ...     }
 ... ''')
[{'type': 'user', 'username': 'abbey aardvaark'},
  {'type': 'user', 'username': 'billy billygoat'}]



This query selects the same objects but it formats each result as a list not an object.

 >>> model1.query('''[id, displayname]''')
[['user:1', 'abbey aardvaark'], ['user:2', 'billy billygoat']]



:token:`constructvalue`
You can select individual values (strings or numbers) by wrapping an :token:`expression`  

 >>> model1.query('''(displayname)''')
['abbey aardvaark', 'billy billygoat']



Filtering (the WHERE() clause)
==============================

Note: Unlike SQL the WHERE expression must be in a parentheses.



joins
=====

You can create a reference to an object creating object labels, which look this this syntax: `?identifier`. 

By declaring the variable 

Once an objected labels, you can create joins by referencing that label in an expression.

This is example, value of the contains property will be any object that

 >>> model1.query('''
 ...     {
 ...     ?parent, 
 ...     *,
 ...     'contains' : { * where (subsumedby = ?parent)}
 ...     }
 ... ''')
None


find all tag, include child tags in result
 >>> model1.query('''
 ...     {
 ...     ?parent, 
 ...     *,
 ...     'contains' : { where(subsumedby = ?parent)}
 ...     }
 ... ''')
None



Objects, id and anonymous objects
=================================

If an object is anonymous it will be expanded, otherwise the object's id will be output. This behaviour can be overridden using DEPTH directive, which will force object references to be expanded, even if objects are duplicated. 


Property Names and `id`
-----------------------

Name tokens not used elsewhere in the grammar are treated as a reference to object properties.
You can specify properties whose name match reserved keywords or have invalid characters by wrapping the property name with "<" and ">". For example, `<where>` or `<a property with spaces>`.

`id` is a reserved name that always refers to the id of the object, not a property named "id".
Such a property can written as `<id>`.

 >>> model2 = app.createStore('''[{'a property with spaces': 'this property name has spaces',
 ...   'id': 'a property named id',
 ...   'key': '1',
 ...   'namemap': {'id': 'key'}}]''')

 >>> model2.query('''{ 'key' : id, <id>, <a property with spaces>}''')
[{'a property with spaces': 'this property name has spaces',
   'id': 'a property named id',
   'key': '1'}]


..  colophon: this doc was generated with "python tests/jsonqlDocTest.py --printdoc > doc/source/spec.rst"

