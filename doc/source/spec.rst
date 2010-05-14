

jsonQL Reference 
~~~~~~~~~~~~~~~~

jsonQL is languages for querying data that can represented in JSON. A jsonQL implementation provides a mapping from objects in a backend datastore to a collection of JSON objects with properties (for example, each object might correspond to a row in table, with a property for each column). A jsonQL query operates on that mapping in a manner similar to a SQL query except that instead of returning rows it returns JSON data structures based on the pattern specified in the query.

The examples here are based on the following example. You can cut an paste or you can run the admin tool on the sample store. 


 >>> model1 = app.createStore(
 ... '''[
 ...   {
 ...     "contentType": "text/plain", 
 ...     "author": "user:1", 
 ...     "tags": [
 ...       "tag:foo"
 ...     ], 
 ...     "published": "", 
 ...     "type": "post", 
 ...     "id": "post1", 
 ...     "contents": "hello world!"
 ...   }, 
 ...   {
 ...     "subcategoryOf": "tag:nonsense", 
 ...     "type": "tag", 
 ...     "id": "tag:foo", 
 ...     "label": "foo"
 ...   }, 
 ...   {
 ...     "type": "tag", 
 ...     "id": "tag:nonsense", 
 ...     "label": "Nonsense"
 ...   }, 
 ...   {
 ...     "displayname": "abbey aardvaark", 
 ...     "type": "user", 
 ...     "id": "user:1", 
 ...     "auth": [
 ...       {
 ...         "name": "abbey aardvaark", 
 ...         "service": "facebook", 
 ...         "facebook_uid": 394090223
 ...       }, 
 ...       {
 ...         "username": "aaardvaark", 
 ...         "language": "en", 
 ...         "name": "abbey aardvaark", 
 ...         "service": "google", 
 ...         "email": "aaardvaark@gmail.com"
 ...       }
 ...     ]
 ...   }, 
 ...   {
 ...     "displayname": "billy billygoat", 
 ...     "type": "user", 
 ...     "id": "user:2"
 ...   }
 ... ]''')


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



.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='3' cols='60'>{ 
      "displayname" : displayname,
      "type" : type
      }</textarea></div>

.. code-block:: javascript

 { 
      "displayname" : displayname,
      "type" : type
      }

.. code-block:: python

    
 >>> model1.query(
 ... '''{ 
 ...     "displayname" : displayname,
 ...     "type" : type
 ...     }
 ... ''')
 [
   {
     "type": "user", 
     "displayname": "abbey aardvaark"
   }, 
   {
     "type": "user", 
     "displayname": "billy billygoat"
   }
 ]



Both the property name and value are expressions. In this example, the property names is simply string constants while the property value are property references. In the next example, the property name is a property reference and property value is a
more complex expression. It uses the MERGEALL option to return a single dictionary of login services where the name of the service is the property and the value depends on the type of service. [#f1]_


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='3' cols='60'>{
    service : maybe facebook_uid or maybe email
    MERGEALL 
  }</textarea></div>

.. code-block:: javascript

 {
    service : maybe facebook_uid or maybe email
    MERGEALL 
  }

.. code-block:: python

    
 >>> model1.query(
 ... '''{
 ...   service : maybe facebook_uid or maybe email
 ...   MERGEALL 
 ... }''')
 [
   {
     "google": "aaardvaark@gmail.com", 
     "facebook": 394090223
   }
 ]



Abbreviated properties: :token:`objectitem`
-------------------------------------------
When a single property name appears instead of a name-value pair, it is 
treated as a name-value pair where the name is the name of the property and 
the value is a reference to the property. So the following example is 
equivalent to the first query: 


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{ displayname, type }</textarea></div>

.. code-block:: javascript

 { displayname, type }

.. code-block:: python

    
 >>> model1.query(
 ... '''{ displayname, type }''')
 [
   {
     "type": "user", 
     "displayname": "abbey aardvaark"
   }, 
   {
     "type": "user", 
     "displayname": "billy billygoat"
   }
 ]



:token:`constructarray`
-----------------------
You can also construct results as arrays (lists) instead of objects. This query selects the same objects but it formats each result as a list not an object.


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>[displayname, type]</textarea></div>

.. code-block:: javascript

 [displayname, type]

.. code-block:: python

    
 >>> model1.query(
 ... '''[displayname, type]''')
 [
   [
     "abbey aardvaark", 
     "user"
   ], 
   [
     "billy billygoat", 
     "user"
   ]
 ]



:token:`constructvalue`
-----------------------

You can select individual values (strings or numbers) by wrapping an :token:`expression` in parentheses. For example:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>(displayname)</textarea></div>

.. code-block:: javascript

 (displayname)

.. code-block:: python

    
 >>> model1.query(
 ... '''(displayname)''')
 [
   "abbey aardvaark", 
   "billy billygoat"
 ]



Property Names and `id`
-----------------------

Name tokens not used elsewhere in the grammar are treated as a reference to object properties.
You can specify properties whose name match reserved keywords or have invalid characters by wrapping the property name with "<" and ">". For example, `<where>` or `<a property with spaces>`.

`id` is a reserved name that always refers to the id of the object, not a property named "id".
Such a property can written as `<id>`.


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>
  model2 = app.createStore(
  '''[
    {
      "a property with spaces": "this property name has spaces", 
      "namemap": {
        "id": "key"
      }, 
      "key": "1", 
      "id": "a property named id"
    }
  ]''')

{ 'key' : id, <id>, <a property with spaces>}</textarea></div>

.. code-block:: javascript

 { 'key' : id, <id>, <a property with spaces>}

.. code-block:: python

    
 >>> model2 = app.createStore(
 ... '''[
 ...   {
 ...     "a property with spaces": "this property name has spaces", 
 ...     "namemap": {
 ...       "id": "key"
 ...     }, 
 ...     "key": "1", 
 ...     "id": "a property named id"
 ...   }
 ... ]''')

 >>> model2.query(
 ... '''{ 'key' : id, <id>, <a property with spaces>}''')
 [
   {
     "id": "a property named id", 
     "key": "1", 
     "a property with spaces": "this property name has spaces"
   }
 ]



Property wildcard ('*')
-----------------------
The "*" will expand to all properties defined for the object. For example, this query retrieves all objects in the store:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{*}</textarea></div>

.. code-block:: javascript

 {*}

.. code-block:: python

    
 >>> model1.query(
 ... '''{*}''')
 [
   {
     "type": "tag", 
     "id": "tag:nonsense", 
     "label": "Nonsense"
   }, 
   {
     "type": "user", 
     "displayname": "abbey aardvaark", 
     "id": "user:1", 
     "auth": [
       {
         "name": "abbey aardvaark", 
         "service": "facebook", 
         "facebook_uid": 394090223
       }, 
       {
         "username": "aaardvaark", 
         "service": "google", 
         "email": "aaardvaark@gmail.com", 
         "language": "en", 
         "name": "abbey aardvaark"
       }
     ]
   }, 
   {
     "contentType": "text/plain", 
     "tags": [
       "tag:foo"
     ], 
     "author": "user:1", 
     "published": "", 
     "type": "post", 
     "id": "post1", 
     "contents": "hello world!"
   }, 
   {
     "type": "user", 
     "displayname": "billy billygoat", 
     "id": "user:2"
   }, 
   {
     "type": "tag", 
     "subcategoryOf": "tag:nonsense", 
     "id": "tag:foo", 
     "label": "foo"
   }
 ]



Multiple values and lists
-------------------------
* list construction -- multiple values are represented as lists

Note that the actually semantics of inserting pjson depends on the data store it is being inserted into. For example, 
does inserted a property that already exists on an object might add a new value or replace the current one.


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>
  model3 = app.createStore(
  '''[
    {
      "a_list": [
        "a", 
        "b"
      ], 
      "id": "1"
    }, 
    {
      "a_list": "c", 
      "id": "1"
    }, 
    {
      "mixed": [
        "a", 
        "b"
      ], 
      "a_list": null, 
      "id": "1"
    }, 
    {
      "mixed": "c", 
      "id": "2"
    }, 
    {
      "mixed": null, 
      "id": "3"
    }
  ]''')

{ id, a_list }</textarea></div>

.. code-block:: javascript

 { id, a_list }

.. code-block:: python

    
 >>> model3 = app.createStore(
 ... '''[
 ...   {
 ...     "a_list": [
 ...       "a", 
 ...       "b"
 ...     ], 
 ...     "id": "1"
 ...   }, 
 ...   {
 ...     "a_list": "c", 
 ...     "id": "1"
 ...   }, 
 ...   {
 ...     "mixed": [
 ...       "a", 
 ...       "b"
 ...     ], 
 ...     "a_list": null, 
 ...     "id": "1"
 ...   }, 
 ...   {
 ...     "mixed": "c", 
 ...     "id": "2"
 ...   }, 
 ...   {
 ...     "mixed": null, 
 ...     "id": "3"
 ...   }
 ... ]''')

 >>> model3.query(
 ... '''{ id, a_list }''')
 [
   {
     "a_list": [
       "a", 
       "b", 
       "c", 
       null
     ], 
     "id": "1"
   }
 ]



"forcelist" syntax
------------------
You can use wrap the property value with brackets to force the value of a property to always be a list, even when the value just as one value or is `null`. If the value is `null`, an empty list (`[]`) will be used. For example, compare the results of the following two examples which are identical except for the second one's use of "forcelist":


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{ id, mixed }</textarea></div>

.. code-block:: javascript

 { id, mixed }

.. code-block:: python

    
 >>> model3.query(
 ... '''{ id, mixed }''')
 [
   {
     "mixed": [
       "a", 
       "b"
     ], 
     "id": "1"
   }, 
   {
     "mixed": null, 
     "id": "3"
   }, 
   {
     "mixed": "c", 
     "id": "2"
   }
 ]






.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{ id, [mixed] }</textarea></div>

.. code-block:: javascript

 { id, [mixed] }

.. code-block:: python

    
 >>> model3.query(
 ... '''{ id, [mixed] }''')
 [
   {
     "mixed": [
       "a", 
       "b"
     ], 
     "id": "1"
   }, 
   {
     "mixed": [], 
     "id": "3"
   }, 
   {
     "mixed": [
       "c"
     ], 
     "id": "2"
   }
 ]



Null values and optional properties
-----------------------------------

results will only include objects that contain the property referenced in the construct list,
For example, the next example just returns one object because only one has a both a displayname and auth property.


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{displayname, auth}</textarea></div>

.. code-block:: javascript

 {displayname, auth}

.. code-block:: python

    
 >>> model1.query(
 ... '''{displayname, auth}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "auth": [
       {
         "name": "abbey aardvaark", 
         "service": "facebook", 
         "facebook_uid": 394090223
       }, 
       {
         "username": "aaardvaark", 
         "service": "google", 
         "email": "aaardvaark@gmail.com", 
         "language": "en", 
         "name": "abbey aardvaark"
       }
     ]
   }
 ]



If property references are modified "maybe" before them then objects without that property will be included in the result. For example:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{displayname, maybe auth}</textarea></div>

.. code-block:: javascript

 {displayname, maybe auth}

.. code-block:: python

    
 >>> model1.query(
 ... '''{displayname, maybe auth}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "auth": [
       {
         "name": "abbey aardvaark", 
         "service": "facebook", 
         "facebook_uid": 394090223
       }, 
       {
         "username": "aaardvaark", 
         "service": "google", 
         "email": "aaardvaark@gmail.com", 
         "language": "en", 
         "name": "abbey aardvaark"
       }
     ]
   }, 
   {
     "displayname": "billy billygoat", 
     "auth": null
   }
 ]



This query still specifies that "auth" property appears in every object in the result -- objects that doesn't have a "auth" property defined have that property value set to null. If you do not want the property included in that case, you can use the the `OMITNULL` modifier instead:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{displayname, omitnull auth}</textarea></div>

.. code-block:: javascript

 {displayname, omitnull auth}

.. code-block:: python

    
 >>> model1.query(
 ... '''{displayname, omitnull auth}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "auth": [
       {
         "name": "abbey aardvaark", 
         "service": "facebook", 
         "facebook_uid": 394090223
       }, 
       {
         "username": "aaardvaark", 
         "service": "google", 
         "email": "aaardvaark@gmail.com", 
         "language": "en", 
         "name": "abbey aardvaark"
       }
     ]
   }, 
   {
     "displayname": "billy billygoat"
   }
 ]



The above examples illustrate using MAYBE and OMITNULL on appreviated properties. 
Specifically `maybe property` is an abbreviation for  `'property' : maybe property`
and `omitnull property` is an abbreviation for `omitnull 'property' : maybe property`.

`omitnull` must appear before the property name and takes effect when any property value expression returns null.
For example, here's a silly query that has a "nullproperty" property with a constant value
but it will never be included in the result because of the "omitnull".


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{displayname, omitnull "nullproperty" : null}</textarea></div>

.. code-block:: javascript

 {displayname, omitnull "nullproperty" : null}

.. code-block:: python

    
 >>> model1.query(
 ... '''{displayname, omitnull "nullproperty" : null}''')
 [
   {
     "displayname": "abbey aardvaark"
   }, 
   {
     "displayname": "billy billygoat"
   }
 ]



The "forcelist" syntax can be combined with `MAYBE` or `OMITNULL`. For example:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='0' cols='60'>{displayname, [maybe auth]}</textarea></div>

.. code-block:: javascript

 {displayname, [maybe auth]}

.. code-block:: python

    
 >>> model1.query(
 ... '''{displayname, [maybe auth]}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "auth": [
       {
         "name": "abbey aardvaark", 
         "service": "facebook", 
         "facebook_uid": 394090223
       }, 
       {
         "username": "aaardvaark", 
         "service": "google", 
         "email": "aaardvaark@gmail.com", 
         "language": "en", 
         "name": "abbey aardvaark"
       }
     ]
   }, 
   {
     "displayname": "billy billygoat", 
     "auth": []
   }
 ]



Sub-queries (nested constructs)
-------------------------------

The value of a property or array item can be another object or list construct instead of an expression. 
If a nested query references an object in the outer query (via `labels`) it will be correlated with the outer query.
If it is independent it will be evaluated for each result, so the result set will equivalent to a cross-join.


Filtering (the WHERE() clause)
==============================

..note Note: Unlike SQL the WHERE expression must be in a parentheses.

* property references in construct
* matching lists 


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


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='4' cols='60'>{
      ?parent, 
      *,
      'contains' : { * where (subsumedby = ?parent)}
      }</textarea></div>

.. code-block:: javascript

 {
      ?parent, 
      *,
      'contains' : { * where (subsumedby = ?parent)}
      }

.. code-block:: python

    
 >>> model1.query(
 ... '''
 ...     {
 ...     ?parent, 
 ...     *,
 ...     'contains' : { * where (subsumedby = ?parent)}
 ...     }
 ... ''')
 null


find all tag, include child tags in result

.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='4' cols='60'>{
      ?parent, 
      *,
      'contains' : { where(subsumedby = ?parent)}
      }</textarea></div>

.. code-block:: javascript

 {
      ?parent, 
      *,
      'contains' : { where(subsumedby = ?parent)}
      }

.. code-block:: python

    
 >>> model1.query(
 ... '''
 ...     {
 ...     ?parent, 
 ...     *,
 ...     'contains' : { where(subsumedby = ?parent)}
 ...     }
 ... ''')
 null



`maybe` and outer joins
-----------------------


object references and anonymous objects
=======================================

If an object is anonymous it will be expanded, otherwise an object reference object will be output. This behavior can be overridden using the `DEPTH` directive, which will force object references to be expanded, even if objects are duplicated. 



.. rubric:: Footnotes

.. [#f1] Note this simplified example isn't very useful since it will merge all user's logins together. Here's a similar query that  returns the login object per user:


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span></div>  
  <textarea rows='6' cols='60'>{ "userid" : id, 
    "logins" : {?login 
                service : maybe facebook_uid or maybe email
                MERGEALL
               }
    where (auth = ?login)  
  }</textarea></div>

.. code-block:: javascript

 { "userid" : id, 
    "logins" : {?login 
                service : maybe facebook_uid or maybe email
                MERGEALL
               }
    where (auth = ?login)  
  }

.. code-block:: python

    
 >>> model1.query(
 ... '''
 ... { "userid" : id, 
 ...   "logins" : {?login 
 ...               service : maybe facebook_uid or maybe email
 ...               MERGEALL
 ...              }
 ...   where (auth = ?login)  
 ... }
 ... ''')
 [
   {
     "logins": {
       "google": "aaardvaark@gmail.com", 
       "facebook": 394090223
     }, 
     "userid": "user:1"
   }
 ]


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

