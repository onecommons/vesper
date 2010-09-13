

jsonQL Reference 
~~~~~~~~~~~~~~~~

jsonQL is a language for querying data that can be represented in JSON. Abstractly, a jsonQL query operates on collection of JSON objects that conform to :doc:`pjson` semantics. More concretely, jsonQL works with a Vesper datastore, which provides a logical mapping between objects in a backend datastore to a collection of JSON objects (for example, each object might correspond to a row in table, with a property for each column). A jsonQL query operates on that mapping in a manner similar to a SQL query except that instead of returning rows it returns JSON data structures based on the pattern specified in the query.


Unless otherwise specified, the example queries here are based on the example datastore found in the :doc:`tutorial`. You can cut and paste or you can run the admin tool on the sample store. 



.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='48' cols='60'>
 from vesper import app
 model1 = app.createStore(
 '''[
    {
      "type": "post", 
      "id": "post1", 
      "contents": "a post", 
      "author": "@user:1"
    }, 
    {
      "contents": "a comment", 
      "type": "comment", 
      "id": "comment1", 
      "parent": "@post1", 
      "author": "@user:2"
    }, 
    {
      "author": "@user:1", 
      "type": "comment", 
      "id": "comment2", 
      "parent": "@comment1", 
      "contents": "a reply"
    }, 
    {
      "author": "@user:1", 
      "type": "comment", 
      "id": "comment3", 
      "parent": "@comment4", 
      "contents": "different parent"
    }, 
    {
      "displayname": "abbey aardvaark", 
      "type": "user", 
      "id": "user:1", 
      "email": [
        "abbey@aardvaark.com", 
        "abbey_aardvaak@gmail.com"
      ]
    }, 
    {
      "displayname": "billy billygoat", 
      "type": "user", 
      "id": "user:2"
    }
  ]''')


  </textarea></div>


.. code-block:: python


 >>> from vesper import app
 >>> model1 = app.createStore(
 ... '''[
 ...   {
 ...     "type": "post", 
 ...     "id": "post1", 
 ...     "contents": "a post", 
 ...     "author": "@user:1"
 ...   }, 
 ...   {
 ...     "contents": "a comment", 
 ...     "type": "comment", 
 ...     "id": "comment1", 
 ...     "parent": "@post1", 
 ...     "author": "@user:2"
 ...   }, 
 ...   {
 ...     "author": "@user:1", 
 ...     "type": "comment", 
 ...     "id": "comment2", 
 ...     "parent": "@comment1", 
 ...     "contents": "a reply"
 ...   }, 
 ...   {
 ...     "author": "@user:1", 
 ...     "type": "comment", 
 ...     "id": "comment3", 
 ...     "parent": "@comment4", 
 ...     "contents": "different parent"
 ...   }, 
 ...   {
 ...     "displayname": "abbey aardvaark", 
 ...     "type": "user", 
 ...     "id": "user:1", 
 ...     "email": [
 ...       "abbey@aardvaark.com", 
 ...       "abbey_aardvaak@gmail.com"
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

Below is simplifed representation of the JQL grammar (the formal grammar can be found :doc:`here <grammar>`). This reference guide will walk through each element of language and provide sample queries illustrating each feature of the language. The queries and sample results are based on the sample json used by the [tutorial] (which, btw, might be a better place to start learning about JQL). 

A jsonQL query consists of a "construct pattern" that describes the JSON output, which can be any JSON type: an object, an array or a simple value like a string. The syntax for jsonQL construct patterns is:

.. productionlist::
 query  : `constructobject` 
        :| `constructarray` 
        :| `constructvalue`
 constructobject : "{" [`label`]
                 :    (`objectitem` | `abbreviateditem` [","])+ 
                 :     [`query_criteria`] 
                 :  "}"
 constructarray  : "[" [`label`]
                 :  (`propertyvalue` [","])+ [`query_criteria`] 
                 : "]"
 constructvalue  : "(" 
                 :    `expression` [`query_criteria`] 
                 : ")"
 objectitem      :  `expression` ":" ["["] ["omitnull"] ["maybe"] `propertyvalue` ["]"]
 propertyvalue   : `expression` | "*" | `nestedconstruct`
 nestedconstruct : `constructarray` | `constructobject`
 abbreviateditem : "ID" | "*" | ["["] ["omitnull"] ["maybe"] `propertyname` ["]"]
 propertyname    : NAME | "<" CHAR+ ">"
 query_criteria  : ["WHERE" `expression`]
                 : ["GROUP BY" (`expression`[","])+]
                 : ["ORDER BY" (`expression` ["ASC"|"DESC"][","])+]
                 : ["LIMIT" number]
                 : ["OFFSET" number]
                 : ["DEPTH" number]
                 : ["MERGEALL"]
                 : ["NAMEMAP" "=" `namemapdict`]
 namemapdict     : "{" [((NAME | STRING) ":" (STRING | `namemapdict`) ","?)+] "}"

The syntax for jsonQL expressions is:

.. productionlist::                 
 expression : `expression` "and" `expression`
            : | `expression` "or" `expression`
            : | "maybe" `expression`
            : | "not" `expression`
            : | `expression` `operator` `expression`
            : | `filterset`
            : | `atom`
            : | "(" `expression` ")"
 operator   : "+" | "-" | "*" | "/" | "%" | "=" | "=="
            : | "<" | "<=" | ">" | "=>" | ["not"] "in"  
 filterset : "{" [`label`] `expression` "}"
 atom       : `label` | `bindvar` | `constant` | `objectreference`
            : | `functioncall` | `propertyreference`
 label      : "?"NAME
 bindvar    : ":"NAME
 objectreference : "@"NAME | "@<" CHAR+ ">"
 propertyreference : [`label`"."]`propertyname`["."`propertyname`]+
 functioncall : NAME([`expression`[","]]+ [NAME"="`expression`[","]]+)
 constant : STRING | NUMBER | "true" | "false" | "null"

Construct Patterns
==================

There are three top level constructions depending on whether you want to construct results as JSON objects (dictionaries), arrays (lists) or simple values (such as a string or number).

A jsonQL query consists of a pattern describes a JSON object (dictionary), a list (array) or simple value -- executing query will construct a list of objects that match the pattern. This example returns a list of all the objects that have properties named "displayname" and "type":



.. code-block:: jsonql

 { 
      "displayname" : displayname,
      "type" : type
      }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='7' cols='60'>
 model1.query(
   '''{ 
      "displayname" : displayname,
      "type" : type
      }''')

  </textarea></div>


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



Both the property name and value are expressions. In this example, the property names is simply string constants while the property value are property references. In the next example, the property name is the object id and property value is a
more complex expression. It uses the MERGEALL option to return a single dictionary that is a merge of the results.


.. code-block:: jsonql

 {
    id : upper(displayname)
    MERGEALL 
  }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='7' cols='60'>
 model1.query(
   '''{
    id : upper(displayname)
    MERGEALL 
  }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{
 ...   id : upper(displayname)
 ...   MERGEALL 
 ... }''')
 [
   {
     "user:1": "ABBEY AARDVAARK", 
     "user:2": "BILLY BILLYGOAT"
   }
 ]



Abbreviated properties: :token:`objectitem`
-------------------------------------------
When a single property name appears instead of a name-value pair, it is 
treated as a name-value pair where the name is the name of the property and 
the value is a reference to the property. So the following example is 
equivalent to the first query: 


.. code-block:: jsonql

 { displayname, type }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{ displayname, type }''')

  </textarea></div>


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


.. code-block:: jsonql

 [displayname, type]


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''[displayname, type]''')

  </textarea></div>


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

You can select simple values (strings or numbers) by wrapping an :token:`expression` in parentheses. For example:


.. code-block:: jsonql

 (displayname)


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''(displayname)''')

  </textarea></div>


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
You can specify properties whose name match reserved keywords or have illegal characters by wrapping the property name with "<" and ">". For example, `<where>` or `<a property with spaces>`.

`id` is a reserved name that always refers to the id of the object, not a property named "id".
Such a property can written as `<id>`.


.. code-block:: jsonql

 { 'key' : id, <id>, <a property with spaces>}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='18' cols='60'>
 from vesper import app
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


 model2.query(
   '''{ 'key' : id, <id>, <a property with spaces>}''')

  </textarea></div>


.. code-block:: python

 >>> from vesper import app
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
     "key": "@1", 
     "a property with spaces": "this property name has spaces"
   }
 ]



Property wildcard ('*')
-----------------------
The "*" will expand to all properties defined for the object. For example, this query retrieves all objects in the store:


.. code-block:: jsonql

 {*}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{*}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{*}''')
 [
   {
     "parent": "@post1", 
     "type": "comment", 
     "id": "comment1", 
     "contents": "a comment", 
     "author": "@user:2"
   }, 
   {
     "type": "user", 
     "displayname": "abbey aardvaark", 
     "email": [
       "abbey@aardvaark.com", 
       "abbey_aardvaak@gmail.com"
     ], 
     "id": "user:1"
   }, 
   {
     "type": "post", 
     "id": "post1", 
     "contents": "a post", 
     "author": "@user:1"
   }, 
   {
     "parent": "@comment1", 
     "type": "comment", 
     "id": "comment2", 
     "contents": "a reply", 
     "author": "@user:1"
   }, 
   {
     "parent": "@comment4", 
     "type": "comment", 
     "id": "comment3", 
     "contents": "different parent", 
     "author": "@user:1"
   }, 
   {
     "type": "user", 
     "displayname": "billy billygoat", 
     "id": "user:2"
   }
 ]



"forcelist" syntax
------------------
You can use wrap the property value with brackets to force the value of a property to always be a list, even when the value just as one value or is `null`. If the value is `null`, an empty list (`[]`) will be used. For example, compare the results of the following two examples which are identical except for the second one's use of "forcelist":


.. code-block:: jsonql

 { id, mixed }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='37' cols='60'>
 from vesper import app
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


 model3.query(
   '''{ id, mixed }''')

  </textarea></div>


.. code-block:: python

 >>> from vesper import app
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






.. code-block:: jsonql

 { id, [mixed] }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model3.query(
   '''{ id, [mixed] }''')

  </textarea></div>


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


.. code-block:: jsonql

 {displayname, email}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{displayname, email}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{displayname, email}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "email": [
       "abbey@aardvaark.com", 
       "abbey_aardvaak@gmail.com"
     ]
   }
 ]



If property references are modified "maybe" before them then objects without that property will be included in the result. For example:


.. code-block:: jsonql

 {displayname, maybe email}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{displayname, maybe email}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{displayname, maybe email}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "email": [
       "abbey@aardvaark.com", 
       "abbey_aardvaak@gmail.com"
     ]
   }, 
   {
     "displayname": "billy billygoat", 
     "email": null
   }
 ]



This query still specifies that "auth" property appears in every object in the result -- objects that doesn't have a "auth" property defined have that property value set to null. If you do not want the property included in that case, you can use the the `OMITNULL` modifier instead:


.. code-block:: jsonql

 {displayname, omitnull maybe email}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{displayname, omitnull maybe email}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{displayname, omitnull maybe email}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "email": [
       "abbey@aardvaark.com", 
       "abbey_aardvaak@gmail.com"
     ]
   }, 
   {
     "displayname": "billy billygoat"
   }
 ]



The above examples illustrate using MAYBE and OMITNULL on appreviated properties. 
Specifically `maybe property` is an abbreviation for  `'property' : maybe property`
and `omitnull property` is an abbreviation for `'property' : omitnull property`.

`omitnull` must appear before the property name and omits the property whenever its value evaluates to null.
For example, here's a silly query that specifies a "nullproperty" property with a constant value
but it will never be included in the result because of the "omitnull".


.. code-block:: jsonql

 {displayname, "nullproperty" : omitnull null}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{displayname, "nullproperty" : omitnull null}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{displayname, "nullproperty" : omitnull null}''')
 [
   {
     "displayname": "abbey aardvaark"
   }, 
   {
     "displayname": "billy billygoat"
   }
 ]



The "forcelist" syntax can be combined with `MAYBE` or `OMITNULL`. For example:


.. code-block:: jsonql

 {displayname, [maybe email]}


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{displayname, [maybe email]}''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{displayname, [maybe email]}''')
 [
   {
     "displayname": "abbey aardvaark", 
     "email": [
       "abbey@aardvaark.com", 
       "abbey_aardvaa@gmail.com"
     ]
   }, 
   {
     "displayname": "billy billygoat", 
     "email": []
   }
 ]



Sub-queries (nested constructs)
-------------------------------

The value of a property or array item can be another query instead of an :ref:`expression`. These sub-query can construct objects or arrays (:token:`constructobject` or a :token:`constructarray`) -- :token:`constructvalue` queries are not allowed as sub-queries.

If the sub-query doesn't have a :ref:`filter` associated with it, the sub-query will be  evaluated in the context of the parent object. For example:


If the sub-query's filter has references to the outer query (via :ref:`labels`) the filter will be joined with the outer query and it will be evaluated using the rows from the resulting join. For example:


Otherwise, the sub-query will be evaluated independently for each result of the outer query. For example:


Data Types
==========

A jsonQL implementation supports at least the data types defined by JSON and may support additional data types if the underlying datastore supports them.

The JSON data types are: (unicode) strings, (floating point) numbers, booleans (true and false) and null. Limits such max string length or numeric range and precision and semantics such as numeric overflow behavior are not specified by jsonQL, they will be dependent on the underlying datastore and implementation language. Most database support richer basic basic data types, for example integer, floating point and decimal, the implementation is responsible for appropriate promotion. 

The values of JSON data types can be expressed in a query as literals that match the JSON syntax. Datastore-specific data type values can be expressed using datastore-specific query functions which construct or convert its arguments, for example, date functions. 

They will be serialized as pjson. If the data type is compatible with JSON type it may converted (for example, from exact precision decimal type to JSON's floating point number) depending on the fidelity needed. In addition, if a :ref:`NAMEMAP` is specified in the query customize the serialization. 

Implicit type conversion, by default, is conversion is lenient [example] but the underlying datastore might be string. 

.. question: should there by a strict mode so implementation matches underlying store?



null handling
-------------

Unlike sql, null value are treated as distinct values, i.e. "null = null" evaluates to true and "null != null" evaluates to false. Operators and functions generally follow SQL: if one of the operands or arguments is null the result is null. 

footnote: Follow SQL for functions and operators: systems that don't follow these null semantics, generally don't support functions (most NO-SQL) or don't support nulls at all (SPARQL). 
Also, unlike SQL null equality, these semantics is generally intuitive.

Aggregate functions, for example, `count()` ignores null values.  

null < 0 so null go first with order by. 


.. code-block:: jsonql

 [null=null, null!=null, null=0, null='', 1+null, trim(null), null > 0, null < 0]


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='26' cols='60'>
 from vesper import app
 model4 = app.createStore(
 '''[
    {
      "id": "1", 
      "value": null
    }, 
    {
      "id": "2", 
      "value": ""
    }, 
    {
      "id": "3", 
      "value": true
    }, 
    {
      "notvalue": "a", 
      "id": "4"
    }
  ]''')


 model4.query(
   '''[null=null, null!=null, null=0, null='', 1+null, trim(null), null > 0, null < 0]''')

  </textarea></div>


.. code-block:: python

 >>> from vesper import app
 >>> model4 = app.createStore(
 ... '''[
 ...   {
 ...     "id": "1", 
 ...     "value": null
 ...   }, 
 ...   {
 ...     "id": "2", 
 ...     "value": ""
 ...   }, 
 ...   {
 ...     "id": "3", 
 ...     "value": true
 ...   }, 
 ...   {
 ...     "notvalue": "a", 
 ...     "id": "4"
 ...   }
 ... ]''')

 >>> model4.query(
 ... '''[null=null, null!=null, null=0, null='', 1+null, trim(null), null > 0, null < 0]''')
 [
   [
     true, 
     false, 
     false, 
     false, 
     null, 
     null, 
     false, 
     true
   ]
 ]



pseudo-value types
------------------

matches value in the list not the list itself. The data-store may support data types that is serialized as a JSON array, the semantics will not apply. [Example]

order may not be preserved.

Objects without (public) unique identifiers can be treated as value types; 
they may not be queried. Note the implementation may store these as object and even provide (for example, forUpdate).



Multiple values and lists
-------------------------
* list construction -- multiple values are represented as lists

Note that the actually semantics of inserting pjson depends on the data store it is being inserted into. For example, 
does inserted a property that already exists on an object might add a new value or replace the current one.


.. code-block:: jsonql

 { id, a_list }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model3.query(
   '''{ id, a_list }''')

  </textarea></div>


.. code-block:: python

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



object references and anonymous objects
---------------------------------------

If an object is anonymous it will be expanded, otherwise an object reference object will be output. This behavior can be overridden using the `DEPTH` directive, which will force object references to be expanded, even if objects are duplicated. 

When a top-level (not embeddd) object is added to a data store without an id it is assigned an autogenerated id (cf. pjson docs). Embedded objects without ids are private and can not be referenced. [what about references amongst themselves?] Filters will not match embedded objects unless referenced through a property. [this implies no need to generate a join -- but what if the property can have a reference to both public and private -- need to double filtering?]


Filtering (the WHERE clause)
==============================

The `where` clause select which objects should appear in the result set. 

In addition, if the construct clause references a property whose 
values are filtered, only those filters will be included in the result.


In other words, results are grouped by the object id. 

value = 1 and value = 2
value in (1, 2)

* property references in construct
* matching lists 
* matching datatypes


all or nothing queries
----------------------



Object References and Joins
===========================

labels
------

You can create a reference to an object creating object labels, which look this this syntax: `?identifier`. 

By declaring the variable 

Once an objected labels, you can create joins by referencing that label in an expression.

This is example, value of the contains property will be any object that


.. code-block:: jsonql

 { ?post 
      *,
      'comments' : { * where parent = ?post}
      where type = 'post'
      }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='8' cols='60'>
 model1.query(
   '''{ ?post 
      *,
      'comments' : { * where parent = ?post}
      where type = 'post'
      }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''
 ...     { ?post 
 ...     *,
 ...     'comments' : { * where parent = ?post}
 ...     where type = 'post'
 ...     }
 ... ''')
 null



filter sets
--------------

When a filter expression is surrounded by braces (`{` and `}`) the filter is applied 
separately from the rest of the expression, and is evaluated as an object reference
to the object that met that criteria. These object references have the same semantics 
as label references. The object references can optionally be labeled and are typically 
used to create joins.

Note that a filter expression like `{id = ?foo}` is logically equivalent to labeling the group `?foo`.


.. code-block:: jsonql

 { * 
  where type = 'comment' and parent = { type = 'post'} 
  }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='6' cols='60'>
 model1.query(
   '''{ * 
  where type = 'comment' and parent = { type = 'post'} 
  }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''
 ... { * 
 ... where type = 'comment' and parent = { type = 'post'} 
 ... }
 ... ''')
 null



joins
------





`maybe` expressions (outer joins)
---------------------------------

The "MAYBE" operator indicates that the expression it modifies is an optional part of the filter set. 
MAYBE can modify property references and join conditions; it is an error to modify any other expression.
When "maybe" modifies a property reference it indicates that the existence of a property not required. When "maybe" modifies a join condition (an expression that joins two filter sets together) if the condition does not match any objects, any references to the missing objects' id or properties will replaced with nulls (this is know as an "outer join"). 


For example, object don't

#property reference in filter prop = maybe ?label and ?label.type = 'type'

#can also appear in the construction: { maybe foo}



.. code-block:: jsonql

 {
  prop1, maybe prop2
  }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='6' cols='60'>
 model1.query(
   '''{
  prop1, maybe prop2
  }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''
 ... {
 ... prop1, maybe prop2
 ... }
 ... ''')
 null



uncorrelated references (cross joins)
-------------------------------------


the follow() function (recursive joins)
---------------------------------------


.. code-block:: jsonql

 { ?post 
      *,
      'comments' : {?comment * where ?comment in rfollow(?post, parent, true)}
      where type = 'post'
      }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='8' cols='60'>
 model1.query(
   '''{ ?post 
      *,
      'comments' : {?comment * where ?comment in rfollow(?post, parent, true)}
      where type = 'post'
      }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''
 ...     { ?post 
 ...     *,
 ...     'comments' : {?comment * where ?comment in rfollow(?post, parent, true)}
 ...     where type = 'post'
 ...     }
 ... ''')
 [
   {
     "id": "post1", 
     "type": "post", 
     "contents": "a post", 
     "comments": [
       {
         "parent": "@post1", 
         "type": "comment", 
         "id": "comment1", 
         "contents": "a comment", 
         "author": "@user:2"
       }, 
       {
         "parent": "@comment1", 
         "type": "comment", 
         "id": "comment2", 
         "contents": "a reply", 
         "author": "@user:1"
       }
     ], 
     "author": "@user:1"
   }
 ]


Expressions
===========

Expressions can be evaluated in two contexts: when they appear inside the where clause and when they appear inside the construction
WHAT ABOUT: order by, group by ?

If an expression contains a property reference whose value a list and the expression doesn't contain any :ref:`aggregate functions', the expression will be evaluated for each item in that list, resulting in a list. If the expression contains more than one property reference, the expression will be evaluated on each tuple obtained from a cartesian product of the list values, using an order based on the depth-first appearance of the property references.

Operator Precedence
-------------------

Follows SQL, from highest to lowest. 

( )
.
unary + / unary - (right)
* / %
+ -
< <= > >= = !=
in 
not
maybe
and
or

Operators with equal precedence are evaluated from left-to-right, except for the unary operator, which evaluate from right-to-left.

Type coercion
-------------

Built-in functions
------------------


Sorting the results: ORDER BY 
=============================



Groupby and aggregate Functions
===============================

If a "group by" clause is not specified, the aggregate function will be apply

Built-in aggregate functions
----------------------------

count, min, max, sum, avg follow standard SQL semantics with regard to null handling, 
*total* follow the semantics sqllite's *total*, described here: http://www.sqlite.org/lang_aggfunc.html



output modifiers
================

MERGEALL
--------

DEPTH
-----

DEPTH may result in duplicate objects being constructed if there are multiple reference to the same object, including circular references [hmmm... better not choose a arbitrary to number to expand all like DEPTH 1000].
Objects no properties are not serialized as objects, they will remain an object reference.

Note: expand a particular object, use ... or use DEPTH in a nested construct.

LIMIT and OFFSET
----------------

LIMIT and OFFSET are applied to the final resultset, after any GROUP BY and ORDER BY operations, but before the MERGEALL operation.


Bind variables
==============


NAMEMAP
========

The value of a NAMEMAP declaration matches pjson's namemap and is used both when parsing the query and when serializing the resultset. 

The namemap applies to the construct pattern it appears in and in any nested constructs. 
If a nested construct has a NAMEMAP described, the effective namemap is the merger of this namemap with the effective parent namemap, as specified for pjson.


.. raw:: html

    <style>
    .example-plaintext { position:absolute; z-index: 2; background-color: lightgray;}
    .close-example-plaintext { float:right; 
      padding-right: 3px;     
      font-size: .83em;
      line-height: 0.7em;
      vertical-align: baseline;
    }
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
      $('.example-plaintext ~ .highlight-python pre').prepend("<span class='toolbar'>Copy Code</span");
      $('.toolbar').click(function() {
        $(this).parents('.highlight-python').prev('.example-plaintext:last')
          .slideDown('fast').find('textarea').focus();
      });
      $('.close-example-plaintext').click(function() { 
            $(this).parents('.example-plaintext').slideUp('fast').find('textarea').blur(); 
      });
    });
    </script>   

..  colophon: this doc was generated by "python tests/jsonqlDocTest.py --printdoc > doc/source/spec.rst"

