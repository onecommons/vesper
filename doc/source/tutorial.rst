.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.


Json Query Language Tutorial
================================

.. toctree::
   :maxdepth: 1

   self
   jsonql
   spec
   grammar

.. contents::

Intro 
-----

This tutorial will walk you through a series of interactive examples of JQL simple enough that you can follow along in your Python interpreter if you so desire. For those of you familar with SQL, we'll also show equivalent SQL examples side-by-side.

First we'll create a simple JSON store with some sample JSON. For readability, we'll use native Python dictionaries and lists instead of strings of JSON.   

 >>> import raccoon
 >>> datastore = raccoon.createStore([
 ... {
 ...   "id" : "user:1",
 ...   "username" : "snoopy",
 ...   "favorites" : [ { 'name' : '',  }, 
 ...   "postaladdress" : [ {
 ...          "street" : "123 1st ave",
 ...          "city" : "",
 ...          "postalcode" : 10001       
 ...    },
 ...    {
 ...              "street" : "123 2nd ave",
 ...              "postalcode" : 10001 
 ...    }
 ...   ],
 ... },
 ... {
 ...   "id" : "user:2",
 ...   "username" : "snoopy",
 ... },
 ... {
 ...   "id" : "project:1", 
 ...    "name" : "project 1",       
 ...    "contributors" : ["user:1", "user:2"]
 ... },
 ... {
 ...   "id" : "project:2", 
 ...    "name" : "project 2",
 ...    "contributors" : ["user:1"]
 ... } 
 ... ])


Four top-level objects, two users and two projects.  
Many-to-many to One user we have two postal addresses and another we have any at all. 

The :doc:`vesper.pjson` module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default :func:`raccoon.createStore` will use a simple in-memory store.

To illustrate let's create SQL schema that will data mapping. One to many 

.. raw:: html

   <table width='100%' align='center'><tr valign='top'><td>

========== ============== 
table *user*              
------------------------- 
id         username       
========== ============== 
1      foo woo            
2      bar sloooo         
========== ============== 

.. raw:: html

   </td><td>

========== ==============
table *project*
-------------------------
id         name 
========== ==============
project:1  project 1
project:2  project 2
========== ==============

.. raw:: html

   </td></tr><tr valign='top'><td>

========== ==============
table *contributors*
-------------------------
user_id    project_id
========== ==============
user:1     project:1 
user:2     project:1
user:2     project:1
========== ==============

.. raw:: html

   </td><td>
   
======= =======  ==========
table *user_address*
---------------------------
user_id street   postalcode
======= =======  ==========
======= =======  ==========

.. raw:: html

   </td></td><table>

Now we can start querying the database. 

select all objects in database
------------------------------

Let's start with query that retrieves all records from the store: 

 >>> from pprint import pprint
 >>> pprint(datastore.query('''
 ... { * }
 ... ''',))
 [{},{}]


..
    select id as user_id, username, null as street, null postalcode,  
      from (select * from users) U
    union
    select U.user_id id, null as username, street, postalcode
    from (select * from user_address)       
    union

This is roughly equivalent to the "SELECT * FROM table" in SQL except of course this just retrieves rows from one table, not the whole database. This points to one conceptual difference before JQL and SQL:  JQL has no notion of tables: queries apply to all objects in the database.

select particular of properties from the database
-------------------------------------------------

 >>> pprint(datastore.query('''
 ... { foo, bar }
 ... ''')) 

This is equivalent to the SQL statement 

  SELECT foo, bar FROM project

Note that the objects that don't have foo and bar properties are not selected by the query. We can select against 

"SELECT foo FROM table
 UNION
 SELECT foo FROM table".

This is because the above query is shorthand for this query:

.. rubric:: explicitly named properties

What?

 >>> pprint(datastore.query('''
 ... { "foo" : foo,
 ...   "fob" : foo,
 ...  "bar" : foo + "blah", 
 ... }
 ... ''')) 

Including the `foo` and `bar` properties names in the where clause only selects where the property exists. 
We could give the propery different names just as can "SELECT foo AS fob FROM table" in SQL.

* lists and objects
* id and anonymous objects
* filter/where clause
* joins, objects and variables
* bind variables
* functions 
* group by 
* recursion
* LIMIT, OFFSET and DEPTH
* identifiers: names, qnames and URIs
* outer joins
  where( foo = '2' or b = 1)
* where foo not in {bar = 2}
   * foo not in (select id from X where bar = 2)
   * join( filter(foo), joincond(join(filter(eq(bar,2)) ), join='a') )
* use case: merging name { 'name' : username or projectname } 

..
    #save for advanced example, with a user case that make sense
    dynamically name properties
    
     >>> pprint(datastore.query('''
     ... { foo : "foo"
     ... }
     ... ''')) 
