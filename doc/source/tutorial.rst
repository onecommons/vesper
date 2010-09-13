


jsonQL Tutorial
================================

Intro 
-----

This tutorial will walk you through a series of interactive examples of JQL simple enough that you can follow along in your Python interpreter if you so desire. For those of you familar with SQL, we'll also show equivalent SQL examples side-by-side.

First we'll create a simple JSON store with some sample JSON. For readability, we'll use native Python dictionaries and lists instead of strings of JSON.   




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




Four top-level objects, two users and two projects.  
Many-to-many to One user we have two postal addresses and another we have any at all. 

The :doc:`pjson` module does the serialization from JSON to an internal representation that can be saved in a variety of backends ranging from a JSON text file to SQL database, RDF datastores and simple Memcache or BerkeleyDb. By default :func:`raccoon.createStore` will use a simple in-memory store.

To illustrate let's create SQL schema that will data mapping. One to many 

.. raw:: html

   <table width='100%' align='center'><tr valign='top'><td>

=== ================ 
table *users*              
--------------------
id  displayname       
=== ================ 
1   abbey aardvaark
2   billy billlygoat    
=== ================ 

.. raw:: html

   </td><td width=20></td><td>

======= =========================
table *user_emails*
---------------------------------
user_id email
======= =========================
1       abbey@aardvaark.com
1       abbey_aardvaak@gmail.com
======= =========================

.. raw:: html

   </td></tr><tr valign='top'><td>

=== ====== ========= ============
table *posts*
---------------------------------
id  author contents  title
=== ====== ========= ============
1   1      "a post"  "first post"
=== ====== ========= ============

.. raw:: html

   </td><td width=20></td><td>

=== ====== ==================  ====  ======
table *comments*
-------------------------------------------
id  author contents            post  parent
=== ====== ==================  ====  ======
1   1      "a comment"         1     null
1   2      "a reply"           null  1
3   1      "different parent"  null  4
=== ====== ==================  ====  ======

.. raw:: html

   </td></td><table>

Now we can start querying the database. 

select all objects in database
------------------------------

Let's start with query that retrieves all records from the store: 



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



This is roughly equivalent to the "SELECT * FROM table" in SQL except of course this just retrieves rows from one table, not the whole database. This points to one conceptual difference before JQL and SQL:  JQL has no notion of tables: queries apply to all objects in the database.

select particular of properties from the database
-------------------------------------------------


.. code-block:: jsonql

 { foo, bar }


.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='4' cols='60'>
 model1.query(
   '''{ foo, bar }''')

  </textarea></div>


.. code-block:: python

 >>> model1.query(
 ... '''{ foo, bar }''')
 []


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

..  colophon: this doc was generated by "python tests/jsonqlTutorialTest.py --printdoc > doc/source/tutorial.rst"

