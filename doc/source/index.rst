.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Introducing Vesper
================================

What is Vesper? 
---------------

Vesper is data-persistence framework that enables you to query and update data as `JSON`_, independent of the database that is actually storing the data. 

Datastore Independence
~~~~~~~~~~~~~~~~~~~~~~
With Vesper, you can switch between a simple text file, a SQL database, or exotic NOSQL or RDF data stores without having to change your application code. And unlike other approaches to object persistence, such as ORMs (Object-Relational Mappers like Ruby-on-Rail's ActiveRecord), Vesper doesn't trade database independence for a codebase dependent on a particular implementation language or framework -- it's just JSON. 

Rapid Development
~~~~~~~~~~~~~~~~~
Vesper can accelerate application development by letting you make arbitrary changes to your data at runtime without re-defining schemas or changing class definitions. And its JSON-based query language removes the "impendence mismatch" between native application data structures and SQL without giving up the power and expressiveness provided by a full-featured query language.

Advanced data management
~~~~~~~~~~~~~~~~~~~~~~~~

Vesper's data model supports explicit metadata designed to enable JSON to be used as data interchange format not only locally but also for decentralized, Internet-scale sharing of public data. Vesper uses this metadata to implement advanced data management functionality such as full revisioning of data, transaction coordination, and asynchronous replication without placing any special requirements on the underlying data store. 

How it works
------------

The core concept behind Vesper is a mapping of the `JSON`_ data format to an abstract intermediate representation which is designed to be compatible with a wide variety of types of data stores, including the relational table model of SQL and RDF's set model. Access to backend data stores are provided by drivers that expose an API that conforms to this mapping.

Application primarily in two ways: through the :doc:`pjson`, a set of conventions for writing JSON that can be used to update a supported datastore; and through :doc:`jsonql`, a query language for accessing data in a data store as JSON.
  
How to use
----------

There are three ways you can use Vesper:

 * as stand-alone HTTP endpoint that you treat as a database server.
 * as a Python library you embed in your application framework of choice. 
 * as an application server you can build your application on top of.

Current Status
--------------

Currently at the proof-of-concept stage with core subset of functionality robust enough for use on small scale applications.

Documentation Contents
----------------------

.. toctree::
   :maxdepth: 2

   self
   admin
   intro
   jsonqltoc
   pjson
   datastore
   vesper.app
   vesper.web
   configvars

* :ref:`modindex`
* :ref:`genindex`

.. _JSON: http://json.org/