.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Intro to Vesper
================================

.. contents::

What is Vesper? 
---------------

Vesper is data-persistence framework that enables you to query and update data as `JSON`_, independent of the database that is actually storing the data. With Vesper, you can switch between a simple text file, a SQL database, or some exotic NOSQL or RDF data store without having to change your application code. And unlike other approaches to object persistence, such as ORMs (Object-Relational Mappers like Ruby-on-Rail's ActiveRecord), Vesper doesn't trade database independence for a codebase dependent on a particular implementation language or framework -- it's just JSON. 

Vesper can accelerate application development by letting you make arbitrary changes to your data at runtime without re-defining schemas or changing class definitions. And its JSON-based query language removes the "impendence mismatch" between native application data structures and SQL without giving up the power and expressiveness provided by a full-featured query language.

Vesper's data model supports explicit metadata designed to enable JSON to be used as data interchange format not only locally but also for decentralized, Internet-scale sharing of public data. Vesper uses this metadata to implement advanced data management functionality such as full revisioning of data, transaction coordination, and asynchronous replication without placing any special requirements on the underlying data store. 

How it works
------------

The core concept behind Vesper is a mapping of the `JSON`_ data format to an abstract intermediate representation which is designed to be compatible with a variety of types of data stores, including the relational table model of SQL and RDF's set model. Access to backend data stores are provided by drivers that expose an API that conforms to this mapping.

Application primarily in two ways: through the :doc:`pjson`, a set of conventions for writing JSON that can be used to update a supported datastore; and through `JSONql`, a query language for accessing data in a data store as JSON.
  
How to use
----------

There are three ways you can use Vesper:

 * as stand-alone HTTP endpoint that you treat as a database server.
 * as a Python library you embed in your application framework of choice. 
 * as an application server you can build your application on top of.


Current Status
--------------

Currently at the proof-of-concept stage with core subset of functionality robust enough for use on small scale applications.

Supported Backends
~~~~~~~~~~~~~~~~~~

Here is the status of data store backends under development:

=============================    =================================================
back-end                         status
=============================    =================================================
JSON or YAML text file           Default
BerkeleyDB                       Recommended for production use
SQL (map to arbitrary schema)    under development
SQL (fixed schema)               supported via RDF backends
RDF                              support for Redland, RDFLib, and 4Suite RDF APIs
Tokyo Cabinet                    Experimental
Memcache                         Experimental
Google AppEngine                 under development
Federated (multiple backends)    planned
=============================    =================================================

Architecture
------------

:mod:`vesper.pjson`
  translates json to internal tuple representation
:mod:`vesper.query`
  executes JSONql queries against model 
:mod:`vesper.data.DataStore`
  high-level query and CRUD interface
:mod:`vesper.data.base`
  base data access APIs 
vesper.data.store.*
  a collection of backend datastore drivers that implement :mod:`vesper.app`
  provides configuration, generic request, and transaction services
:mod:`vesper.web`
  wsgi middleware translates HTTP requests into vesper requests
:mod:`vesper.web.baseapp`
  wsgi app that provides an query and update HTTP endpoint and a Javascript library for using it.

.. _JSON: http://json.org/