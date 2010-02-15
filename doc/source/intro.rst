Intro to Vesper
================================

.. contents::

Intro 
-----

Vesper is data-persistence framework that enables you to query and update data as JSON, independent of the database. With Vesper application code doesn't need to change whether the backend is a SQL database, a simple text file, or an exotic store like NOSQL or RDF. Unlike other approaches to object persistence, such as ORMs (Object-Relational Mapping like Ruby-on-Rail's ActiveRecord), Vesper doesn't tie your data to any particular implementation language or framework -- its just JSON. 

Vesper enables. Vesper also enables to create `context explicit` data

Vesper elements
-----------------
Vesper consists of the following components
 
 * `pjson`
 * `JSONQL`
 * `vesper` implementation transaction coordination, version history, replication and offline support

It is designed to work with a wide variety of databases and backends: from SQL database to NOSQL key-value stores like.
Embedded databases like BerkeleyDB, even a plain text file of JSON or YAML, 

json text file to highly-scalable to cloud computing Google's AppEngine, RDF datastores. And because you are working directly with web-friendly JSON, you can spend much less time developing code for data-development becomes much easier with ad-hoc schemas

How to Use
----------

There are three ways you can use Vesper:

 * as stand-alone HTTP endpoint that you treat as a database server.
 * as a Python library you embed in your application framework of choice. 
 * as an application server you can build your application on top of.

Current Status
--------------

Currently proof-of-concept stage with subset of functionality production-ready.
