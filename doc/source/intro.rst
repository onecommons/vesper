.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Overview
================================


Supported Storage Backends
--------------------------

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
