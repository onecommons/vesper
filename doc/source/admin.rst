.. :copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Quick Start
=========== 

Download and installation
-------------------------

You can find Vesper on Github at https://github.com/aszs/vesper or download the latest code as eggs, zips or tarballs at
http://onecommons.org/vesper/dist/.

Vesper requires Python 2.4 or later. To install using easy_install, pass it an URL to the egg, for example ``easy_install http://onecommons.org/vesper/dist/vesper-LATEST-py2.6.egg``. If you are installing from the source directory run either ``python setup.py install`` or run ``python setup.py develop`` if you don't want to reinstall it everytime its source is updated.

If you've downloaded the source you can verify it working properly by running the unit tests: ``cd tests && python __init__.py``

Running the Administration Server
---------------------------------

Once installed, you should be able to start the vesper admin server with `vesper-admin`.  If not, it's possible
that easy_install didn't put the script on your path.  Where these scripts are placed is platform
dependent, but the most common locations are:

============= =====================================
Platform       Path
============= =====================================
OS X           /usr/local/bin
Debian/Ubuntu  /usr/bin or /usr/local/bin
Redhat/Fedora  /usr/bin
Windows        c:\\Python2.6\\Scripts
============= =====================================

But first, you'll need to specify a datastore for it to open.  If you downloaded the vesper.tgz file,
examples are in the top level under 'examples'.  If you installed via easy_install or from an egg,
the files will be in your site-packages inside the vesper egg directory.  (If you need help finding
this directory, try: ``"import vesper; print vesper.__file__"``)

From the examples directory, use the following command to open a sample datastore::

    vesper-admin -s file://sample.mjson

On your terminal, you should see something like::

    Using FileStore at sample.mjson
    Starting HTTP on port 8000...

You should also be able to see the vesper admin server on port 8000.

Making Queries
--------------

For starters, enter the following query in the query field and click 'Go'::

  { * } 

This will show all objects in the sample datastore.  (There should be XXX)

One way to think of the query syntax is specifying a `template` of the JSON that you'd
like to have matched.  In this case, it's matching against a JSON object with anything inside - in other words all objects.

If you'd like to narrow down the objects to only those that have a certain attribute, use a query like::

 { "created":* }

which will match any top-level object (XXX) with a 'created' attribute.

For more information on making queries, see the query guide XXX

Using different data stores
---------------------------

As you saw earlier, `vesper-admin` requires an URL that specifies which datastore it opens.  This URL
is of the form `protocol://location` and indicates both the type and location of the datastore.  The
most common types are:

========= ====================================
protocol  type
========= ====================================
file      JSON or YAML text file
bdb       Berkeley DB
mem       In-memory store (not persistent)
========= ====================================

How the location portion of the URL is interpreted depends on the protocol, but for the file and bdb
stores it's just a path to a file.  If a path to a nonexistent datastore is specified, a new one will be created.

The memory store doesn't require an argument, it simply creates a private, in-memory store that can't
be shared with other processes (so there's really no point in trying to give it a name).

For more information on the different datastores, see XXX.