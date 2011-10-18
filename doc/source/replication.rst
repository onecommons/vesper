Replication
===========

Introduction
------------

Vesper supports asynchronous replication of data between multiple datastore instances using a message queue to broadcast data changes.

This approach allows for more flexible set of topologies than normally found in database servers (e.g. enabling disconnected nodes and hierarchical topologies) while off-loading reliability and scalability requirements to the message queue broker, of which there are several proven, enterprise-class options available to choose from.

Requirements
------------

`stomp.py <http://code.google.com/p/stomppy/>`_ (``easy_install stomp.py`` if you didn't include the "replication" extra when you installed vesper [e.g.  ``easy_install "vesper[replication]"``])

But note that the current version only works on Python 2.6 or later -- ``easy_install http://stomppy.googlecode.com/files/stomp.py-2.0.2.tar.gz`` for compatibility with older versions of Python.

A `STOMP <http://stomp.codehaus.org/>`_-compatible message queue broker. The following message queue brokers have been tested:
 * ActiveMQ 5.3.0 or greater. Recommended for production use.
 * `coilmq <http://code.google.com/p/coilmq/>`_ (``easy_install coilmq``). (Doesn't support Python 2.4)
 * `MorbidQ <http://www.morbidq.com/>`_ 0.8 or greater (``easy_install Twisted morbid``)

Installing the "tests" extra (e.g. ``easy_install "vesper[tests]"``) will install `coilmq` or `MorbidQ` and `Twisted` if you are running Python 2.4. 

Configure Vesper instances
--------------------------

Each instance requires the following configuration variables to be set:::

    #each instance needs a different branch id
    branchId="0A" 
    #revision history must be activated for replication to work
    saveHistory = True 
    #list of (hostname, port) pairs identifying the message broker
    #(specifying more than one will enable failover)
    replication_hosts=[('localhost', 61613)]
    #channel can be any name 
    replication_channel="VESPER_DATA" 

If you are plan to use MorbidQ as your message broker also add ``send_stomp_ack=False`` to your configuration 
because MorbidQ doesn't support replying with STOMP ack messages.

Run a message broker
--------------------

coilmq
~~~~~~

Coilmq can be started by running `coilmq` from your shell.

ActiveMQ
~~~~~~~~

To enable the STOMP protocol on ActiveMQ, add a connector to its configuration file, e.g.:::

  <transportConnectors>
    <transportConnector name="stomp" uri="stomp://localhost:61613"/>
  </transportConnectors>

See http://activemq.apache.org/stomp.html and http://twiki.cern.ch/twiki/bin/view/EGEE/MsgTutorial for more info.

Testing
-------

You can verify you have the basic components installed correctly by running the replication unit 
tests found in tests/replicationTest.py, for example:::

 cd ./tests
 python replicationTest.py


By default, the tests will launch (and stop) its own instance of a message broker 
(either `coilmq` or `MorbidQ` if you are running Python 2.4). To have the tests use an different message broker, 
use `--mq host:port` as a command line argument, for example:::

 python replicationTest.py --mq test-queue:61613

