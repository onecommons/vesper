#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import unittest
import random
import time
import os
import multiprocessing
from urllib2 import urlopen
from urllib import urlencode

from vesper.backports import json
from vesper import app
from vesper.data import replication
from vesper.web import route

import logging
logging.basicConfig()

# uncomment or use --mq cmd line option to test against an existing stomp message queue
USE_EXISTING_MQ = None #"test-queue:61613"

def startMorbidQueue(port):
    print "starting morbid queue on port %d" % port
    options = {
        'config':None,
        'port':port,
        'interface':'',
        'auth':'',
        'restq':'',
        'verbose':True
    }
    stomp_factory = morbid.get_stomp_factory(cfg=options)
    reactor.listenTCP(options['port'], stomp_factory, interface=options['interface'])
    reactor.run()

def startCoilMQ(port):
    print "starting coilmq queue on port %d" % port
    server = ThreadedStompServer(('127.0.0.1', port), 
        queue_manager = QueueManager(store=MemoryQueue()),
        topic_manager = TopicManager() )
    server.serve_forever()

try:
    from coilmq.server.socketserver import ThreadedStompServer
    from coilmq.topic import TopicManager
    from coilmq.queue import QueueManager
    from coilmq.store.memory import MemoryQueue
    startQueue = startCoilMQ
except ImportError:
    import morbid
    from twisted.internet import reactor
    startQueue = startMorbidQueue

def invokeAPI(name, data, where=None, port=8000):
    "make a vesper api call"
    url = "http://localhost:%d/api/%s" % (port, name)

    if isinstance(data, dict):
        data = json.dumps(data)

    post = {
        "data":data
    }
    if where:
        post['where'] = where

    tmp = urlopen(url, data=urlencode(post)).read()
    return json.loads(tmp)

def startVesperInstance(trunk_id, nodeId, port, queueHost, queuePort, channel):
    try:
        import coverage, sys, signal, atexit
        coverage.process_startup()        
        
        def safeterminate(num, frame):
            #coverage registers an atexit function
            #so have atexit functions called when terminating            
            atexit._run_exitfuncs() #for some reason sys.exit isn't calling this
            sys.exit()
        
        signal.signal(signal.SIGTERM, safeterminate)
    except ImportError:
        pass
    
    print "creating vesper instance:%s (%s:%d)" % (nodeId, queueHost, port)
    conf = {
        'storage_url':"mem://",
        'save_history':True,
        'trunk_id': trunk_id,
        'branch_id':nodeId,
        'replication_hosts':[(queueHost, queuePort)],
        'replication_channel':channel
        
    }
    # assume remote queue implements message ack
    if startQueue is startMorbidQueue:
        #morbid doesn't support stomp ack
        autoAck=True
    else:
        autoAck=False
    
    rep = replication.get_replicator(nodeId, conf['replication_channel'], hosts=conf['replication_hosts'], autoAck=autoAck)
    conf['changeset_hook'] = rep.replication_hook
    
    @app.Action
    def startReplication(kw, retVal):
        # print "startReplication callback!"
        rep.start(kw.__server__)
        
    conf['actions'] = {
        'http-request':route.gensequence,
        'load-model':[startReplication]    
    }
    
    app.createApp(baseapp='miniserver.py',model_uri = 'test:', port=port, **conf).run()
    # blocks forever
    

class BasicReplicationTest(unittest.TestCase):
    
    def setUp(self):
        if USE_EXISTING_MQ:
            (mq_host, mq_port) = USE_EXISTING_MQ.split(':')
            mq_port = int(mq_port)
        else:
            mq_host = "localhost"
            mq_port = random.randrange(5000,9999)
            self.morbidProc = multiprocessing.Process(target=startQueue, args=(mq_port,))
            self.morbidProc.start()        
        
        self.replicationTopic = "UNITTEST" + str(random.randrange(100,999))
        self.rhizomeA_port = random.randrange(5000,9999)
        self.rhizomeB_port = random.randrange(5000,9999)
        
        self.rhizomeA   = multiprocessing.Process(target=startVesperInstance, args=("AA", "AA", self.rhizomeA_port, mq_host, mq_port, self.replicationTopic))
        self.rhizomeA.start()
        self.rhizomeB   = multiprocessing.Process(target=startVesperInstance, args=("AA", "BB", self.rhizomeB_port, mq_host, mq_port, self.replicationTopic))
        self.rhizomeB.start()
        time.sleep(1) # XXX
        
    def tearDown(self):
        self.rhizomeA.terminate()
        self.rhizomeB.terminate()
        if hasattr(self, 'morbidProc'):
            self.morbidProc.terminate()
    
    def testSingleMessage(self):
        "testing single-message replication"
        # post an add to rhizomeA
        sample = {"id":"@1234", "foo":"bar"}
        r = invokeAPI("add", sample, port=self.rhizomeA_port)
        
        time.sleep(1) # XXX
        # do a query on rhizomeB
        r2 = invokeAPI("query", "{*}", port=self.rhizomeB_port)
        self.assertEquals([sample], r2['results'])
        
    def testMessageSequence(self):
        "testing a simple sequence of messages"
        samples = [
            ('add', {"id":"123", "foo":"bar"}, None),
            ('add', {"id":"456", "value":"four hundred fifty six"}, None),
            ('update', {"foo":"baz"}, 'id="123"'),
            ('add', {"id":"789", "foo":"bar", "color":"green"}, None),
        ]
        expected = [{u'foo': u'baz', u'id': u'@123'}, {u'color': u'green', u'foo': u'bar', u'id': u'@789'}, {u'id': u'@456', u'value': u'four hundred fifty six'}]
        
        # post an add to rhizomeA
        for (action, data, where) in samples:
            r = invokeAPI(action, data, where=where, port=self.rhizomeA_port)

        time.sleep(1) # XXX
        # do a query on rhizomeB
        r2 = invokeAPI("query", "{*}", port=self.rhizomeB_port)
        self.assertEquals(expected, r2['results']) # XXX is this order predictable?        

if __name__ == '__main__':
    if sys.argv.count('--mq'):
        arg = sys.argv.index('--mq')
        USE_EXISTING_MQ = sys.argv[arg+1]
        del sys.argv[arg:arg+2]        
        startQueue = None
    
    unittest.main()
