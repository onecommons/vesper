#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import logging
import threading, Queue, time
from datetime import datetime

import stomp

from vesper.backports import *

class ChangesetListener(object):
    """
    Receives message notifications from Stomp.
    This is called in a Stomp listener thread, not the main program thread
    """
    def __init__(self, replicator, storeName='default', sendAck=True):
        self.replicator = replicator
        self.sendAck = sendAck
        self.storeName = storeName
    
    def on_connecting(self, host_and_port):
        self.replicator.log.debug("connecting to %s:%s" % host_and_port)
        self.replicator.connected_to = host_and_port
        
    def on_connected(self, headers, body):
        self.replicator.log.debug("connected!")
        self.replicator.connected = True
        
    def on_disconnected(self, headers, body):
        # headers and body are (almost?) always null; but stomp.py explodes on python 2.4 without them
        self.replicator.log.warning("lost connection to server! trying to reconnect")
        self.replicator.connected = False
        
        self.replicator.conn.start()
        self.replicator.conn.connect(wait=True)
        
    def on_message(self, headers, message):
        if len(message) > 0:
            # print 'message: %s' % (message)
            # print '         %s' % str(headers)
            if not self.replicator.first_ts:
                self.replicator.first_ts = datetime.now()
            self.replicator.last_ts = datetime.now()
            
            message_id = headers['message-id']
            self.replicator.log.debug("Node %s processing message:%s"
                                      % (self.replicator.clientid, message_id))
                        
            obj = json.loads(message)
            
            # sanity check to make sure this is a message we need to care about
            if (headers['clientid'] != obj['origin']):
                self.replicator.log.warning("Node %s ignoring message id %s with "
                "mismatched origins! headers: %s obj:%s" % (self.replicator.clientid,
                                  message_id, headers['clientid'], obj['origin']))
                self.replicator.msg_recv_err += 1
                # XXX do we ack this or not?
            elif (self.replicator.clientid == headers['clientid']):
                self.replicator.log.warning(
                    "Node %s ignoring message id %s from myself"
                                % (self.replicator.clientid, message_id))
                self.replicator.msg_recv_err += 1
                # XXX do we ack this or not?
            else:
                
                try:
                    dataStore = self.replicator.server.stores[self.storeName]
                    assert not dataStore.model._currentTxn
                    dataStore.merge(obj)
                    self.replicator.msg_recv += 1
                    if self.sendAck:
                        self.replicator.conn.ack({'message-id':message_id})
                except Exception, e:
                    self.replicator.log.exception("error storing replicated changeset")
                    self.replicator.msg_recv_err += 1
                    raise e
                
    def on_error(self, headers, message):
        self.replicator.log.error("stomp error: %s" % message)
        
    # def on_receipt(self, headers, message):
    #     print 'receipt: %s' % message

class StompQueueReplicator(object):
    
    log = logging.getLogger("replication")
    
    def __init__(self, clientid, channel, hosts, storeName='default', sendAck=True):
        self.clientid = clientid
        self.channel  = channel
        self.hosts    = hosts
        self.storeName = storeName
        self.sendAck  = sendAck
        self.connected = False
        self.connected_to = None
        
        self.changeset_queue = Queue.Queue()
        
        # statistics
        self.msg_sent = 0
        self.msg_recv = 0
        self.msg_sent_err = 0
        self.msg_recv_err = 0
        self.first_ts = None
        self.last_ts  = None
        
    def start(self, server):
        """
        Connect to the stomp server and subscribe to the replication topic
        """
        self.server = server
        
        self.log.info("connecting to %s" % str(self.hosts))
        self.conn = stomp.Connection(self.hosts)
        self.conn.set_listener('changes', ChangesetListener(self, self.storeName, self.sendAck))
        self.conn.start()
        
        subscription_name = "%s-%s" % (self.clientid, self.channel)
        subscribe_headers = {
            "activemq.subscriptionName":subscription_name,
            "selector":"clientid <> '%s'" % self.clientid  # XXX perf implications here?
        }
        self.log.debug("subscribing to topic:" + self.channel)
        
        self.conn.connect(headers={'client-id':self.clientid})
        self.conn.subscribe(destination='/topic/%s' % self.channel, 
                                ack='client', headers=subscribe_headers)
        self.start_sending_thread()
        
    def start_sending_thread(self):
        def worker():
            log = logging.getLogger("replication")
            while True:
                changeset = self.changeset_queue.get()
                if changeset == 'done':
                    break
                    
                while changeset != None:
                    try:
                        self.send_changeset(changeset)
                        changeset = None
                    except Exception:
                        log.error("couldn't post changeset, waiting to retry")
                        time.sleep(1)
                
        
        send_thread = threading.Thread(target=worker)
        send_thread.daemon = True
        send_thread.start()
        
    def stop(self):
        self.changeset_queue.put("done")
        if self.connected:
            self.conn.disconnect()
        
    def replication_hook(self, changeset):
        self.changeset_queue.put(changeset)
        
    def send_changeset(self, changeset):    
        HEADERS = {
            'persistent':'true',
            'clientid':self.clientid
        }
        self.log.debug("posting changeset %s to channel %s" % (changeset.revision, self.channel))
        data = json.dumps(changeset) #,sort_keys=True, indent=4)
        try:
            self.conn.send(data, destination='/topic/%s' % self.channel, headers=HEADERS)
            self.msg_sent += 1
        except Exception, e:
            self.log.exception("exception posting changeset")
            self.msg_sent_err += 1
            raise e
    
    def stats(self):
        if self.connected:
            state = self.connected_to
        else:
            state = "Disconnected"
        if self.first_ts and self.last_ts:
            elapsed = self.last_ts - self.first_ts
        else:
            elapsed = None
        return ('Replication', [
            ('connected', state),
            ('clientid', self.clientid),
            ('channel', self.channel),
            ('messages sent', self.msg_sent),
            ('messages received', self.msg_recv),
            ('send errors', self.msg_sent_err),
            ('receive errors', self.msg_recv_err),
            ('last message received', self.last_ts),
            ('elapsed', elapsed),            
            ('send queue size', self.changeset_queue.qsize()),
        ])
        

def get_replicator(clientid, channel, host=None, port=61613, hosts=None, storeName='default', sendAck=True):
    """
    Connect to a Stomp message queue for replication
    
    - clientid is the replication id of this node
    - channel is the stomp topic to listen to
    - host, port specifies a single stomp server to connect to
    - hosts specifies a list of (host,port) tuples to use for failover
      e.g. hosts=[('tokyo-vm', 61613), ('mqtest-vm', 61613)]
    - storeName specifies which data store to update ('default' by default)
    - sendAck Specifies whether a acknowledgment message should sent after the message has been processed successfully.
      Not all Stomp message brokers support this (e.g. MorbidQ doesn't) (default: True) 
    
    Returns a replicator object
    """
    if not hosts:
        hosts=[(host,port)]
    
    obj = StompQueueReplicator(clientid, channel, hosts, storeName, sendAck)
    return obj
