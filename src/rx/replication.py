from rx.python_shim import *
import logging
import stomp

class ChangesetListener(object):
    """
    Receives message notifications from Stomp.
    This is called in a Stomp listener thread, not the main program thread
    """
    def __init__(self, replicator, autoAck=False):
        self.replicator = replicator
        self.autoAck = autoAck
    
    # def on_connecting(self, host_and_port):
    #     print "connecting to %s:%s" % host_and_port
        
    def on_connected(self, headers, body):
        self.replicator.log.debug("connected!")
        
    def on_disconnected(self):
        self.replicator.log.warning("lost connection to server! trying to reconnect")
        
        self.replicator.conn.start()
        self.replicator.conn.connect(wait=True)
        
    def on_message(self, headers, message):
        if len(message) > 0:
            # print 'message: %s' % (message)
            # print '         %s' % str(headers)
            
            message_id = headers['message-id']
            self.replicator.log.debug("Node %s processing message:%s" % (self.replicator.clientid, message_id))
                        
            obj = json.loads(message)
            
            # sanity check to make sure this is a message we need to care about
            if (headers['clientid'] != obj['origin']):
                self.replicator.log.warning("Node %s ignoring message id %s with mismatched origins! headers: %s obj:%s" % (self.replicator.clientid, message_id, headers['clientid'], obj['origin']))
                # XXX do we ack this or not?
            elif (self.replicator.clientid == headers['clientid']):
                self.replicator.log.warning("Node %s ignoring message id %s from myself" % (self.replicator.clientid, message_id))
                # XXX do we ack this or not?
            else:
                try:
                    self.replicator.server.txnSvc.begin()
                    assert not self.replicator.server.domStore.model._currentTxn
                    self.replicator.server.domStore.merge(obj)
                except:
                    self.replicator.server.txnSvc.abort()
                    raise
                else:
                    self.replicator.server.txnSvc.commit()
                    if not self.autoAck:
                        self.replicator.conn.ack({'message-id':message_id})
                
    def on_error(self, headers, message):
        self.replicator.log.error("stomp error: %s" % message)
        
    # def on_receipt(self, headers, message):
    #     print 'receipt: %s' % message

class StompQueueReplicator(object):
    
    log = logging.getLogger("replication")
    
    def __init__(self, clientid, channel, hosts, autoAck=False):
        self.clientid = clientid
        self.channel  = channel
        self.hosts    = hosts
        self.autoAck  = autoAck
        
    def start(self, server):
        """
        Connect to the stomp server and subscribe to the replication topic
        """
        self.server = server
        
        self.log.info("connecting to %s" % str(self.hosts))
        self.conn = stomp.Connection(self.hosts)
        self.conn.set_listener('changes', ChangesetListener(self, self.autoAck))
        self.conn.start()
        
        subscription_name = "%s-%s" % (self.clientid, self.channel)
        subscribe_headers = {
            "activemq.subscriptionName":subscription_name,
            "selector":"clientid <> '%s'" % self.clientid  # XXX perf implications here?
        }
        # self.log.debug("subscribing to topic:" + self.channel)
        
        self.conn.connect(headers={'client-id':self.clientid})
        self.conn.subscribe(destination='/topic/%s' % self.channel, ack='client', headers=subscribe_headers)
        
    def stop(self):
        self.conn.disconnect()        
        
    def replication_hook(self, changeset):
        HEADERS = {
            'persistent':'true',
            'clientid':self.clientid
        }        
        self.log.debug("posting changeset %s to channel %s" % (changeset.revision, self.channel))
        data = json.dumps(changeset) #,sort_keys=True, indent=4)
        try:
            self.conn.send(data, destination='/topic/%s' % self.channel, headers=HEADERS)
        except Exception, e:
            self.log.error("exception posting changeset", e)

def get_replicator(clientid, channel, host=None, port=61613, hosts=None, autoAck=False):
    """
    Connect to a Stomp message queue for replication
    
    - clientid is the replication id of this node
    - channel is the stomp topic to listen to
    - host, port specifies a single stomp server to connect to
    - hosts specifies a list of (host,port) tuples to use for failover
      e.g. hosts=[('tokyo-vm', 61613), ('mqtest-vm', 61613)]
    - autoAck specifies whether a message acknowlegement should be delayed until after
      the replication message is successfully processed.  Not all queues (morbidQ)
      support this
    
    Returns a replicator object
    """
    if not hosts:
        hosts=[(host,port)]
    
    obj = StompQueueReplicator(clientid, channel, hosts, autoAck)
    return obj
