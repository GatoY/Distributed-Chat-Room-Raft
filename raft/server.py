# this script handles the message passing of raft
import json
import os
import sys
import logging
from socket import *
import datacenter
from threading import Timer
import pickle

CONFIG = json.load(open('config.json'))


class server(object):
    """
    This class is a virtual server with it's own local storage
    A controller for crashing condition
    A controller for network condition
    And full support for message passing
    """

    def __init__(self, center_id, port):
        self.ip = gethostbyname('')
        self.port = port
        self.center_id = center_id
        logging.info('server running at {:s}:{:4d}'.format(self.ip, self.port))
        try:
            self.listener = socket(AF_INET, SOCK_DGRAM)
            self.listener.bind((self.ip, self.port))
            #self.listener.listen(5) # Max connections
            logging.info('listener start successfully...')
        except Exception as e:
            # socket create fail
            logging.warning("Socket create fail.{0}".format(e))
        self.dc = datacenter.datacenter(self.center_id, self)
        self.waitConnection()

    def sendMessage(self, target_meta, message):
        """
        send a message to the target server
        should be a UDP packet, without gauranteed delivery
        :type target_meta: e.g. { "port": 12348 }
        :type message: str
        """
        if target_meta is None:
            logging.warning('trying to sent to server not in config')
            return
        peer_socket = socket(AF_INET, SOCK_DGRAM)
        host = ''
        port = target_meta["port"]
        addr = (host, port)
        logging.debug("{0} <-- {1}".format(target_meta['port'], message))
        sent = peer_socket.sendto(message, addr)
        # peer_socket.connect(addr)
        #self.all_socket[port].send(message)

    def requestVote(self, current_term, latest_log_term, latest_log_index):
        # broadcast the requestVote message to all other datacenters
        def sendMsg():
            message = ('REQ_VOTE:"{datacenter_id}",' +
                       '{current_term},{latest_log_term},' +
                       '{latest_log_index}\n').format(
                datacenter_id=self.center_id,
                current_term=current_term,
                latest_log_term=latest_log_term,
                latest_log_index=latest_log_index
            )
            for center_id in self.dc.getAllCenterID(committed=False):
                if center_id != self.center_id:
                    self.sendMessage(self.dc.getMetaByID(center_id), message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def requestVoteReply(self, target_id, current_term, grant_vote):
        # send reply to requestVote message
        def sendMsg():
            message = ('REQ_VOTE_REPLY:"{datacenter_id}",' +
                       '{current_term},{grant_vote}\n').format(
                        datacenter_id=self.center_id,
                        current_term=current_term,
                        grant_vote=json.dumps(grant_vote))
            self.sendMessage(self.dc.getMetaByID(target_id), message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def appendEntry(self, target_id, current_term, prev_log_idx,
                    prev_log_term, entries, commit_idx):
        def sendMsg():
            message = ('APPEND:"{datacenter_id}",{current_term},' +
                       '{prev_log_idx},{prev_log_term},{entries},' +
                       '{commit_idx}\n').format(
                           datacenter_id=self.center_id,
                           current_term=current_term,
                           prev_log_idx=prev_log_idx,
                           prev_log_term=prev_log_term,
                           entries=json.dumps([x.getVals() for x in entries]),
                           commit_idx=commit_idx)
            self.sendMessage(self.dc.getMetaByID(target_id), message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def appendEntryReply(self, target_id, current_term, success,
                         follower_last_index):
        def sendMsg():
            message = ('APPEND_REPLY:"{datacenter_id}",{current_term},' +
                       '{success},{follower_last_index}\n').format(
                           datacenter_id=self.center_id,
                           current_term=current_term,
                           success=json.dumps(success),
                           follower_last_index=follower_last_index)
            self.sendMessage(self.dc.getMetaByID(target_id), message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def handleIncommingMessage(self, message_type, content, address):
        # handle incomming messages
        # Message types:
        # messages from servers
        # 1. requestVote RPC
        if message_type == 'REQ_VOTE':
            logging.info("--> {0}. {1}".format(message_type, content))
            self.dc.handleRequestVote(*json.loads('[%s]' % content))
        # 2. requestVoteReply RPC
        elif message_type == 'REQ_VOTE_REPLY':
            logging.info("--> {0}. {1}".format(message_type, content))
            follower_id, follower_term, vote_granted \
                = json.loads('[%s]' % content)
            self.dc.handleRequestVoteReply(*json.loads('[%s]' % content))
        # 3. appendEntry RPC
        elif message_type == 'APPEND':
            logging.debug("--> {0}. {1}".format(message_type, content))
            leader_id, leader_term, leader_prev_log_idx,\
                leader_prev_log_term, entries, leader_commit_idx =\
                json.loads('[%s]' % content)
            self.dc.handleAppendEntry(
                leader_id, leader_term,
                leader_prev_log_idx,
                leader_prev_log_term,
                map(datacenter.LogEntry, entries),
                leader_commit_idx)
        # 4. appendEntryReply RPC
        elif message_type == 'APPEND_REPLY':
            logging.debug("--> {0}. {1}".format(message_type, content))
            self.dc.handleAppendEntryReply(*json.loads('[%s]' % content))
        # messages from clients
        # 1. buy
        elif message_type == 'BUY':
            #test
            # for center_id in self.dc.datacenters:
            #     if center_id != self.center_id:
            #         # target_meta = self.dc.datacenters[center_id]
            #         # port = target_meta["port"]
            #         # self.all_socket[port] = socket(AF_INET, SOCK_STREAM)
            #         # host = ''
            #         # addr = (host, port)
            #         # self.all_socket[port].connect(addr)
            #         self.sendMessage(self.dc.datacenters[center_id], content)
            logging.info("--> {0}. {1}".format(message_type, content))
            self.dc.handleBuy(*tuple(json.loads('[%s]' % content))+address)
        # 1.1. forwarded buy
        elif message_type == 'BUY-FORWARD':
            logging.info("--> {0}. {1}".format(message_type, content))
            self.dc.handleBuy(*json.loads('[%s]' % content))
        # 2. show
        elif message_type == 'SHOW':
            logging.info("--> {0}. {1}".format(message_type, content))
            self.dc.handleShow()
        # 3. change
        elif message_type == 'CHANGE':
            logging.info("--> {0}. {1}".format(message_type, content))
            # the config should be a config json object
            # similar to that "datacenters" field in config.json
            self.dc.handleChange(content)

    def waitConnection(self):
        '''
        This function is used to wait for incomming connections,
        either from peer datacenters or from clients
        Incomming connections from clients are stored for later response
        '''
        num = 0
        while True:
            try:
                # conn, addr = self.listener.accept()
                # logging.debug('Connection from {address} connected!'
                #               .format(address=addr))
                # msg = conn.recv(1024)
                msg, address = self.listener.recvfrom(4096)
                # logging.info("Connection from %s" % str(address))
                for line in msg.split('\n'):
                    if len(line) == 0: continue
                    try:
                        self.handleIncommingMessage(
                            *tuple(line.strip().split(':', 1))+(address, ))
                    except Exception as e:
                        logging.error('Error with incomming message. {0} {1}'
                                      .format(e, line))
                        raise
            except Exception as e:
                logging.error('Error with incomming connection. {0} {1}'
                              .format(e, msg))
                raise

def main():
    logging.info("Start datacenter...")
    datacenter_cfg = CONFIG['datacenters']
    # port = datacenter_cfg[sys.argv[1]]['port']
    Server = server(sys.argv[1], int(sys.argv[2]))

if __name__ == "__main__":
    logging.basicConfig(format='(DC-%s)' % sys.argv[1] +
                               ' %(asctime)s [%(levelname)s]:%(message)s',
                        datefmt='%I:%M:%S',
                        level=logging.INFO)
    main()