"""Server for multithreaded (asynchronous) chat application in distributed server supported by Raft Algorithm."""

from socket import *
from threading import Thread
import sys
import json
import random
from threading import Timer
import numpy as np
import os
import time


#
#
#   msg: {'REQ_VOTE'}
#        {'REQ_VOTE_REPLY'}
#        {'ClientRequest'}
#        {'AppendEntry'}
#        {'AppendEntryConfirm'}
#   Log status:         # Uncommit Commited Applied
#
#   AppendEntries RPC: HeartBeat
#   # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit
#
#   RPC Reply: CurrentTerm, Success
#
#
#

class Server:
    def __init__(self, server_id, CONFIG):

        self.server_id = server_id
        self.leader_id = None
        json.dump(CONFIG, open('config.json', 'w'))
        self.server_port = CONFIG['server_port']
        self.clients_con = []
        self.addresses = {}
        self.HOST = 'localhost'
        self.BUFSIZ = 1024

        self.server = socket(AF_INET, SOCK_STREAM)

        self.server.bind((self.HOST, self.server_port[self.server_id]['port']))
        self.server.listen(8)
        log = {'Content': '', 'term': 0, 'index': 0}
        self.log = [log]

        self.listener = socket(AF_INET, SOCK_DGRAM)
        self.listener.bind((self.HOST, self.server_port[self.server_id]['server_port']))

        self.CommitIndex = 0
        self.LastApplied = 0
        self.nextIndices = {}
        self.loggedIndices = {}

        self.current_term = 0
        self.timeout = 5
        self.heartbeat_timeout = 1
        self.role = 'follower'
        self.election_timeout = random.uniform(self.timeout, 1.2 * self.timeout)

        # become candidate after timeout
        self.votes = {}
        self.vote_log = {}
        self.heartbeat_timer = None

        print('Election Timeout is: %ss' % self.election_timeout)

        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

        Thread(target=self.start())
        Thread(target=self.rec_msg())
        print('server running at ip: %s, port: %s' % (self.HOST, self.PORT))

    # handleIncommingMessage
    def handleIncommingMessage(self, msg):
        # handle incomming messages
        # Message types:
        # messages from servers
        # 1. requestVote RPC
        msg_type = msg['Command']
        if msg_type == 'REQ_VOTE':
            self.handleRequestVote(msg)
        # 2. requestVoteReply RPC
        elif msg_type == 'REQ_VOTE_REPLY':
            self.handleRequestVoteReply(msg)
        # 3. deal with clients
        elif msg_type == 'ClientRequest':
            self.handelClientRequest(msg)
        # 4. append entry
        elif msg_type == 'AppendEntry':
            self.CommitEntry(msg)
        # 5. Confirm append entry
        elif msg_type == 'AppendEntryConfirm':
            self.handleAppendEntryReply(msg)

    def start(self):
        print("Waiting for connection...")
        self.new_thread = Thread(target=self.accept_incoming_connections)
        self.new_thread.start()

    # start election
    def start_election(self):
        """
                start the election process
        """
        print('start election')
        self.role = 'candidate'
        self.leader_id = None
        self.resetElectionTimeout()
        self.current_term += 1
        self.votes[self.current_term] = self.server_id
        self.vote_log[self.current_term] = [self.server_id]

        print('become candidate for term {}'.format(self.current_term))
        # handle the case where only one server is left
        if not self.isLeader() and self.enoughForLeader():
            self.becomeLeader()
            return
        # send RequestVote to all other servers
        # (index & term of last log entry)
        self.requestVote()

    def resetElectionTimeout(self):
        """
        reset election timeout
        """
        if self.election_timer:
            self.election_timer.cancel()
        # need to restart election if the election failed
        # print('reset ElectionTimeout')
        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

    def rec_msg(self):
        print('rec msg')
        while True:
            try:
                msg, address = self.listener.recvfrom(4096)
                msg = json.loads(msg.decode())
                self.handleIncommingMessage(msg)
            except KeyboardInterrupt:
                # print('KeyboardInterrupt')
                server_id = self.server_id
                CONFIG = json.load(open("config.json"))
                CONFIG['server_on'].remove(server_id)
                json.dump(CONFIG, open('config.json', 'w'))
                os._exit(0)
                # os.system('python3 state_ini.py 5')

    # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, server_id, Command
    def handelClientRequest(self, msg):
        # term = msg['current_term']
        # if term < self.current_term:
        #     pass
        #     # self.clientRequestReply(msg, False)
        # serverId = msg['server_id']
        # self.nextIndices[serverId] = msg['CommitIndex']
        # self.log.append(msg['Entries'])
        # msg = {'Command': 'ClientRequest', 'Content': content, 'term'}

        del msg['Command']
        self.log.append(msg)
        print('handle client request')
        self.CommitIndex += 1
        self.LastApplied += 1
        self.broadcast_client(msg['Content'])
        self.sendHeartbeat()

    # def clientRequestReply(self, msg, answer):
    #     # answer_msg = {'Command':}
    #     # self.sendMessage(msg['server_id'], )
    #     pass

    # TODO add self.log
    def requestVote(self):
        # broadcast the request Vote message to all other datacenters
        message = {'Command': 'REQ_VOTE', 'ServerId': self.server_id, 'current_term': self.current_term,
                   'log_len': len(self.log)}
        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']
        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                self.sendMessage(server_id, message)

        # delay
        # Timer(CONFIG['messageDelay'], sendMsg).start()

    # new_add
    def handleRequestVote(self, msg):
        """
        Handle incoming requestVote message
        :type candidate_id: str
        :type candidate_term: int
        :type candidate_log_term: int
        :type candidate_log_index: int
        """
        candidate_term = msg['current_term']
        candidate_id = msg['ServerId']

        if candidate_term < self.current_term:
            self.requestVoteReply(candidate_id, False)
            return

        self.current_term = max(candidate_term, self.current_term)
        grant_vote = False

        if candidate_id not in self.vote_log:
            self.stepDown()
            self.role = 'follower'
            if self.current_term not in self.vote_log:
                self.vote_log[self.current_term] = [candidate_id]
            else:
                self.vote_log[self.current_term].append(candidate_id)

            print('voted for DC-{} in term {}'.format(candidate_id, self.current_term))
            grant_vote = True
        if msg['log_len'] < len(self.log):
            grant_vote = False
        if candidate_term in self.votes:
            if self.votes[candidate_term] != candidate_id:
                grant_vote = False
        self.requestVoteReply(candidate_id, grant_vote)

    def handleRequestVoteReply(self, msg):
        """
        handle the reply from requestVote RPC
        :type follower_id: str
        :type follower_term: int
        :type vote_granted: bool
        """

        follower_id = msg['server_id']
        follower_term = msg['current_term']
        vote_granted = msg['Decision']

        if vote_granted:
            self.vote_log[self.current_term].append(follower_id)
            print('get another vote in term {}, votes got: {}'.format(self.current_term,
                                                                      self.vote_log[self.current_term]))
            if not self.isLeader() and self.enoughForLeader():
                self.becomeLeader()
        else:
            if follower_term > self.current_term:
                self.current_term = follower_term
                self.stepDown()

    def stepDown(self, new_leader=None):
        print('update itself to term {}'.format(self.current_term))
        # if candidate or leader, step down and acknowledge the new leader
        if self.isLeader():
            # if the datacenter was leader
            self.heartbeat_timer.cancel()
        if new_leader != self.leader_id:
            print('leader become {}'.format(new_leader))
        self.leader_id = new_leader
        # need to restart election if the election failed
        self.resetElectionTimeout()
        # convert to follower, not sure what's needed yet
        self.role = 'follower'
        self.vote_log[self.current_term] = []

    def becomeLeader(self):
        """
        do things to be done as a leader
        """
        print('become leader for term {}'.format(self.current_term))
        #self.broadcast_client("SERVERINFO:After election, server %s becomes leader"%self.server_id)
        text = 'SERVERINFO:After election, server %s becomes leader'%self.server_id
        msg = {'Content': text, 'term': self.current_term, 'index': len(self.log)}
        for log in self.log:
            if log['Content'] == text:
                msg = {'Content': 'SERVERINFO:After election, new server %s becomes leader'%self.server_id, 'term': self.current_term, 'index': len(self.log)}
                break
        self.log.append(msg)
        self.CommitIndex += 1
        self.LastApplied += 1
        self.broadcast_client(msg['Content'])
        # no need to wait for heartbeat anymore
        self.election_timer.cancel()

        self.role = 'leader'
        self.leader_id = self.server_id
        CONFIG = json.load(open("config.json"))
        server_on_list = CONFIG['server_on']
        # initialize a record of nextIdx
        self.nextIndices = dict([(server_id, len(self.log) - 1)
                                 for server_id in server_on_list
                                 if server_id != self.server_id])
        print('1nextIndices',self.nextIndices)
        print('send heartbeat')
        self.sendHeartbeat()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def sendHeartbeat(self):
        """
        Send heartbeat message to all pears in the latest configuration
        if the latest is a new configuration that is not committed
        go to the join configuration instead
        :type ignore_last: bool
              - this is used for the broadcast immediately after a new
              config is committed. We need to send not only to sites
              in the newly committed config, but also to the old ones
        """

        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']

        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                print("11",server_id)
                if server_id not in self.nextIndices:
                    self.nextIndices[server_id] = 0
                print("22", server_id)
                print('2nextIndices', self.nextIndices)
                self.sendAppendEntry(server_id)

        self.resetHeartbeatTimeout()


    def resetHeartbeatTimeout(self):
        """z
        reset heartbeat timeout
        """
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, server_id, Command

    def appendEntry(self, target_id, prev_log_idx,
                    prev_log_term, entries):
        msg = {'Command': 'AppendEntry', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
               'PrevLogTerm': prev_log_term, 'Entries': entries, 'LeaderCommit': self.CommitIndex,
               'LeaderId': self.server_id}
        # print('send entry heartbeat %s' % entries)
        print('this the msg',msg)
        self.sendMessage(target_id, msg)

    def sendAppendEntry(self, server_id):
        """
        send an append entry message to the specified datacenter
        :type center_id: str
        """
        max_num = len(self.log)
        if self.nextIndices[server_id] - 1 >= 0:
            print('1serverod',server_id)
            print("2test",self.nextIndices[server_id])
            print("3test11",self.log)
            prevEntry = self.log[self.nextIndices[server_id] - 1]

        else:
            prevEntry = self.log[0]

        # print(self.nextIndices)
        # print(self.CommitIndex)
        # print(prevEntry)

        self.appendEntry(server_id, prevEntry['index'], prevEntry['term'], self.log[self.nextIndices[server_id]])




    # msg = {'Command': 'AppendEntry', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
    #        'PrevLogTerm': prev_log_term, 'Entries': entries, 'CommitIndex': self.CommitIndex}
    def CommitEntry(self, msg):
        # print(msg)
        # print(msg['LeaderId'])
        # print('send commit entry')
        self.leader_id = msg['LeaderId']
        term = msg['current_term']
        self.current_term = term if self.current_term < term else self.current_term
        self.resetElectionTimeout()
        msg['server_id'] = self.server_id
        msg['Command'] = 'AppendEntryConfirm'
        if msg['Entries'] in self.log:
            msg['Confirm'] = 'AlreadyGot'
            self.sendMessage(self.leader_id, msg)
            return
        print('append %s' % msg['Entries'])
        msg['Confirm'] = 'Success'
        self.sendMessage(self.leader_id, msg)
        self.log.append(msg['Entries'])
        self.CommitIndex += 1
        self.broadcast_client(msg['Entries']['Content'])

    # msg = {'Command': 'Append', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
    #        'PrevLogTerm': prev_log_term, 'Entries': entries, 'CommitIndex': self.CommitIndex}
    def handleAppendEntryReply(self, msg):
        """
        handle replies to appendEntry message
        decide if an entry can be committed
        :type follower_id: str
        :type follower_term: int
        :type success: bool
        :type follower_last_index: int
        """
        follower_id = msg['server_id']
        follower_term = msg['current_term']
        success = msg['Confirm']
        # if success=='AlreadyGot':
        #     follower_last_index = msg['PrevLogIndex']
        # else:
        if follower_term > self.current_term:
            self.current_term = follower_term
            self.stepDown()
            return

        # if I am no longer the leader, ignore the message
        if not self.isLeader(): return
        # if the leader is still in it's term
        # adjust nextIndices for follower
        # print(msg)
        # print(self.nextIndices[follower_id])
        # print(follower_last_index)
        # print(self.CommitIndex)

        if self.nextIndices[follower_id] != self.CommitIndex:
            print('self.commitIndex',self.CommitIndex)
            self.nextIndices[follower_id] += 1
            print('update nextIndex of {} to {}'.format(follower_id, self.nextIndices[follower_id]))

        if success == 'AlreadyGot':
            # self.sendAppendEntry(follower_id)
            # print('already got that entry')
            return

        # find out the index most followers have reached
        majority_idx = self.maxQualifiedIndex(self.nextIndices)
        print('the index logged by majority is {0}'.format(majority_idx))
        # commit entries only when at least one entry in current term
        # has reached majority
        if self.log[majority_idx]['term'] != self.current_term:
            print('term no right')
            return
        # if we have something to commit
        # if majority_idx < self.CommitIndex, do nothing
        # old_commit_idx = self.CommitIndex
        # self.CommitIndex = max(self.CommitIndex, majority_idx)
        # TODO
        # list(map(self.commitEntry, self.log[old_commit_idx + 1:majority_idx + 1]))

    def maxQualifiedIndex(self, indices):
        """
        Given a dictionary of datacenters and the max index in their log
        we find of the maximum index that has reached a majority in
        current configuration
        """
        # entry = self.getConfig()
        # the leader keep its own record updated to the newest
        indices[self.server_id] = len(self.log) - 1
        # print('!!!!!', indices)
        # if entry['config'] == 'single':
        #     return sorted([indices[x] for x in entry['data']])[int((len(entry['data'])-1)/2)]
        # maxOld = sorted([indices[x] for x in entry['data'][0]])[int((len(entry['data'][0])-1)/2)]
        # maxNew = sorted([indices[x] for x in entry['data'][1]])[int((len(entry['data'][1])-1)/2)]

        return min(indices.values())

    def enoughForLeader(self):
        """
        Given a list of servers who voted, find out whether it
        is enough to get a majority based on the current config
        :rtype: bool
        """
        CONFIG = json.load(open("config.json"))
        server_on_list = CONFIG['server_on']
        print('enough for leader? %s > %s' % (
            np.unique(np.array(self.vote_log[self.current_term])).shape[0], len(server_on_list) / 2))
        return np.unique(np.array(self.vote_log[self.current_term])).shape[0] > len(server_on_list) / 2

    def isLeader(self):
        """
        determine if the current server is the leader
        """
        return self.server_id == self.leader_id

    # new_add
    def requestVoteReply(self, target_id, grant_vote):
        # send reply to requestVote message
        message = {'Command': 'REQ_VOTE_REPLY', 'server_id': self.server_id, 'current_term': self.current_term,
                   'Decision': grant_vote}
        self.sendMessage(target_id, message)

    # Timer(CONFIG['messageDelay'], sendMsg).start()

    # new_add
    def sendMessage(self, server_id, message):
        """
        send a message to the target server
        should be a UDP packet, without gauranteed delivery
        :type target_meta: e.g. { "port": 12348 }
        :type message: str
        """
        message = json.dumps(message)
        peer_socket = socket(AF_INET, SOCK_DGRAM)
        # print('server_id %s' % server_id)
        # print('leader_id %s' % self.leader_id)
        #########
        port = self.server_port[server_id]['server_port']
        addr = (self.HOST, port)
        peer_socket.sendto(message.encode(), addr)

        # peer_socket.connect(addr)
        # self.all_socket[port].send(message)

    def accept_incoming_connections(self):
        """Sets up handling for incoming clients."""
        while True:
            client, client_address = self.server.accept()
            self.clients_con.append(client)
            print("%s:%s has connected." % client_address)
            client.send(bytes("Welcome! You are at server %s. Type your username and press enter to continue."%self.server_id, "utf8"))
            self.addresses[client] = client_address
            Thread(target=self.handle_client, args=(client,)).start()

    # def rec_client(self, msg):
    #     print('receive client request')
    #     # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit
    #     if len(self.log) > 0:
    #         PrevLogIndex = self.log[-1]['CommitIndex']
    #         PrevLogTerm = self.log[-1]['current_term']
    #     else:
    #         PrevLogIndex = None
    #         PrevLogTerm = None
    #
    #     entry = {'current_term': self.current_term, 'LeaderId': self.leader_id, 'PrevLogIndex': PrevLogIndex,
    #              'PrevLogTerm': PrevLogTerm, 'Entries': msg}
    #     self.log.append(entry)
    #     entry['server_id'] = self.server_id
    #     entry['Command'] = 'ClientRequest'
    #     self.CommitIndex += 1
    #     if self.server_id != self.leader_id:
    #         self.sendMessage(self.leader_id, entry)
    #     else:
    #         self.log.append(msg)

    def rec_client(self, content):
        print('receive client request')
        msg = {'Command': 'ClientRequest', 'Content': content, 'term': self.current_term, 'index': len(self.log)}
        if self.server_id != self.leader_id:
            print(' Transfer to leader')
            self.sendMessage(self.leader_id, msg)
        else:
            print('log record client')
            del msg['Command']
            self.log.append(msg)
            print(self.log)
            self.CommitIndex += 1
            self.LastApplied += 1
            self.broadcast_client(content)
            self.sendHeartbeat()

    def handle_client(self, client):  # Takes client socket as argument.
        """Handles a single client connection."""

        name = client.recv(self.BUFSIZ).decode("utf8")
        welcome = 'Welcome %s! If you want to quit, type {quit} to exit.' % name
        client.send(bytes(welcome, "utf8"))
        msg = "%s has joined the chat!" % name
        for log in self.log:
            time.sleep(0.1)
            client.send(bytes(log['Content'], 'utf8'))

        self.rec_client(msg)

        # self.broadcast(msg, name)
        # self.broadcast_client(msg)
        while True:
            msg = client.recv(self.BUFSIZ)
            if msg != bytes("{quit}", "utf8"):
                msg = msg.decode('utf8')
                # self.broadcast_client(msg)
                # self.broadcast(msg, name + ": ")
                self.rec_client(name + ': ' + msg)

            else:
                # client.send(bytes("{quit}", "utf8"))
                client.close()
                self.clients_con.remove(client)
                # self.broadcast("%s has left the chat." % name)
                msg = "%s has left the chat." % name
                # client.send(bytes("", "utf8") + msg)
                self.rec_client(msg)
                break

    def broadcast(self, msg, name):  # prefix is for name identification.
        """Broadcasts a message to all the servers."""
        message = {'Command': 'Broadcast', 'msg': msg, 'name': name}
        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']
        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                self.sendMessage(server_id, message)

    def broadcast_client(self, msg, prefix=""):
        for sock in self.clients_con:
            sock.send(bytes(prefix + msg, "utf8"))


if __name__ == "__main__":
    CONFIG = json.load(open("config.json"))
    server_on_list = CONFIG['server_on']
    all_server_id = sorted(CONFIG['server_port'].keys())
    for i in all_server_id:
        if i not in server_on_list:
            server_id = i
            break
    try:
        CONFIG['server_on'].append(server_id)
    except:
        print('no more place for another server!')
        sys.exit(1)

    try:
        server = Server(server_id, CONFIG)
        server.start()

    except KeyboardInterrupt:
        # print('KeyboardInterrupt')
        server_id = server_id
        CONFIG = json.load(open("config.json"))
        CONFIG['server_on'].remove(server_id)
        json.dump(CONFIG, open('config.json', 'w'))
        # os.system('python3 state_ini.py 5')