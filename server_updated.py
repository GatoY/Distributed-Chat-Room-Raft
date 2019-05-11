"""Server for multithreaded (asynchronous) chat application."""
from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
import sys


class Server:
    def __init__(self, server_id, port=33000):
        self.clients = {}
        self.addresses = {}
        self.HOST = ''
        self.PORT = port
        self.BUFSIZ = 1024


        ###

        self.server_id = server_id
        self.current_term = 0
        self.timeout = 1
        self.role = 'follower'
        self.election_timeout = random.uniform(self.timeout, 2*self.timeout)

        # become candidate after timeout
        if self.role != 'leader':
            self.election_timer = Timer(self.election_timeout, self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()
        else:
            self.election_timer = None


        print('server running at ip: %s, port: %s'%(self.HOST, self.PORT))

    def start(self):
        self.server = socket(AF_INET, SOCK_STREAM)
        self.server.bind(self.HOST, self.PORT)

        self.server.listen(5)
        print("Waiting for connection...")
        self.new_thread= Thread(target=self.accept_incoming_connections)
        self.new_thread.start()
        self.new_thread.join()
        self.server.close()

    # new_add
    def start_election(self):
        """
                start the election process
        """

        self.role = 'candidate'
        # self.leader_id = None
        self.resetElectionTimeout()
        self.current_term += 1
        self.votes = [self.server_id]
        self.voted_for = self.server_id

        # dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for}
        print('become candidate for term {}'.format(self.current_term))

        # handle the case where only one server is left
        # if not self.isLeader() and self.enoughForLeader(self.votes):
        #     self.becomeLeader()

        # send RequestVote to all other servers
        # (index & term of last log entry)
        self.server.requestVote()

    def get_server_id_list(self):
        return list()
        pass

    def rec_msg(self):
        while True:
            try:
                msg, address = self.listener.recvfrom(4096)
                # logging.info("Connection from %s" % str(address))
                #print(msg.split(str.encode('\n')))
                for line in msg.split(str.encode('\n')):
                    if len(line) == 0: continue
                    try:
                        self.handleIncommingMessage(
                            *tuple(line.strip().split(str.encode(':'), 1))+(address, ))
                    except Exception as e:
                        logging.error('Error with incomming message. {0} {1}'
                                      .format(e, line))
                        raise
            except Exception as e:
                logging.error('Error with incomming connection. {0} {1}'
                              .format(e, msg))
                raise
    # new_add
    def handleIncommingMessage(self, message_type, content, address):
        # handle incomming messages
        # Message types:
        # messages from servers
        # 1. requestVote RPC
        message_type = message_type.decode()
        if message_type == 'REQ_VOTE':
            # logging.info("--> {0}. {1}".format(message_type, content))
            self.handleRequestVote(*json.loads(json.dumps(eval(('[%s]' % content.decode())))))
        # 2. requestVoteReply RPC
        elif message_type == 'REQ_VOTE_REPLY':

            logging.info("--> {0}. {1}".format(message_type, content))
            follower_id, follower_term, vote_granted \
                = json.loads(json.dumps(eval(('[%s]' % content.decode()))))
            self.dc.handleRequestVoteReply(*json.loads(json.dumps(eval(('[%s]' % content.decode())))))

    # new_add
    def requestVote(self):
        # broadcast the request Vote message to all other datacenters
        def sendMsg():
            message = ('REQ_VOTE:"{server_id}",' +
                       '{current_term}}\n').format(
                server_id=self.server_id,
                current_term=self.current_term,
            )
            for server_id in self.get_server_id_list():
                if server_id != self.server_id:
                    self.sendMessage(server_id, message)


        # delay
        # Timer(CONFIG['messageDelay'], sendMsg).start()

    # new_add
    def handleRequestVote(self, candidate_id, candidate_term,
                          candidate_log_term, candidate_log_index):
        """
        Handle incoming requestVote message
        :type candidate_id: str
        :type candidate_term: int
        :type candidate_log_term: int
        :type candidate_log_index: int
        """
        if candidate_id not in self.getAllCenterID():
            logging.warning('{} requested vote, but he is not in current config'
                            .format(candidate_id))
            return
        if candidate_term < self.current_term:
            self.server.requestVoteReply(
                candidate_id, self.current_term, False)
            return
        if candidate_term > self.current_term:
            self.current_term = candidate_term
        self.current_term = max(candidate_term, self.current_term)
        grant_vote = (not self.voted_for or self.voted_for == candidate_id) and\
            candidate_log_index >= self.getLatest()[1]
        if grant_vote:
            self.stepDown()
            self.voted_for = candidate_id
            logging.debug('voted for DC-{} in term {}'
                          .format(candidate_id, self.current_term))
        dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
        filename = "./state"+self.datacenter_id+'.pkl'
        fileobj = open(filename, 'wb')
        pickle.dump(dictobj, fileobj)
        fileobj.close()
        self.server.requestVoteReply(
            candidate_id, self.current_term, grant_vote)

    # new_add
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

    # new_add
    def sendMessage(self, server_id, message):
        """
        send a message to the target server
        should be a UDP packet, without gauranteed delivery
        :type target_meta: e.g. { "port": 12348 }
        :type message: str
        """
        peer_socket = socket(AF_INET, SOCK_DGRAM)
        host = '127.0.0.1'
        port = server_port[server_id]
        addr = (host, port)
        sent = peer_socket.sendto(message.encode(), addr)

        # peer_socket.connect(addr)
        #self.all_socket[port].send(message)


    # new_add
    def resetElectionTimeout(self):
        """
        reset election timeout
        """
        if self.election_timer:
            self.election_timer.cancel()
        # need to restart election if the election failed
        self.election_timer = Timer(self.election_timeout, self.startElection)
        self.election_timer.daemon = True
        self.election_timer.start()



    def accept_incoming_connections(self):
        """Sets up handling for incoming clients."""
        while True:
            client, client_address = self.SERVER.accept()
            print("%s:%s has connected." % client_address)
            client.send(bytes("Welcome! Type your username and press enter to continue.", "utf8"))
            self.addresses[client] = client_address
            Thread(target=self.handle_client, args=(client,)).start()


    def handle_client(self, client):  # Takes client socket as argument.
        """Handles a single client connection."""

        name = client.recv(self.BUFSIZ).decode("utf8")
        welcome = 'Welcome %s! If you want to quit, type {quit} to exit.' % name
        client.send(bytes(welcome, "utf8"))
        msg = "%s has joined the chat!" % name
        self.broadcast(bytes(msg, "utf8"))
        self.clients[client] = name

        while True:
            msg = client.recv(self.BUFSIZ)
            if msg != bytes("{quit}", "utf8"):
                self.broadcast(msg, name + ": ")
            else:
                client.send(bytes("{quit}", "utf8"))
                client.close()
                del self.clients[client]
                self.broadcast(bytes("%s has left the chat." % name, "utf8"))
                break


    def broadcast(self, msg, prefix=""):  # prefix is for name identification.
        """Broadcasts a message to all the clients."""

        for sock in self.clients:
            sock.send(bytes(prefix, "utf8") + msg)



if __name__ == "__main__":
    server = Server(sys.args[0])
    server.start()
