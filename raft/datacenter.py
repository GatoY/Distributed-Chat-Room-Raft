# this script handles the logic of datacenter

import json
import bisect
from threading import Lock
from threading import Timer
import logging
import random
import pickle

CONFIG = json.load(open('config.json'))


class LogEntry(object):
    """
    The log entry
    """
    def __init__(self, term, index=None, command=None):
        if index is None:
            self.term, self.index, self.command = term
        else:
            self.term = term
            self.index = index
            self.command = command

    def __str__(self):
        return '%s,%s,%s' % (self.term, self.index, self.command)

    def getVals(self):
        return self.term, self.index, self.command


class datacenter(object):
    """
    This class handles the protocol logic of datacenters
    """
    def __init__(self, datacenter_id, server):
        """
        we use datacenter_id instead of pid because it is potentially
        deployed on multiple servers, and many have the same pid
        """
        logging.debug('initialized')
        self.datacenter_id = datacenter_id
        # self.datacenters = CONFIG['datacenters']
        # update the datacenters, so that the id and port are all int
        # self.datacenters = dict([(x, y) for x, y in self.datacenters.items()])
        self.total_ticket = CONFIG['total_ticket']

        # get current_term, voted_for, log from the state.p
        filename = 'state'+datacenter_id+'.pkl'
        pkl_file = open(filename, 'rb')
        data = pickle.load(pkl_file)

        self.current_term = data['current_term']
        self.voted_for = data['voted_for']

        self.role = 'follower'

        # keep a list of log entries
        # put a dummy entry in front
        #self.log = [LogEntry(0, 0)]
        self.log = data['log']


        # record the index of the latest comitted entry
        # 0 means the dummy entry is already comitted
        self.commit_idx = -1

        # store the server object to be used for making requests
        self.server = server
        self.leader_id = None

        self.election_timeout = random.uniform(CONFIG['T'], 2*CONFIG['T'])

        # become candidate after timeout
        if self.datacenter_id in self.getAllCenterID():
            self.election_timer = Timer(self.election_timeout, self.startElection)
            self.election_timer.daemon = True
            self.election_timer.start()
        else:
            self.election_timer = None
        logging.debug('started election countdown')

        # used by leader only
        self.heartbeat_timeout = CONFIG['heartbeat_timeout']
        self.heartbeat_timer = None

    def resetHeartbeatTimeout(self):
        """
        reset heartbeat timeout
        """
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

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
        logging.debug('reset election countdown')

    def isLeader(self):
        """
        determine if the current datacenter is the leader
        """
        return self.datacenter_id == self.leader_id

    def getLatest(self):
        """
        get term and index of latest entry in log
        """
        return (self.log[-1].term, self.log[-1].index)

    def startElection(self):
        """
        start the election process
        """
        self.role = 'candidate'
        self.leader_id = None
        self.resetElectionTimeout()
        self.current_term += 1
        self.votes = [self.datacenter_id]
        self.voted_for = self.datacenter_id
        dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
        filename = "./state"+self.datacenter_id+'.pkl'
        fileobj = open(filename, 'wb')
        pickle.dump(dictobj, fileobj)
        fileobj.close()

        logging.debug('become candidate for term {}'.format(self.current_term))

        # handle the case where only one server is left
        if not self.isLeader() and self.enoughForLeader(self.votes):
            self.becomeLeader()

        # send RequestVote to all other servers
        # (index & term of last log entry)
        self.server.requestVote(self.current_term, self.getLatest()[0], \
                                self.getLatest()[1])


    def requestInLog(self, client_id, request_id):
        for entry in self.log:
            if not entry.command: continue
            if 'client_id' in entry.command and \
               client_id == entry.command['client_id'] and \
               'request_id' in entry.command and \
               request_id == entry.command['request_id']:
                return True
        return False

    def handleChange(self, config_msg):
        """
        Handle the request to change config
        :type new_config: Object
              - similar to that "datacenters" field in config.json
        """
        new_config = json.loads(config_msg)
        if self.isLeader():
            # add a joint config into logging
            entry = self.getConfig()

            for center_id in new_config:
                if center_id not in self.loggedIndices and \
                   center_id != self.datacenter_id:
                    # initialize a record of nextIdx
                    self.loggedIndices[center_id] = 0
                    self.nextIndices[center_id] = self.getLatest()[1]+1

            # only go to joint config if the current config is not joint
            # otherwise, ignore the command
            if entry['config'] == 'single':
                self.log.append(LogEntry(self.current_term, len(self.log),
                                         {'config': 'joint',
                                          'data': [entry['data'], new_config]}))
            self.sendHeartbeat()
        else:
            # if there is a current leader, then send the request to
            # the leader
            message = 'CHANGE:%s' % config_msg
            logging.info('forward change request to leader {}'
                         .format(self.leader_id))
            if self.leader_id:
                self.server.sendMessage(self.getMetaByID(self.leader_id),
                                        message)

    def handleBuy(self, client_id, request_id, ticket_count,
                  client_ip, client_port):
        """
        Handle the request to buy ticket,
        :type client_id: str
        :type request_id: int
        :type ticket_count: int
        """
        if self.isLeader():
            # if the request is already in the list, ignore it
            if not self.requestInLog(client_id, request_id):
                self.log.append(LogEntry(self.current_term, len(self.log),
                                         {'client_id': client_id,
                                          'request_id': request_id,
                                          'ticket_count': ticket_count,
                                          'client_ip': client_ip,
                                          'client_port': client_port}))
                dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
                filename = "./state"+self.datacenter_id+'.pkl'
                fileobj = open(filename, 'wb')
                pickle.dump(dictobj, fileobj)
                fileobj.close()
        else:
            # if there is a current leader, then send the request to
            # the leader, otherwise, ignore the request, the client
            # will eventually retry
            # need a way to know who to send the success message to
            message = ('BUY-FORWARD:"{client_id}",{request_id},' +
                       '{ticket_count},"{client_ip}",{client_port}').format(
                                client_id=client_id,
                                request_id=request_id,
                                ticket_count=ticket_count,
                                client_ip=client_ip,
                                client_port=client_port)
            logging.info('forward ticket request to leader {}'
                         .format(self.leader_id))
            if self.leader_id:
                self.server.sendMessage(self.getMetaByID(self.leader_id),
                                        message)

    def getConfig(self, committed=False, ignore_last=False):
        """
        Get the most recent config in log
        :type committed: bool
              - whether we only consider single config that are committed
        :type ignore_last: bool
              - whether we ignore the latest committed entry
        :rtype: a log entry for config
        """
        # go back from the latest entry, find the most recent config entry
        for idx, entry in list(enumerate(self.log))[::-1]:
            if 'config' in entry.command:
                if not committed: break
                if entry.command['config'] == 'joint': break
                if self.commit_idx >= idx and not ignore_last: break
        # print('committed: %s, ignore_last: %s, FETCHED config: %s' % (committed, ignore_last, entry.command))
        return entry.command

    def getAllCenterID(self, committed=True, ignore_last=False):
        """
        Find out the id for all datacenters in the latest log entry
        :type committed: bool
              - whether we only consider single config that are committed
        :rtype: a list of id of all currently running datacenters
        """
        # go back from the latest entry, find the most recent config entry
        entry = self.getConfig(committed, ignore_last=ignore_last)
        if entry['config'] == 'single':
            return entry['data'].keys()
        # remove duplicate entries in the final list
        return list(set(entry['data'][0].keys() + entry['data'][1].keys()))

    def getMetaByID(self, target_id):
        """
        Given an id, find out the meta information about this datacenter
        Note that this is not necessarily the latest config
        This the new config is not committed, we may still need information
        from the joint config
        :type target_id: str
        :rtype: Object containing meta data for the center
        """
        # get the latest committed single config or lastest join config
        entry = self.getConfig(committed=True, ignore_last=True)
        if entry['config'] == 'single':
            if target_id in entry['data']:
                return entry['data'][target_id]
            else:
                return None
        if target_id in entry['data'][0]:
            return entry['data'][0][target_id]
        if target_id in entry['data'][1]:
            return entry['data'][1][target_id]
        return None

    def enoughForLeader(self, votes):
        """
        Given a list of datacenters who voted, find out whether it
        is enough to get a majority based on the current config
        :rtype: bool
        """
        entry = self.getConfig()
        if entry['config'] == 'single':
            validVotes = len(set(entry['data'].keys()) & set(votes))
            return validVotes > len(entry['data']) / 2
        validVotesOld = len(set(entry['data'][0].keys()) & set(votes))
        validVotesNew = len(set(entry['data'][1].keys()) & set(votes))
        return validVotesOld > len(entry['data'][0]) / 2 and \
               validVotesNew > len(entry['data'][1]) / 2

    def maxQualifiedIndex(self, indices):
        """
        Given a dictionary of datacenters and the max index in their log
        we find of the maximum index that has reached a majority in
        current configuration
        """
        entry = self.getConfig()
        # the leader keep its own record updated to the newest
        indices[self.datacenter_id] = len(self.log) - 1
        # print('!!!!!', indices)
        if entry['config'] == 'single':
            return sorted([indices[x] for x in entry['data']])[(len(entry['data'])-1)/2]
        maxOld = sorted([indices[x] for x in entry['data'][0]])[(len(entry['data'][0])-1)/2]
        maxNew = sorted([indices[x] for x in entry['data'][1]])[(len(entry['data'][1])-1)/2]
        return min(maxOld, maxNew)

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

    def becomeLeader(self):
        """
        do things to be done as a leader
        """
        logging.info('become leader for term {}'.format(self.current_term))

        # no need to wait for heartbeat anymore
        self.election_timer.cancel()

        self.role = 'leader'
        self.leader_id = self.datacenter_id
        # keep track of the entries known to be logged in each data center
        # note that when we are in the transition phase
        # we as the leader need to keep track of nodes in
        # the old and the new config
        self.loggedIndices = dict([(center_id, 0)
                                   for center_id in self.getAllCenterID()
                                   if center_id != self.datacenter_id])
        # initialize a record of nextIdx
        self.nextIndices = dict([(center_id, self.getLatest()[1]+1)
                                 for center_id in self.getAllCenterID()
                                 if center_id != self.datacenter_id])

        self.sendHeartbeat()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def stepDown(self, new_leader=None):
        logging.debug('update itself to term {}'.format(self.current_term))
        # if candidate or leader, step down and acknowledge the new leader
        if self.isLeader():
            # if the datacenter was leader
            self.heartbeat_timer.cancel()
        if new_leader != self.leader_id:
            logging.info('leader become {}'.format(new_leader))
        self.leader_id = new_leader
        # need to restart election if the election failed
        self.resetElectionTimeout()
        # convert to follower, not sure what's needed yet
        self.role = 'follower'
        self.voted_for = None
        dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
        filename = "./state"+self.datacenter_id+'.pkl'
        fileobj = open(filename, 'wb')
        pickle.dump(dictobj, fileobj)
        fileobj.close()

    def handleRequestVoteReply(self, follower_id, follower_term, vote_granted):
        """
        handle the reply from requestVote RPC
        :type follower_id: str
        :type follower_term: int
        :type vote_granted: bool
        """
        if vote_granted:
            self.votes.append(follower_id)
            logging.info('get another vote in term {}, votes got: {}'
                          .format(self.current_term, self.votes))

            if not self.isLeader() and self.enoughForLeader(self.votes):
                self.becomeLeader()
        else:
            if follower_term > self.current_term:
                self.current_term = follower_term
                dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
                filename = "./state"+self.datacenter_id+'.pkl'
                fileobj = open(filename, 'wb')
                pickle.dump(dictobj, fileobj)
                fileobj.close()
                self.stepDown()

    def sendAppendEntry(self, center_id):
        """
        send an append entry message to the specified datacenter
        :type center_id: str
        """
        prevEntry = self.log[self.nextIndices[center_id]-1]
        self.server.appendEntry(center_id, self.current_term,
                                prevEntry.index, prevEntry.term,
                                self.log[self.nextIndices[center_id]:],
                                self.commit_idx)

    def handleShow(self):
        """
        display the state of datacenter
        first line: current state
        following lines: committed log
        """
        logging.info(self.total_ticket)
        for entry in self.log:
            logging.info(entry)
        logging.info(self.role)

    def sendHeartbeat(self, ignore_last=False):
        """
        Send heartbeat message to all pears in the latest configuration
        if the latest is a new configuration that is not committed
        go to the join configuration instead
        :type ignore_last: bool
              - this is used for the broadcast immediately after a new
              config is committed. We need to send not only to sites
              in the newly committed config, but also to the old ones
        """
        for center_id in self.getAllCenterID(ignore_last=ignore_last):
            if center_id != self.datacenter_id:
                # send a heartbeat message to datacenter (center_id)
                self.sendAppendEntry(center_id)
        self.resetHeartbeatTimeout()

    def handleAppendEntryReply(self, follower_id, follower_term, success,
                               follower_last_index):
        """
        handle replies to appendEntry message
        decide if an entry can be committed
        :type follower_id: str
        :type follower_term: int
        :type success: bool
        :type follower_last_index: int
        """
        if follower_term > self.current_term:
            self.current_term = follower_term
            dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
            filename = "./state"+self.datacenter_id+'.pkl'
            fileobj = open(filename, 'wb')
            pickle.dump(dictobj, fileobj)
            fileobj.close()
            self.stepDown()
            return
        # if I am no longer the leader, ignore the message
        if not self.isLeader(): return
        # if the leader is still in it's term
        # adjust nextIndices for follower
        if self.nextIndices[follower_id] != follower_last_index + 1:
            self.nextIndices[follower_id] = follower_last_index + 1
            logging.debug('update nextIndex of {} to {}'
                          .format(follower_id, follower_last_index + 1))
        if not success:
            self.sendAppendEntry(follower_id)
            return
        # check if there is any log entry committed
        # to do that, we need to keep tabs on the successfully
        # committed entries
        self.loggedIndices[follower_id] = follower_last_index
        # find out the index most followers have reached
        majority_idx = self.maxQualifiedIndex(self.loggedIndices)
        logging.debug('the index logged by majority is {0}'
                      .format(majority_idx))
        # commit entries only when at least one entry in current term
        # has reached majority
        if self.log[majority_idx].term != self.current_term:
            return
        # if we have something to commit
        # if majority_idx < self.commit_idx, do nothing
        if majority_idx != self.commit_idx:
            logging.info('log committed upto {}'.format(majority_idx))
        old_commit_idx = self.commit_idx
        self.commit_idx = max(self.commit_idx, majority_idx)
        map(self.commitEntry, self.log[old_commit_idx+1:majority_idx+1])

    def commitEntry(self, entry):
        """
        commit a log entry
        :type entry: LogEntry
        """
        logging.info('entry committing! {}'.format(entry))
        # check if the entry is a buy ticket entry
        # if so, and if I am the leader, send a message to client about
        # the operation being successful
        if entry.command and 'ticket_count' in entry.command:
            ticket = entry.command['ticket_count']
            if self.isLeader():
                self.server.sendMessage(
                    {'port': entry.command['client_port']},
                    ('Here is your tickets, remaining tickets %d' % (self.total_ticket - ticket))
                    if self.total_ticket >= ticket else 'Sorry, not enough tickets left')
            if self.total_ticket >= ticket:
                self.total_ticket -= ticket
                logging.info('{0} ticket sold to {1}'.format(
                             ticket, entry.command['client_id']))
        elif entry.command and 'config' in entry.command:
            if entry.command['config'] == 'joint':
                # when the joint command is committed, the leader should
                # add a new config into log entry and broadcast it to all
                # datacenteres
                # for none leander, it doesn't change anything
                if self.isLeader():
                    self.log.append(LogEntry(self.current_term, len(self.log),
                                             {'config': 'single',
                                              'data': entry.command['data'][1]}))
                    # send the updated message to all servers, including
                    # the ones that are in the old configuration
                    self.sendHeartbeat()
            else:
                if self.isLeader():
                    self.sendHeartbeat(ignore_last=True)
                # when a single config is committed, the datacenter should
                # check whether it is in the new config
                # if not, it need to retire itself
                # print('---!!!!', self.getAllCenterID())
                if self.datacenter_id not in self.getAllCenterID():
                    logging.info('retire itself')
                    exit(1)

    def handleAppendEntry(self, leader_id, leader_term, leader_prev_log_idx,
                          leader_prev_log_term, entries, leader_commit_idx):
        """
        handle appendEntry RPC
        accept it if the leader's term is up-to-date
        otherwise reject it
        :type leader_id: str
        :type leader_term: int
        :type leader_prev_log_idx: int
        :type leader_prev_log_term: int
        :type entries: List[LogEntry]
               A heartbeat is a meesage with [] is entries
        :type leader_commit_idx: int
        """
        _, my_prev_log_idx = self.getLatest()
        if self.current_term > leader_term:
            success = False
        else:
            self.current_term = max(self.current_term, leader_term)
            self.stepDown(leader_id)
            if my_prev_log_idx < leader_prev_log_idx \
               or self.log[leader_prev_log_idx].term != leader_prev_log_term:
                logging.debug('log inconsistent with leader at {}'
                              .format(leader_prev_log_idx))
                success = False

            else:
                # remove all entries going after leader's last entry
                if my_prev_log_idx > leader_prev_log_idx:
                    self.log = self.log[:leader_prev_log_idx+1]
                    logging.debug('remove redundent logs after {}'
                                  .format(leader_prev_log_idx))
                # Append any new entries not already in the log
                for entry in entries:
                    logging.debug('adding {} to log'.format(entry))
                    self.log.append(entry)
                # check the leader's committed idx
                if leader_commit_idx > self.commit_idx:
                    old_commit_idx = self.commit_idx
                    self.commit_idx = leader_commit_idx
                    map(self.commitEntry,
                        self.log[old_commit_idx+1:leader_commit_idx+1])
                    logging.debug('comitting upto {}'.format(leader_commit_idx))
                success = True
            dictobj = {'current_term': self.current_term, 'voted_for': self.voted_for, 'log': self.log}
            filename = "./state"+self.datacenter_id+'.pkl'
            fileobj = open(filename, 'wb')
            pickle.dump(dictobj, fileobj)
            fileobj.close()
        # reply along with the lastest log entry
        # so that the leader will know how much to update the
        # nextIndices record
        # if failed, reply index of highest possible match:
        #  leader_prev_log_idx-1
        self.server.appendEntryReply(leader_id, self.current_term, success,
                                     self.getLatest()[1] if success
                                     else leader_prev_log_idx-1)
