from threading import Timer
import random
import time

class test():

    def __init__(self):
        self.timeout = 3
        self.election_timeout = random.uniform(self.timeout, 2 * self.timeout)
        print(self.election_timeout)
        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

        # self.sendHeartbeat()
        while(1):
            print('click')
            time.sleep(5)

    def start_election(self):
        """
        Send heartbeat message to all pears in the latest configuration
        if the latest is a new configuration that is not committed
        go to the join configuration instead
        :type ignore_last: bool
              - this is used for the broadcast immediately after a new
              config is committed. We need to send not only to sites
              in the newly committed config, but also to the old ones
        """

        print('bong')
        self.resetElectionTimeout()

    def resetElectionTimeout(self):
        """z
        reset heartbeat timeout
        """
        print('reset heartbeat')
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

t = test()