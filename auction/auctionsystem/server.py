from auctionsystem.udp.server import UDPServer
from auctionsystem.tcp.server import TCPServer
from auctionsystem.protocol import MESSAGE, REASON, PROTOCOL, AUCTION_CONSTS

import asyncio
import pickle
import logging
import sys


class AuctionServer:
    def __init__(self, recover=False):

        self.logger = logging.getLogger('Logger')
        self.logger.addHandler(logging.FileHandler('server_Log.log'))
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.setLevel('DEBUG')

        self.loop = asyncio.get_event_loop()

        # File path for registration table file
        self.registration_file = 'registration_file.pickle'

        # Load the registration_table from file if it exists, else create it
        try:
            if recover:
                self.registration_table = pickle.load(open(self.registration_file, "rb"))
        except (OSError, IOError) as e:
            self.registration_table = dict()
            pickle.dump(self.registration_table, open(self.registration_file, "wb"))

        # File path for offers file
        self.offers_file = 'offers_file.pickle'
        # Load the offers from file if it exists, else create it
        try:
            if recover:
                self.offers = pickle.load(open(self.offers_file, "rb"))
        except (OSError, IOError) as e:
            self.offers = dict()
            pickle.dump(self.offers, open(self.offers_file, "wb"))

        # If not set to recover mode, ignore previous data saved in files
        if not recover:
            self.registration_table = dict()
            self.offers = dict()

        # Keep track of item numbers to ensure they're all unique
        self.next_item_number = 0
        if recover:
            if len(self.offers) > 0:
                # Set the item number to the largest item number in the recovered offers file + 1
                self.next_item_number = sorted([int(item_num) for item_num in self.offers.keys()])[-1] + 1

        # Setup sockets
        self.udp_server = None
        try:
            self.udp_server = UDPServer(self.loop, self.handle_receive, logger=self.logger)
        except OSError as err:
            self.logger.error('Server could not start the UDP server: Error {}'.format(err))
            sys.exit()

        self.tcp_servers = dict()

        if recover and self.offers:
            # Handle all previously created offers that were never finished
            # Reset each field for each offer in the dict as if we're receiving new offers
            # Signal the clients to change TCP connections to a new socket using a new new_item command
            # Clients need to handle the fact it's a new_item command with an item_num they're already bidding on
            # They should all resend their last bid so we can re-establish the new highest_bid[_by]
            # Clients should also cancel their previous connection if they haven't already
            for item_num, offer in self.offers.items():
                self.offers[item_num]['highest_bid'] = self.offers[item_num]['min']
                self.offers[item_num]['highest_bid_by'] = ''
                self.offers[item_num]['highest_bid_addr'] = None
                # Inform all clients that a new item is offered at the auction
                self.sendall_new_item(self.offers[item_num])

        # Run the event loop
        try:
            self.loop.run_forever()
            self.loop.close()
        except KeyboardInterrupt:
            self.loop.close()
            sys.exit()

    def __del__(self):
        if self.udp_server:
            self.udp_server.close_socket()

    def handle_receive(self, data, addr=None):
        # TODO: Handle the case where data[0] is damaged or None

        # Parse the data
        data = data.decode().split(PROTOCOL.DELIMITER)

        command = data[0]
        # TODO: Get rid of this
        if command == 'TEST':
            return
        req_num = data[1]

        if req_num is None:
            pass  # TODO something bad if request number is not valid

        if command == MESSAGE.REGISTER.value:
            self.rcv_register(req_num, name=data[2], ip_addr=data[3], port_num=data[4])
        elif command == MESSAGE.DEREGISTER.value:
            self.rcv_deregister(req_num, name=data[2], ip_addr=data[3], port_num=addr[1])
        elif command == MESSAGE.OFFER.value:
            self.rcv_offer(req_num, name=data[2], ip_addr=data[3], port_num=addr[1], desc=data[4], min=int(data[5]))
        elif command == MESSAGE.BID.value:
            self.rcv_bid(req_num, item_num=data[2], amount=data[3], name=data[4], addr=addr)
        else:
            pass #TODO: Something went super wrong, this wasn't a valid command message :O

    def rcv_register(self, req_num, name, ip_addr, port_num):
        # Is this a valid registration attempt?
        validity = REASON.VALID

        # The client's name is assumed to be unique, it should not already be there
        if name in self.registration_table:
            validity = REASON.ALREADY_REGISTERED

        if validity is REASON.VALID:
            #  Update the registration table with the new entry
            registration_data = {'req_num': req_num, 'ip_addr': ip_addr,
                                 'port_num': port_num}
            self.registration_table[name] = registration_data

            #  Serialize and save the registration_data to file
            with open(self.registration_file, 'wb') as file:
                pickle.dump(self.registration_table, file)

            #  Acknowledge the registration to the client
            self.send_registered(req_num, name, ip_addr, port_num)

        else:
            # Respond with why the client can't register
            self.send_unregistered(req_num, validity.val, ip_addr, port_num)

    def send_registered(self, req_num, name, ip_addr, port_num):
        #  Send UDP message to confirm registration to the client
        self.send_udp_message(req_num, name, ip_addr, port_num,
                              client_address=(ip_addr, int(port_num)),
                              message=MESSAGE.REGISTERED)

    def send_unregistered(self, req_num, reason, ip_addr, port_num):
        #  Send UDP message with req_unm and reason for unregistrating client
        self.send_udp_message(req_num, reason,
                              client_address=(ip_addr, int(port_num)),
                              message=MESSAGE.UNREGISTERED)

    def rcv_deregister(self, req_num, name, ip_addr, port_num):
        #  Is this a valid deregistration attempt?
        validity = REASON.VALID

        #  The client's name is assumed to already be in the table, else can't remove
        if name not in self.registration_table:
            validity = REASON.NOT_REGISTERED

        #  Is client offering an item for auction? Then they can't deregister until their item's auction is done
        if [offer for key, offer in self.offers.items() if name == offer['offered_by']]:
            validity = REASON.ITEM_OFFERED

        #  Is client active in a bid (ie. have the highest bid on an item)
        if [offer for key, offer in self.offers.items() if name == offer['highest_bid_by']]:
            validity = REASON.ACTIVE_BID

        #  TODO: what if ip_address or DEREGISTER is damaged?
        bad_ip = False
        if bad_ip:
            validity = REASON.BAD_IP

        if validity is REASON.VALID:
            #  Update the registration table by removing the entry
            del self.registration_table[name]

            #  Serialize and update the registration_data to file
            with open(self.registration_file, 'wb') as file:
                pickle.dump(self.registration_table, file)

            #  Acknowledge the deregistration to the client
            self.send_deregister_confirm(req_num, ip_addr, port_num)

        else:
            # Respond with why the client can't deregister
            self.send_deregister_denied(req_num, validity.val, ip_addr, port_num)

    def send_deregister_confirm(self, req_num, ip_addr, port_num):
        #  Send UDP message to confirm deregistration to the client
        self.send_udp_message(req_num, client_address=(ip_addr, int(port_num)), message=MESSAGE.DEREGISTER_CONFIRM)

    def send_deregister_denied(self, req_num, reason, ip_addr, port_num):
        #  Send UDP message with req_num and reason for not deregistering client
        self.send_udp_message(req_num, reason,
                              client_address=(ip_addr, int(port_num)),
                              message=MESSAGE.DEREGISTER_DENIED)

    def rcv_offer(self, req_num, name, ip_addr, port_num, desc, min):
        # Is this a valid offer attempt?
        validity = REASON.VALID

        # The client needs to be registered to make an offer
        if name not in self.registration_table:
            validity = REASON.NOT_REGISTERED

        # The client is only allowed to make 3 simultaneous offers
        if len([offer for key, offer in self.offers.items() if name == offer['offered_by']]) \
                >= AUCTION_CONSTS.OFFER_LIMIT:
            validity = REASON.OFFER_LIMIT

        # TODO: what if ip_address or data is damaged?
        bad_ip = False
        if bad_ip:
            validity = REASON.BAD_IP

        if validity is REASON.VALID:
            # Update the offers dictionary with the new offer
            item_num = str(self.next_item_number)
            offer = {'item_num': item_num, 'offered_by': name, 'desc': desc, 'min': min,
                     'highest_bid': str(int(min) - 1), 'highest_bid_by': '', 'highest_bid_addr': None}
            self.offers[item_num] = offer
            self.next_item_number += 1

            # Acknowledge the offer to the client
            self.send_offer_confirm(req_num, offer, ip_addr, port_num)

            # Inform all clients that a new item is offered at the auction
            self.sendall_new_item(offer)
        else:
            # Respond with why the client can't offer the item
            self.send_offer_denied(req_num, validity.val, ip_addr, port_num)

    def send_offer_confirm(self, req_num, offer, ip_addr, port_num):
        #  Send UDP message to confirm offer to the client
        self.send_udp_message(req_num, offer['item_num'], offer['desc'], offer['min'],
                              client_address=(ip_addr, int(port_num)),
                              message=MESSAGE.OFFER_CONFIRM)

    def send_offer_denied(self, req_num, reason, ip_addr, port_num):
        #  Send UDP message to deny offer to the client and explain why
        self.send_udp_message(req_num, reason, client_address=(ip_addr, int(port_num)), message=MESSAGE.OFFER_DENIED)

    def sendall_new_item(self, offer):
        #  Create new TCP socket to handle bidding for this item ; send to all clients
        new_item_server = TCPServer(self.loop, self.handle_receive, logger=self.logger)

        # For each client connected to the server, send a UDP message to inform them of a new item up for bidding
        seller_name = offer['offered_by']
        for client in self.registration_table.values():
            if seller_name in self.registration_table:
                if client['ip_addr'] == self.registration_table[seller_name]['ip_addr'] and \
                        client['port_num'] == self.registration_table[seller_name]['port_num']:
                    continue  # Don't send New Item to seller

            self.send_udp_message(offer['item_num'], offer['desc'], offer['min'], new_item_server.get_port_number(),
                                  client_address=(client['ip_addr'], int(client['port_num'])),
                                  message=MESSAGE.NEW_ITEM)

        # Keep track of the TCP server socket servicing this new auctioned item
        self.tcp_servers[offer['item_num']] = new_item_server

        #  Serialize and save the the updated offers dictionary to file
        with open(self.offers_file, 'wb') as file:
            pickle.dump(self.offers, file)

        # Start a coroutine that starts a 5 minute timer after which this offer should no longer accept bids
        self.loop.create_task(self.bidding_counter(item_num=offer['item_num']))

    async def bidding_counter(self, item_num):
        # Start the bidding counter and handle the events at the end of the bidding period
        bidding_period = 60*5  # in seconds
        await asyncio.sleep(bidding_period)

        # Get the seller's address and prepare to send them a message whether or not the item was sold
        offer = self.offers[item_num]
        seller = self.registration_table[offer['offered_by']]
        seller_address = (seller['ip_addr'], int(seller['port_num']))

        # Get the winning bidder's name if there is one, along with his address and the highest bid
        winner_name = offer['highest_bid_by']
        winner_addr = offer['highest_bid_addr']
        bid_amount = offer['highest_bid']

        # Send the winning message if there was a winner
        if winner_name:
            data_to_send = self.make_data_to_send(MESSAGE.WIN.value, item_num, winner_name,
                                                  winner_addr[0], winner_addr[1], bid_amount)
            self.tcp_servers[item_num].conn[winner_addr].send(data_to_send)

        # Signal that the bidding is over to all the other clients
        for connection in self.tcp_servers[item_num].conn.values():
            # Don't signal biding-over to winner
            if connection.addr == winner_addr:
                continue
            data_to_send = self.make_data_to_send(MESSAGE.BID_OVER.value, item_num, bid_amount)
            connection.send(data_to_send)

        # At this point all TCP connections are assumed closed, so inform seller through UDP
        if winner_name:
            self.send_udp_message(item_num, winner_name, winner_addr[0], winner_addr[1], bid_amount,
                                  client_address=seller_address, message=MESSAGE.SOLD_TO)
        else:
            # Inform the seller that no one bought their auctioned item using UDP
            self.send_udp_message(item_num, REASON.NO_VALID_BIDS.val,
                                  client_address=seller_address, message=MESSAGE.NOT_SOLD)

        # Close the TCP server for this item and remove the offer since bidding is over
        del self.tcp_servers[item_num]
        del self.offers[item_num]
        #  Serialize and save the the updated offers dictionary to file
        with open(self.offers_file, 'wb') as file:
            pickle.dump(self.offers, file)

    def rcv_bid(self, req_num, item_num, amount, name, addr):
        # If the bid is the highest one yet for the item, then sendall_highest_bid
        offer = self.offers[item_num]
        if not offer:
            #  TODO: Handle case where we receive a bid for an item that's not offered
            pass

        # Check if this bid is the new highest bid for this item
        if int(amount) > int(offer['highest_bid']):
            offer['highest_bid'] = amount
            offer['highest_bid_by'] = name
            offer['highest_bid_addr'] = (addr[0], addr[1])
            # Inform all auction participants for this item that there is a new highest bid
            self.sendall_highest_bid(item_num, amount)

    def sendall_highest_bid(self, item_num, amount):
        #  Use TCP socket to send new highest bid to all clients that are listening to this item's auction
        data_to_send = self.make_data_to_send(MESSAGE.HIGHEST.value, item_num, amount)
        for connection in self.tcp_servers[item_num].conn.values():
            #  Send TCP message to each client
            connection.send(data_to_send)

    @staticmethod
    def make_data_to_send(*argv):
        # Returns encoded binary data formatted as a string with delimiters between each element
        args = []
        for arg in argv:
            args.append(str(arg))
        data = PROTOCOL.DELIMITER.join(args)
        return data.encode()

    def send_udp_message(self, *argv, client_address, message):
        #  Send UDP message to to the client
        data_to_send = self.make_data_to_send(message.value, *argv)
        self.udp_server.send(data_to_send, client_address)
