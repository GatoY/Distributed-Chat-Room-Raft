from auctionsystem.udp.client import UDPClient
from auctionsystem.tcp.client import TCPClient
from auctionsystem.protocol import MESSAGE, PROTOCOL, REASON, AUCTION_CONSTS
import asyncio

# To generate random name for client
import string
import random

# Just to get fully qualified domain name
import socket


class AuctionClient:
    def __init__(self, name=None, server_address=(socket.getfqdn(), 8888),
                 gui_update_cb=None, gui_timeout_cb=None, loop=None):

        if not gui_update_cb:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

        # Setup callback to GUI
        self.gui_update_cb = gui_update_cb
        self.gui_timeout_cb = gui_timeout_cb
        self.client_name = name
        if not self.client_name:
            self.client_name = ''.join(
                random.choices(string.ascii_letters + string.digits, k=10))  # Random letters and numbers

        # Setup sockets
        self.server_address = server_address
        self.udp_client = UDPClient(self.loop, self.handle_receive, self.server_address)
        self.tcp_clients = dict()

        self.client_name = ''
        self.sent_messages = dict()
        self.bidding_items = dict()
        self.offers = dict()

        self.request_num_counter = 0

        # Run the event loop
        if not self.gui_update_cb:
            self.loop.run_until_complete(self.run())
            self.loop.close()

    def __del__(self):
        if not self.udp_client:
            self.udp_client.close_socket()

    async def run(self):
        # Send registration request to server before doing anything else
        address = self.udp_client.address
        self.send_register(self.client_name, address[0], str(address[1]))
        while True:
            for item_num in list(self.bidding_items.keys()):
                item = self.bidding_items[item_num]
                self.send_bid(item_num, str(int(item['min']) + random.randint(1, 3000)))
                await asyncio.sleep(1)
            await asyncio.sleep(0.1)

    def handle_receive(self, data, addr=None):

        if data is None:
            # Other side has disconnected
            print('Connection has been lost!')
            return

        # Parse the data
        data = data.decode().split(PROTOCOL.DELIMITER)

        command = data[0]
        if command is 'TEST':
            return

        if command == MESSAGE.REGISTERED.value:
            self.rcv_registered(req_num=data[1], name=data[2], ip_addr=data[3], port_num=data[4])
        elif command == MESSAGE.UNREGISTERED.value:
            self.rcv_unregistered(req_num=data[1], reason=data[2])
        elif command == MESSAGE.DEREGISTER_CONFIRM.value:
            self.rcv_dereg_conf(req_num=data[1])
        elif command == MESSAGE.DEREGISTER_DENIED.value:
            self.rcv_dereg_denied(req_num=data[1], reason=data[2])
        elif command == MESSAGE.OFFER_CONFIRM.value:
            self.rcv_offer_conf(req_num=data[1], item_num=data[2], desc=data[3], min_price=data[4])
        elif command == MESSAGE.OFFER_DENIED.value:
            self.rcv_offer_denied(req_num=data[1], reason=data[2])
        elif command == MESSAGE.NEW_ITEM.value:
            self.rcv_new_item(item_num=data[1], desc=data[2], min_price=data[3], port=data[4])
        elif command == MESSAGE.HIGHEST.value:
            self.rcv_highest(item_num=data[1], amount=data[2])
        elif command == MESSAGE.WIN.value:
            self.rcv_win(item_num=data[1], name=data[2], ip_addr=data[3], port_num=data[4], amount=data[5])
        elif command == MESSAGE.BID_OVER.value:
            self.rcv_bid_over(item_num=data[1], amount=data[2])
        elif command == MESSAGE.SOLD_TO.value:
            self.rcv_sold_to(item_num=data[1], name=data[2], ip_addr=data[3], port=data[4], amount=data[5])
        elif command == MESSAGE.NOT_SOLD.value:
            self.rcv_not_sold(item_num=data[1], reason=data[2])
        else:
            pass  # TODO: Something went super wrong, this wasn't a valid command message :O

    def rcv_registered(self, req_num, name, ip_addr, port_num):
        self.confirm_acknowledgement(req_num)

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.REGISTERED)
        else:
            print('received registered')

    def rcv_unregistered(self, req_num, reason):
        # The client could not register with the server
        self.confirm_acknowledgement(req_num)

        reason_str = REASON.get_reason_str(reason)

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.UNREGISTERED, reason_str)
        else:
            print('Could not register because {}'.format(reason_str))

    def rcv_dereg_conf(self, req_num):
        self.confirm_acknowledgement(req_num)

        self.tcp_clients.clear()  # Should close all of the connections
        self.udp_client.close_socket()
        # self.udp_client = None

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.DEREGISTER_CONFIRM)
        else:
            print('Successfully deregistered')

    def rcv_dereg_denied(self, req_num, reason):
        # The client could not register with the server
        self.confirm_acknowledgement(req_num)

        reason_str = REASON.get_reason_str(reason)

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.DEREGISTER_DENIED, reason_str)
        else:
            print('Deregistration was denied because {}'.format(reason_str))

    def rcv_offer_conf(self, req_num, item_num, desc, min_price):
        # Handle UDP message to confirm registration offer was made for item
        self.confirm_acknowledgement(req_num)
        self.offers[item_num] = {'desc': desc, 'min': min_price}
        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.OFFER_CONFIRM, item_num, desc, min_price)

    def rcv_offer_denied(self, req_num, reason):
        # The client could not register with the server
        self.confirm_acknowledgement(req_num)

        reason_str = REASON.get_reason_str(reason)

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.OFFER_DENIED, reason_str)
        else:
            print('Offer was denied because {}'.format(reason_str))

    def rcv_new_item(self, item_num, desc, min_price, port):
        if item_num in self.bidding_items.keys():
            # If the NEW_ITEM message is received for an item we're already bidding on, server might be recovering
            # Re-init this item's tcp client
            del self.tcp_clients[item_num]
            self.tcp_clients[item_num] = TCPClient(self.loop, self.handle_receive, (self.server_address[0], int(port)))

            # Resend last bid
            self.send_bid(item_num, self.bidding_items[item_num]['last_bid'])
        else:
            self.bidding_items[item_num] = {'item_num': item_num, 'port_num': port, 'desc': desc, 'min': min_price,
                                            'highest': False, 'highest_bid': min_price, 'last_bid': '0'}
            self.tcp_clients[item_num] = TCPClient(self.loop, self.handle_receive, (self.server_address[0], int(port)))

            if self.gui_update_cb:
                self.gui_update_cb(MESSAGE.NEW_ITEM, item_num)

    def rcv_highest(self, item_num, amount):
        self.bidding_items[item_num]['highest_bid'] = amount
        if amount == self.bidding_items[item_num]['last_bid']:
            self.bidding_items[item_num]['highest'] = True
        else:
            self.bidding_items[item_num]['highest'] = False

        # Choose whether or not to bid more on this item
        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.HIGHEST, item_num)

    def rcv_win(self, item_num, name, ip_addr, port_num, amount):
        # Handle victory
        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.WIN, item_num, amount)
        else:
            print("You are the winner of item {}, bought for {}!".format(item_num, amount))
        self.bidding_ended(item_num)

    def rcv_bid_over(self, item_num, amount):
        # Item has been sold to another client
        self.bidding_ended(item_num)
        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.BID_OVER, item_num, amount)
        else:
            print("You are NOT the winner of item {}, bought for {}!".format(item_num, amount))

    def bidding_ended(self, item_num):
        self.tcp_clients[item_num].close_connections()
        del self.tcp_clients[item_num]
        del self.bidding_items[item_num]

    def rcv_sold_to(self, item_num, name, ip_addr, port, amount):
        # Figure out which client won the item
        del self.offers[item_num]
        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.SOLD_TO, item_num, name, ip_addr, port, amount)
        else:
            print('Winner of item {}, for {}, is {}, at {}:{}'.format(item_num, amount, name, ip_addr, port))

    def rcv_not_sold(self, item_num, reason):
        reason_str = REASON.get_reason_str(reason)

        if self.gui_update_cb:
            self.gui_update_cb(MESSAGE.NOT_SOLD, item_num, reason_str)
        else:
            print('Item {}, was not sold because {}'.format(item_num, reason_str))

    # Send Messages
    def send_register(self, name, ip_addr, port_num, resending=False, req_num_resend=-1):
        self.send_udp_message(name, ip_addr, port_num, message=MESSAGE.REGISTER,
                              resending=resending, req_num_resend=req_num_resend)

    def send_deregister(self, name, ip_addr, resending=False, req_num_resend=-1):
        self.send_udp_message(name, ip_addr, message=MESSAGE.DEREGISTER,
                              resending=resending, req_num_resend=req_num_resend)

    def send_offer(self, name, ip_addr, desc, min_price, resending=False, req_num_resend=-1):
        self.send_udp_message(name, ip_addr, desc, min_price, message=MESSAGE.OFFER,
                              resending=resending, req_num_resend=req_num_resend)

    def send_bid(self, item_num, amount):
        data_to_send = self.make_data_to_send(MESSAGE.BID.value, self.request_num_counter, item_num, amount,
                                              self.client_name)
        self.tcp_clients[item_num].send(data_to_send)
        if int(amount) > int(self.bidding_items[item_num]['last_bid']):
            self.bidding_items[item_num]['last_bid'] = amount
        self.request_num_counter += 1

    @staticmethod
    def make_data_to_send(*argv):
        # Returns encoded binary data formatted as a string with delimiters between each element
        args = []
        for arg in argv:
            args.append(str(arg))
            
        data = PROTOCOL.DELIMITER.join(args)
        return data.encode()

    def send_udp_message(self, *args, message, resending=False, req_num_resend=-1):
        #  Send UDP message to to the client
        req_num = str(self.request_num_counter if not resending else req_num_resend)
        data_to_send = self.make_data_to_send(message.value, req_num, *args)
        self.udp_client.send(data_to_send, self.server_address)
        self.loop.create_task(self.ensure_ack_received(*args, req_num=req_num, message=message, time_delay=5))
        if not resending:
            self.request_num_counter += 1

    async def ensure_ack_received(self, *args, req_num, message, time_delay):

        # Handle reset limit, don't resend same message more than 3 times
        if self.sent_messages.get(req_num):
            resend_counter = self.sent_messages[req_num][0]
            resend_counter = resend_counter - 1
            if resend_counter < 1:

                del self.sent_messages[req_num]

                if self.gui_timeout_cb:
                    self.gui_timeout_cb(message)
                else:
                    print('Error: Could not send {}. Please try again.'.format(message))

                return
            else:
                self.sent_messages[req_num][0] = resend_counter
        else:
            self.sent_messages[req_num] = [AUCTION_CONSTS.RESEND_LIMIT, args]

        # Wait until time-out to check if acknowledgement was received
        await asyncio.sleep(time_delay)

        if self.sent_messages[req_num]:
            # We timed-out without receiving an acknowledgement

            if self.gui_timeout_cb:
                resend_count = AUCTION_CONSTS.RESEND_LIMIT - self.sent_messages[req_num][0] + 1
                print('resend count = {}'.format(resend_count))
                self.gui_timeout_cb(message, resend_count)

            # For debugging
            print('TimeOut: Request number {} with args {} has not received acknowledgement'
                  .format(req_num, self.sent_messages[req_num]))

            self.send_udp_message(*args, message=message, resending=True, req_num_resend=int(req_num))

    def confirm_acknowledgement(self, req_num):
        self.sent_messages[req_num] = None
