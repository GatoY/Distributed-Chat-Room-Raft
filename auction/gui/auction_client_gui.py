import tkinter as tk
import gui.gui_helper as helper
from auctionsystem.client import AuctionClient
from gui.register_panel import RegisterPanel
from gui.deregister_panel import DeregisterPanel
from gui.items_list_panel import ItemsListPanel
from gui.my_offers_panel import MyOffersPanel
from gui.new_offer_panel import NewOfferPanel
from auctionsystem.protocol import *
import asyncio


# TODO: Add the callbacks as args to the constructor
class AuctionClientGui(tk.Frame):

    def __init__(self, master=None):
        super(AuctionClientGui, self).__init__(master)

        # asyncio event loop
        self.loop = asyncio.get_event_loop()

        self.master = master
        self.grid()

        # Fields
        self.client = None
        self.offers_history = {}
        self.registered = False

        # Row 0
        self.reg_panel = RegisterPanel(master=self, register_cb=self.register_cb)
        self.reg_panel.grid(row=0, column=0)

        self.dereg_panel = DeregisterPanel(master=self, dereg_cb=self.deregister_cb)
        self.dereg_panel.grid(row=0, column=1)

        # Row 1
        self.items_list_panel = ItemsListPanel(master=self, list_sel_cb=self.bidding_item_sel_cb, bid_cb=self.bid_cb)
        self.items_list_panel.grid(row=1, column=0)

        self.my_offers_panel = MyOffersPanel(master=self, list_ongoing_sel_cb=self.ongoing_item_sel_cb,
                                             list_ended_sel_cb=self.ended_item_sel_cb)
        self.my_offers_panel.grid(row=1, column=1)

        self.new_offer_panel = NewOfferPanel(master=self, send_offer_cb=self.send_offer_cb)
        self.new_offer_panel.grid(row=1, column=2)

        self.non_reg_panels = [self.dereg_panel, self.items_list_panel, self.my_offers_panel, self.new_offer_panel]
        self.activate_client_interface(False)

    # Recursively sets a widget and all of its children disabled or non-disabled
    def set_disabled(self, widget, disable):

        state = tk.DISABLED if disable else tk.NORMAL

        # Frame instances don't have this configuration
        if 'state' in widget.config():
            widget.configure(state=state)

        for child in widget.winfo_children():
            self.set_disabled(child, disable)

    # Button callbacks

    def register_cb(self, name, server_ip, port_num):
        self.client = AuctionClient(name=name, server_address=(server_ip, int(port_num)), gui_update_cb=self.rcv_msg,
                                    gui_timeout_cb=self.display_timeout_msg, loop=self.loop)

        self.client.client_name = name
        client_address = self.client.udp_client.address

        if client_address:
            self.client.send_register(name, client_address[0], client_address[1])
        else:
            self.set_reg_panel_response('Invalid IP address!')

    def deregister_cb(self):
        self.client.send_deregister(self.client.client_name, self.client.udp_client.address[0])

    def send_offer_cb(self, desc, min_price):
        self.client.send_offer(self.client.client_name, self.client.udp_client.address[0], desc, min_price)

    def bid_cb(self, item_num, amount):
        if item_num in self.client.bidding_items:
            self.client.send_bid(item_num, amount)

            # We want to keep a record of items that we are bidding on
            if item_num not in self.offers_history:
                self.add_new_offer_history(item_num, self.client.bidding_items[item_num]['min'],
                                           self.client.bidding_items[item_num]['desc'])

            self.set_new_item_panel_response('Sent bid of: ${}'.format(amount))
        else:
            self.set_new_item_panel_response('Invalid item number. Please select an item from the list to bid on.')

    # Listbox callbacks

    def bidding_item_sel_cb(self, item_num):
        self.update_new_item_panel(item_num)

    def ongoing_item_sel_cb(self, item_num):
        self.update_offer_info_panel(item_num)

    def ended_item_sel_cb(self, item_num):
        self.update_offer_info_panel(item_num)

    # Client callbacks

    def rcv_msg(self, command, *args):

        if command == MESSAGE.REGISTERED:
            self.activate_client_interface(True)
        elif command == MESSAGE.UNREGISTERED:
            self.set_reg_panel_response(response=args[0])
        elif command == MESSAGE.DEREGISTER_CONFIRM:
            self.activate_client_interface(False)
        elif command == MESSAGE.DEREGISTER_DENIED:
            self.set_dereg_panel_response(response=args[0])
        elif command == MESSAGE.OFFER_CONFIRM:
            self.rcv_offer_conf(item_num=args[0])
        elif command == MESSAGE.OFFER_DENIED:
            self.set_new_offer_panel_response(response=args[0])
        elif command == MESSAGE.NEW_ITEM:
            self.rcv_new_item(item_num=args[0])
        elif command == MESSAGE.HIGHEST:
            self.rcv_highest(item_num=args[0])
        elif command == MESSAGE.WIN:
            self.rcv_win(item_num=args[0], amount=args[1])
        elif command == MESSAGE.BID_OVER:
            self.rcv_bid_over(item_num=args[0], amount=args[1])
        elif command == MESSAGE.SOLD_TO:
            self.rcv_sold_to(item_num=args[0], name=args[1], ip_addr=args[2], port=args[3], amount=args[4])
        elif command == MESSAGE.NOT_SOLD:
            self.rcv_not_sold(item_num=args[0], reason=args[1])

    def display_timeout_msg(self, command, resend_count=-1):

        if resend_count > 0:
            response = 'Timeout: {0} has not received acknowledgement. Resend count: {1}'.format(command, resend_count)
        else:
            response = 'Error: Could not send {}. Please try again.'.format(command)

        if command == MESSAGE.REGISTER:
            self.set_reg_panel_response(response=response)
        elif command == MESSAGE.DEREGISTER:
            self.set_dereg_panel_response(response=response)
        elif command == MESSAGE.OFFER:
            self.set_new_offer_panel_response(response=response)

    # Message Functions

    # Called when receiving registered or dereg confirmed messages
    def activate_client_interface(self, activate):

        self.registered = activate

        # When the interface is active, the register panel is inaccessible and the rest is
        # vice-versa when it's no longer active

        self.set_disabled(self.reg_panel, activate)

        for panel in self.non_reg_panels:
            self.set_disabled(panel, not activate)

        if activate:
            self.reg_panel.clear()
            self.dereg_panel.clear()
            self.items_list_panel.clear()
            self.my_offers_panel.clear()
            self.new_offer_panel.clear()

    def set_reg_panel_response(self, response):
        if not self.registered:
            self.reg_panel.set_response_text(helper.get_formatted_display_text(response))

    def set_dereg_panel_response(self, response):
        self.dereg_panel.set_response_text(helper.get_formatted_display_text(response))

    def rcv_offer_conf(self, item_num):
        self.my_offers_panel.add_new_ongoing_item(item_num)
        self.add_new_offer_history(item_num, self.client.offers[item_num]['min'], self.client.offers[item_num]['desc'])

    def set_new_offer_panel_response(self, response):
        self.new_offer_panel.set_response_text(helper.get_formatted_display_text(response))

    def rcv_new_item(self, item_num):
        if item_num not in self.client.offers:
            self.items_list_panel.add_new_item(item_num)

    def set_new_item_panel_response(self, response):
        self.items_list_panel.new_item_panel.set_response_text(helper.get_formatted_display_text(response))

    def rcv_highest(self, item_num):
        if self.items_list_panel.selected_item == item_num:
            self.update_new_item_panel(item_num)

    def rcv_win(self, item_num, amount):
        status = 'You have won this item for ${}.'.format(amount)
        self.add_offer_history_to_list(item_num, status)

    def rcv_bid_over(self, item_num, amount):
        status = 'This item has been sold for ${}.'.format(amount)
        self.add_offer_history_to_list(item_num, status)

    def rcv_sold_to(self, item_num, name, ip_addr, port, amount):
        status = 'This item has been sold to {name}, located at ' \
                 '{ip_addr}:{port} for ${amount}.'.format(name=name, ip_addr=ip_addr, port=port, amount=amount)
        self.add_offer_history_to_list(item_num, status)

    def rcv_not_sold(self, item_num, reason):
        status = 'This item has been not been sold because: {}'.format(reason)
        self.add_offer_history_to_list(item_num, status)

    # Useful functions

    def update_new_item_panel(self, item_num):
        if item_num in self.client.bidding_items:
            self.items_list_panel.new_item_panel.update_fields(item_num=item_num,
                                                               min_price=self.client.bidding_items[item_num]['min'],
                                                               descr=self.client.bidding_items[item_num]['desc'],
                                                               highest=
                                                               self.client.bidding_items[item_num]['highest_bid'],
                                                               winning=self.client.bidding_items[item_num]['highest'])

    def update_offer_info_panel(self, item_num):
        self.my_offers_panel.info_panel.update_fields(item_num=item_num,
                                                      min_price=self.offers_history[item_num]['min'],
                                                      status=self.offers_history[item_num]['status'],
                                                      desc=self.offers_history[item_num]['desc'])

    def add_new_offer_history(self, item_num, min_price, desc):
        self.offers_history[item_num] = {'min': min_price, 'status': 'Currently being auctioned for.', 'desc': desc}

    def add_offer_history_to_list(self, item_num, status):
        self.items_list_panel.remove_item(item_num)
        if item_num in self.offers_history:
            self.offers_history[item_num]['status'] = status
            self.my_offers_panel.add_new_ended_item(item_num)

    async def run_tk_loop(self, interval=0.05):
        try:
            while True:
                self.master.update()
                await asyncio.sleep(interval)
        except tk.TclError as e:
            if "application has been destroyed" not in e.args[0]:
                raise


def exec_client():
    root = tk.Tk()
    root.title('Auction Client')
    app = AuctionClientGui(master=root)
    app.loop.run_until_complete(app.run_tk_loop())


if __name__ == '__main__':
    exec_client()
