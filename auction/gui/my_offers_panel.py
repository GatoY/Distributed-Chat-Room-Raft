import tkinter as tk
from gui.my_offers_info_panel import MyOffersInfoPanel
import gui.gui_helper as helper

class MyOffersPanel(tk.Frame):

    def __init__(self, master=None, list_ongoing_sel_cb=None, list_ended_sel_cb=None):
        super(MyOffersPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields
        self.selected_ongoing_item = ''
        self.list_ongoing_sel_cb = list_ongoing_sel_cb
        self.selected_ended_item = ''
        self.list_ended_sel_cb = list_ended_sel_cb

        # Create Widgets

        # Row 0
        self.ongoing_offers_label = tk.Label(self, text='My Offered Items')
        self.ongoing_offers_label.grid(row=0, column=0)

        # Row 1
        self.ongoing_offers_label = tk.Label(self, text='Ongoing Auction')
        self.ongoing_offers_label.grid(row=1, column=0)

        self.ended_offers_label = tk.Label(self, text='Auction Ended')
        self.ended_offers_label.grid(row=1, column=1)

        # Row 2
        self.ongoing_offers_listbox = tk.Listbox(self)
        self.ongoing_offers_listbox.event_generate(helper.SELECTION_EVENT_STR)
        self.ongoing_offers_listbox.bind(helper.SELECTION_EVENT_STR, self.selection_ongoing_cb)
        self.ongoing_offers_listbox.grid(row=2, column=0)

        self.ended_offers_listbox = tk.Listbox(self)
        self.ended_offers_listbox.event_generate(helper.SELECTION_EVENT_STR)
        self.ended_offers_listbox.bind(helper.SELECTION_EVENT_STR, self.selection_ended_cb)
        self.ended_offers_listbox.grid(row=2, column=1)

        # Row 3
        self.info_panel = MyOffersInfoPanel(self)
        self.info_panel.grid(row=3, column=0)

    def selection_ongoing_cb(self, event):
        if self.ongoing_offers_listbox.curselection():
            self.selected_ongoing_item = self.ongoing_offers_listbox.get(self.ongoing_offers_listbox.curselection())
            # See AuctionClientGui's ongoing_item_sel_cb function
            self.list_ongoing_sel_cb(self.selected_ongoing_item)

    def selection_ended_cb(self, event):
        if self.ended_offers_listbox.curselection():
            self.selected_ended_item = self.ended_offers_listbox.get(self.ended_offers_listbox.curselection())
            # See AuctionClientGui's ended_item_sel_cb function
            self.list_ended_sel_cb(self.selected_ended_item)

    def add_new_ongoing_item(self, item_num):
        self.ongoing_offers_listbox.insert(tk.END, item_num)

    def add_new_ended_item(self, item_num):

        # First remove the offer corresponding to the item number
        # if it is in that list
        if helper.item_in_listbox(item_num, self.ongoing_offers_listbox):

            self.info_panel.conditional_clear(item_num)
            helper.delete_listbox_item(item_num, self.ongoing_offers_listbox)

        self.ended_offers_listbox.insert(tk.END, item_num)

    def clear(self):
        self.ongoing_offers_listbox.delete(0, tk.END)
        self.ended_offers_listbox.delete(0, tk.END)
        self.info_panel.clear()
