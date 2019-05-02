import tkinter as tk
from gui.new_item_panel import NewItemPanel
import gui.gui_helper as helper


class ItemsListPanel(tk.Frame):

    def __init__(self, master=None, list_sel_cb=None, bid_cb=None):
        super(ItemsListPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields
        self.list_sel_cb = list_sel_cb
        self.selected_item = ''

        # Create widgets

        # Row 0
        self.items_label = tk.Label(self, text='Items offered by others:')
        self.items_label.grid(row=0, column=0)

        # Row 1
        self.items_listbox = tk.Listbox(self)
        self.items_listbox.event_generate(helper.SELECTION_EVENT_STR)
        self.items_listbox.bind(helper.SELECTION_EVENT_STR, self.selection_cb)
        self.items_listbox.grid(row=1, column=0)

        # Row 2
        self.new_item_panel = NewItemPanel(self, bid_cb=bid_cb)
        self.new_item_panel.grid(row=2, column=0)

    def selection_cb(self, event):
        if self.items_listbox.curselection():
            self.selected_item = self.items_listbox.get(self.items_listbox.curselection())
            # See AuctionClientGui's bidding_item_sel_cb function
            self.list_sel_cb(self.selected_item)

    def add_new_item(self, item_num):
        self.items_listbox.insert(0, item_num)

    def remove_item(self, item_num):

        if helper.item_in_listbox(item_num, self.items_listbox):

            self.new_item_panel.conditional_clear(item_num)
            helper.delete_listbox_item(item_num, self.items_listbox)

    def clear(self):
        self.items_listbox.delete(0, tk.END)
        self.new_item_panel.clear()
