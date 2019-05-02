import tkinter as tk
import gui.gui_helper as helper

class MyOffersInfoPanel(tk.Frame):

    def __init__(self, master=None):
        super(MyOffersInfoPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields

        # Create widgets

        # Row 0
        self.item_num_label = tk.Label(self, text='Item:')
        self.item_num_label.grid(row=0, column=0)

        self.item_num_label_val = tk.Label(self)
        self.item_num_label_val.grid(row=0, column=1)

        # Row 1
        self.min_price_label = tk.Label(self, text='Minimum Price:')
        self.min_price_label.grid(row=1, column=0)

        self.min_price_label_val = tk.Label(self)
        self.min_price_label_val.grid(row=1, column=1)

        # Row 2
        self.status_label = tk.Label(self, text='Status:')
        self.status_label.grid(row=2, column=0)

        self.status_label_val = tk.Label(self)
        self.status_label_val.grid(row=2, column=1)

        # Row 3
        self.desc_label = tk.Label(self, text='Description:')
        self.desc_label.grid(row=3, column=0)

        self.desc_label_val = tk.Label(self)
        self.desc_label_val.grid(row=3, column=1)

        # Initial setting of fields
        self.update_fields()

    def update_fields(self, item_num='---', min_price='---', status='---', desc='---'):
        self.item_num_label_val['text'] = item_num
        self.min_price_label_val['text'] = '${}'.format(min_price)
        self.status_label_val['text'] = helper.get_formatted_display_text(status)
        self.desc_label_val['text'] = helper.get_formatted_display_text(desc)

    def clear(self):
        self.update_fields()  # Default args are 'empty' values.

    def conditional_clear(self, item_num):
        if self.item_num_label_val['text'] == item_num:
            self.clear()
