import tkinter as tk
import gui.gui_helper as helper

class NewOfferPanel(tk.Frame):

    def __init__(self, master=None, send_offer_cb=None):
        super(NewOfferPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields
        self.send_offer_cb = send_offer_cb

        # Create widgets

        # Row 0
        self.new_offer_label = tk.Label(self, text='New Offer')
        self.new_offer_label.grid(row=0, column=0)

        # Row 1
        self.descr_label = tk.Label(self, text='Item Description:')
        self.descr_label.grid(row=1, column=0)

        # Row 2
        self.descr_text = tk.Text(self, width=30, height=6)
        self.descr_text.grid(row=2, column=0, columnspan=2)

        # Row 3
        self.min_price_label = tk.Label(self, text='Minimum Price:')
        self.min_price_label.grid(row=3, column=0)

        self.min_price_entry = tk.Entry(self)
        self.min_price_entry.grid(row=3, column=1)

        # Row 4
        self.put_up_offer_button = tk.Button(self, text='Put up for Auction', command=self.put_up_offer_cmnd)
        self.put_up_offer_button.grid(row=4, column=0, columnspan=2)

        # Row 5
        self.response_label = tk.Label(self, text='Response:')
        self.response_label.grid(row=5, column=0)

        self.response_msg_label = tk.Label(self, text='')
        self.response_msg_label.grid(row=5, column=1)

    def put_up_offer_cmnd(self):
        desc = helper.get_truncated_text(self.descr_text, 100)
        min_price = helper.get_truncated_entry(self.min_price_entry, 10)
        if min_price.isdigit():
            self.send_offer_cb(desc, min_price)
            self.set_response_text('Sent new offer.')
        else:
            self.set_response_text('Invalid minimum price. Please enter a valid number (< 10 digits).')

    def set_response_text(self, response=''):
        self.response_msg_label['text'] = helper.get_formatted_display_text(response)

    def clear(self):
        self.set_response_text()  # Default arg is empty
