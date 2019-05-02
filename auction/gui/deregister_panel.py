import tkinter as tk


class DeregisterPanel(tk.Frame):

    def __init__(self, master=None, dereg_cb=None):
        super(DeregisterPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields
        self.dereg_cb = dereg_cb

        # Create widgets

        # Row 0
        self.dereg_button = tk.Button(self, text='Deregister', command=self.dereg_cmnd)
        self.dereg_button.grid(row=0, column=0)

        # Row 1
        # TODO: Change to read only text, make method to fill text
        self.response_label = tk.Label(self, text='Response:')
        self.response_label.grid(row=1, column=0)

        self.response_msg_label = tk.Label(self, text='')
        self.response_msg_label.grid(row=1, column=1)

    def dereg_cmnd(self):
        self.dereg_cb()

    def set_response_text(self, response=''):
        self.response_msg_label['text'] = response

    def clear(self):
        self.set_response_text()  # Default arg is empty
