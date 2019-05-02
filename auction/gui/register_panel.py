import tkinter as tk
import gui.gui_helper as helper

class RegisterPanel(tk.Frame):

    def __init__(self, master=None, register_cb=None):
        super(RegisterPanel, self).__init__(master)
        self.master = master
        self.grid()

        # Fields
        self.register_cb = register_cb

        # Create widgets

        # Row 0
        self.name_label = tk.Label(self, text='Name')
        self.name_label.grid(row=0, column=0)

        self.name_entry = tk.Entry(self)
        self.name_entry.insert(0, helper.gen_rand_str(10))
        self.name_entry.grid(row=0, column=1)

        self.server_ip_label = tk.Label(self, text='Server IP Address')
        self.server_ip_label.grid(row=0, column=2)

        self.server_ip_entry = tk.Entry(self)
        self.server_ip_entry.insert(0, '(127.0.0.1.)')
        self.server_ip_entry.grid(row=0, column=3)

        self.server_port_num = tk.Entry(self)
        self.server_port_num.insert(0, '8888')
        self.server_port_num.grid(row=0, column=4)

        # Row 1
        self.register_label = tk.Button(self, text='Register', command=self.register_cmnd)
        self.register_label.grid(row=1, column=0)

        self.response_label = tk.Label(self, text='Response:')
        self.response_label.grid(row=1, column=1)

        self.response_msg_label = tk.Label(self, text='')
        self.response_msg_label.grid(row=1, column=2, columnspan=2)

    def register_cmnd(self):
        name = helper.get_truncated_entry(self.name_entry, 10)
        server_ip = helper.get_truncated_entry(self.server_ip_entry, 41)
        port_num = helper.get_truncated_entry(self.server_port_num, 5)
        if port_num.isdigit():
            self.register_cb(name, server_ip, port_num)
        else:
            # TODO: add real user error check
            # TODO: check valid server address type
            print('PORT NUMBER MUST BE A VALID NUMBER')

    def set_response_text(self, response=''):
        self.response_msg_label['text'] = response

    def clear(self):
        self.set_response_text()  # Default arg is empty

