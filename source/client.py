from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
import tkinter
import json
import sys


class Client():
    def __init__(self, server_id):
        CONFIG = json.load(open("config.json"))
        PORT = CONFIG['server_port'][server_id]['port']

        self.top = tkinter.Tk()
        self.top.title("Chat")

        self.messages_frame = tkinter.Frame(self.top)

        self.my_msg = tkinter.StringVar()  # For the messages to be sent.
        self.my_msg.set("")

        self.scrollbar = tkinter.Scrollbar(self.messages_frame)  # To navigate through past messages.
        # Following will contain the messages.
        self.msg_list = tkinter.Listbox(self.messages_frame, height=15, width=100, yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
        self.msg_list.pack(side=tkinter.LEFT, fill=tkinter.BOTH)
        self.msg_list.pack()
        self.messages_frame.pack()

        self.entry_field = tkinter.Entry(self.top, textvariable=self.my_msg, width=75)
        self.entry_field.bind("<Return>", self.send)
        self.entry_field.pack()
        self.send_button = tkinter.Button(self.top, text="Send", command=self.send)
        self.send_button.pack()

        self.top.protocol("WM_DELETE_WINDOW", self.on_closing)

        # ----Now comes the sockets part----
        HOST = 'localhost'
        self.BUFSIZ = 1024
        ADDR = (HOST, PORT)

        self.client_socket = socket(AF_INET, SOCK_STREAM)
        self.client_socket.connect(ADDR)

        receive_thread = Thread(target=self.receive)
        receive_thread.start()
        tkinter.mainloop()  # Starts GUI execution.

    def receive(self):
        """Handles receiving of messages."""
        while True:
            try:
                msg = self.client_socket.recv(self.BUFSIZ).decode("utf8")
                self.msg_list.insert(tkinter.END, msg)
            except OSError:  # Possibly client has left the chat.
                break

    def send(self, event=None):  # event is passed by binders.
        """Handles sending of messages."""
        msg = self.my_msg.get()
        self.my_msg.set("")  # Clears input field.
        self.client_socket.send(bytes(msg, "utf8"))
        if msg == "{quit}":
            self.client_socket.close()
            self.top.quit()

    def on_closing(self, event=None):
        """This function is to be called when the window is closed."""
        self.my_msg.set("{quit}")
        self.send()

client = Client(sys.argv[1])

