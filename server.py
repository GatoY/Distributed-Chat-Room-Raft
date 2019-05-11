"""Server for multithreaded (asynchronous) chat application."""
from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread


class Server:
    def __init__(self):
        self.clients = {}
        self.addresses = {}
        self.HOST = ''
        self.PORT = 33000
        self.BUFSIZ = 1024
        self.ADDR = (self.HOST, self.PORT)

    def start(self):
        self.SERVER = socket(AF_INET, SOCK_STREAM)
        self.SERVER.bind(self.ADDR)

        self.SERVER.listen(5)
        print("Waiting for connection...")
        self.ACCEPT_THREAD = Thread(target=self.accept_incoming_connections)
        self.ACCEPT_THREAD.start()
        self.ACCEPT_THREAD.join()
        self.SERVER.close()

    def accept_incoming_connections(self):
        """Sets up handling for incoming clients."""
        while True:
            client, client_address = self.SERVER.accept()
            print("%s:%s has connected." % client_address)
            client.send(bytes("Welcome! Type your username and press enter to continue.", "utf8"))
            self.addresses[client] = client_address
            Thread(target=self.handle_client, args=(client,)).start()


    def handle_client(self, client):  # Takes client socket as argument.
        """Handles a single client connection."""

        name = client.recv(self.BUFSIZ).decode("utf8")
        welcome = 'Welcome %s! If you want to quit, type {quit} to exit.' % name
        client.send(bytes(welcome, "utf8"))
        msg = "%s has joined the chat!" % name
        self.broadcast(bytes(msg, "utf8"))
        self.clients[client] = name

        while True:
            msg = client.recv(self.BUFSIZ)
            if msg != bytes("{quit}", "utf8"):
                self.broadcast(msg, name + ": ")
            else:
                client.send(bytes("{quit}", "utf8"))
                client.close()
                del self.clients[client]
                self.broadcast(bytes("%s has left the chat." % name, "utf8"))
                break


    def broadcast(self, msg, prefix=""):  # prefix is for name identification.
        """Broadcasts a message to all the clients."""

        for sock in self.clients:
            sock.send(bytes(prefix, "utf8") + msg)



if __name__ == "__main__":
    server = Server()
    server.start()