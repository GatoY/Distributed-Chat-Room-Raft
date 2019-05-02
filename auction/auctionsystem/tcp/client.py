import asyncio
import socket
import sys


class TCPClient:

    def __init__(self, loop, handle_receive_cb, server_address=('', 8888)):
        self.server_address = server_address
        self._socket = self.get_tcp_client_socket()
        self._send_queue = asyncio.Queue(loop=loop)

        self._closed = False
        self.tasks = []
        self.tasks.append(loop.create_task(self.initialize_client(loop, handle_receive_cb)))

    def __del__(self):
        self.close_connections()

    def send(self, data):
        # Add data to async queue to ensure it's sent in order
        # TODO: handle exception where send_queue is full
        self._send_queue.put_nowait(data)

    def close_connections(self):
        print('Closing connections to this TCP client', file=sys.stderr)
        # Stop listening for new data to send or receive
        self._closed = True

        # Close this client's socket
        self._socket.close()

    def get_port_number(self):
        return str(self._socket.getsockname()[1])

    @staticmethod
    def get_tcp_client_socket():
        # Create a TCP socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            print('TCP client socket created')
        except OSError as err:
            print('Failed to create client socket. Error: {0}'.format(err))
            sys.exit()
        return sock

    async def initialize_client(self, loop, handle_receive_cb):
        # This task is required to ensure that the socket is connected before launching receive/send handlers
        await loop.create_task(self.connect(loop, self.server_address))
        # Create listener to receive data and send data after connection is established
        self.tasks.append(loop.create_task(self._handle_receive(loop, handle_receive_cb)))
        self.tasks.append(loop.create_task(self._handle_send(loop)))

    async def connect(self, loop, server_address):
        # Connect the TCP socket asynchronously
        try:
            await loop.sock_connect(self._socket, server_address)
            print('TCP client socket connected to server')
        except OSError as err:
            print('Failed to connect client socket. Error: {0}'.format(err))
            sys.exit()

    async def _handle_receive(self, loop, handle_receive_cb):
        while not self._closed:
            try:
                data = await loop.sock_recv(self._socket, 1024)
                print('Received: {0} from {1}'.format(data, self.server_address), file=sys.stderr)
                handle_receive_cb(data, self.server_address)
            except OSError:
                await asyncio.sleep(0.1)
                continue

    async def _handle_send(self, loop):
        while not self._closed:
            # Send data to client asynchronously
            data = await self._send_queue.get()
            try:
                await loop.sock_sendall(self._socket, data)
                print('Sent: {0} to {1}'.format(data, self.server_address), file=sys.stderr)
                self._send_queue.task_done()
            except OSError:
                print('Could not Send: {0} to {1} '
                      'Server connection could not be reached'.format(data, self.server_address), file=sys.stderr)

