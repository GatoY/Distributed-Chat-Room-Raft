import asyncio
import socket
import sys


class TCPServer:

    class Connection:

        def __init__(self, loop, connection, address, handle_receive_cb, logger):
            self.logger = logger
            self.addr = address
            self.sock = connection
            self._send_queue = asyncio.Queue(loop=loop)
            self.tasks = []

            self._closed = False

            # Create listener to receive data
            self.tasks.append(loop.create_task(self._handle_receive(loop, handle_receive_cb)))
            self.tasks.append(loop.create_task(self._handle_send(loop)))

        def __del__(self):
            self._closed = True
            self.sock.close()
            # Stop listening for new data to send or receive

        def send(self, data):
            # Add data to async queue to ensure it's sent in order
            # TODO: handle exception where send_queue is full
            self._send_queue.put_nowait(data)

        async def _handle_receive(self, loop, handle_receive_cb):
            while not self._closed:
                data = await loop.sock_recv(self.sock, 1024)
                self.logger.info('Received: {0} from {1}'.format(data, self.addr))
                handle_receive_cb(data, self.addr)

        async def _handle_send(self, loop):
            while not self._closed:
                # Send data to client asynchronously
                data = await self._send_queue.get()
                try:
                    await loop.sock_sendall(self.sock, data)
                    self.logger.info('Sent: {0} to {1}'.format(data, self.addr))
                    self._send_queue.task_done()
                except OSError as err:
                    self.logger.error('Could not Send: {0} to {1} Client connection could not be reached'
                                      .format(data, self.addr))

    def __init__(self, loop, handle_receive_cb, logger, port_number=0):

        self.logger = logger
        self.conn = {}  # Dict of all valid connections
        self._socket = self.get_tcp_server_socket(self.logger, port_number)

        self._closed = False

        # Start listener for new connections
        self.receive_task = loop.create_task(self._handle_new_connections(loop, handle_receive_cb))

    def __del__(self):
        self.close_connections()

    @staticmethod
    def get_tcp_server_socket(logger, port_number=0):
        ip_address = socket.getfqdn()
        server_address = (ip_address, port_number)

        # Create a TCP socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            logger.info('TCP server socket created')
        except OSError as err:
            logger.error('Failed to create server socket. Error: {0}'.format(err))
            sys.exit()

        # Bind the socket to the port
        try:
            sock.bind(server_address)
            updated_server_address = (server_address[0], sock.getsockname()[1])
            logger.info('Socket binded to {0}'.format(updated_server_address))
        except OSError as err:
            logger.error('Binding Failed to {0}. Error: {1}'.format(server_address, err))
            sys.exit()

        # Listen for new incoming connection requests
        sock.listen(8)
        return sock

    async def _handle_new_connections(self, loop, handle_receive_cb):
        while not self._closed:
            conn, addr = await loop.sock_accept(self._socket)
            connection = self.Connection(loop, conn, addr, handle_receive_cb, self.logger)
            self.conn[addr] = connection

    def close_connections(self):
        self.logger.warning('Closing connections to this TCP server')
        # Stop listening for new connections to this socket
        self.receive_task.cancel()

        # Close each client connection
        self.conn.clear()

        # Close this server's socket
        self._socket.close()

    def close_connection(self, conn):
        # Close this specific connection
        self.logger.warning('Closing connection to {}'.format(conn.addr))
        del self.conn[conn.addr]

    def get_port_number(self):
        return str(self._socket.getsockname()[1])
