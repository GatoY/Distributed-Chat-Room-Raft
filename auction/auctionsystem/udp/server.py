import asyncio
import socket
import sys


class UDPServer:

    def __init__(self, loop, handle_receive_cb, logger, port_number=8888):
        self.logger = logger

        self._closed = False
        self._socket = self._get_udp_server_socket(self.logger, port_number)

        # Start listeners for read/write events
        self.task = loop.create_task(self._handle_receive(loop, handle_receive_cb))

    def send(self, data, addr):
        self._socket.sendto(data, addr)
        self.logger.info('To {0} sent: {1}'.format(addr, data))

    def close_socket(self):
        self.logger.info('Closing socket')
        self._closed = True
        self._socket.close()

    @staticmethod
    def _get_udp_server_socket(logger, port_number=8888):
        # Create a UDP socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # TODO: Check if we need to use sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            logger.info('UDP server socket created')
        except OSError as err:
            logger.error('Failed to create server socket. Error: {0}'.format(err))
            sys.exit()

        ip_address = socket.getfqdn()  # Get fully qualified domain name
        server_address = (ip_address, port_number)

        # Bind the socket to the port
        try:
            sock.bind(server_address)
            logger.info('Socket binded to {0}'.format(server_address))
        except OSError as err:
            logger.error('Binding Failed to {0}. Error: {1}'.format(server_address, err))
            sys.exit()

        return sock

    async def _handle_receive(self, loop, handle_receive_cb):
        while not self._closed:
            data, addr = await self._async_recvfrom(loop, 1024)
            if (data, addr) == (None, None):
                # This means we received an ICMP Error, ignore this data
                continue
            self.logger.info('From {0} received: {1}'.format(addr, data))
            handle_receive_cb(data, addr)

    def _async_recvfrom(self, loop, n_bytes, future=None, registered=False):
        # asyncio doesn't have an asynchronous version of recvfrom so this is an implementation of it

        # Get the socket's file number so that it can be monitored for change
        sock_file = self._socket.fileno()

        # Only create a new Future object if there isn't already one
        if future is None:
            future = loop.create_future()

        # Avoid adding the same reader multiple times
        if registered:
            loop.remove_reader(sock_file)

        # Check and see if the data and address have been received, else wait on future values
        try:
            data, addr = self._socket.recvfrom(n_bytes)
        except (BlockingIOError, InterruptedError):
            loop.add_reader(sock_file, self._async_recvfrom, loop, n_bytes, future, True)
        except OSError as err:
            # If we get this OSError, it is most likely because the remote host forcibly closed their socket
            # In such a case, we can receive an ICMP error packet in the recvfrom after our last sendto failed to reach
            # In our case, we can assume this probably means that the client was somehow forcibly closed
            future.set_result((None, None))
            return future
        else:
            future.set_result((data, addr))
        return future
