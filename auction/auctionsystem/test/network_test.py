import unittest
from multiprocessing import Process
from auctionsystem import client, server


class NetworkTests(unittest.TestCase):

    @staticmethod
    def test_basic_run():
        process_server = Process(target=NetworkTests.start_server)
        process_client = Process(target=NetworkTests.start_client)

        process_server.start()
        process_client.start()

    # Utility methods

    @staticmethod
    def start_server():
        server.AuctionServer()

    @staticmethod
    def start_client():
        client.AuctionClient()


if __name__ == '__main__':
    unittest.main()

