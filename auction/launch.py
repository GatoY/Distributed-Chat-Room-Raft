from auctionsystem import server
from gui.auction_client_gui import exec_client

print('server (0) or client(1) or server_with_recover(2):')
i = input()
print(i)

if i is '0':
    print('server')
    server.AuctionServer()
if i is '1':
    print('client')
    exec_client()
if i is '2':
    print('server')
    server.AuctionServer(recover=True)
