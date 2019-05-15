# Distributed-Chat-Room-Raft

## Intro

Chat room supported by multi servers with Raft consensus algorithm.

## Run
python3 state_ini.py 5  # means that there can be 5 servers.

python3 server.py  # Run server, can run <= 5 servers in total.

python3 client.py 0 # Run client, connect to server 0; 1 for server 1;... 4 for server 4


In Client interface, first type client ID and enter the chatroom. Then live chat begin. Type {quit} to quit chatroom.
