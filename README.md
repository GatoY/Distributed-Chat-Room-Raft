# Distributed-Chat-Room-Raft

* An implementation of Raft algorithm in distributed chat-room.

## Introduction

Distributed Chat room supported by Raft consensus algorithm.

## Environment
Python 3.6+


Test on Win10 and MAC

## Architecture

* state_ini.py: initiate servers.
* config.json: records server ports and active servers.
* server.py: server for multithreaded (asynchronous) chat application in distributed server supported by Raft Algorithm.
* client.py: client part with a simple GUI.
* test.py & test2.py: test use only, not for demo.

## Run
    python3 state_ini.py k  # config for k servers and initiate server state. (e.g., python3 state_ini.py 5)
    python3 server.py  # Run server, k servers at most(type this command for k times). serverID count from 0 to k-1.
    python3 client.py i # Run client, connect to ith server (e.g., python3 client.py 0)
In Client interface, first type client ID and enter the chatroom. Then live chat begin. Type {quit} to quit chatroom.
If you want to close (fail) the leader server to see the process of leader selection, please Ctrl+c in cmd of that server, DO NOT close the session (window) directly.
Can launch new server after you close server(s) as you want until reach k servers on site.


