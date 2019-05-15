# Distributed-Chat-Room-Raft

* An implementation of Raft algorithm in distributed chat-room.

## Introduction

Distributed Chat room supported by Raft consensus algorithm.

## Run
    python3 state_ini.py k  # config for k servers.
    python3 server.py  # Run server, k servers at most.
    python3 client.py i # Run client, connect to ith server 
In Client interface, first type client ID and enter the chatroom. Then live chat begin. Type {quit} to quit chatroom.


