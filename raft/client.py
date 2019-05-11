#	client 
#	Implemented by Zhijing@Mar. 7
import json
import socket
import datetime
import sys
import time
import threading

CONFIG = json.load(open('config.json'))

client_id = sys.argv[1]


def Receive(c):
    while True:
        data, address = c.recvfrom(4096)
        print(data)


def Request(port, message, c):
    host = ''
    addr = (host, port)
    sent = c.sendto(message, addr)
    # !!! TODO: change recvfrom to a global wait and print
    # time.sleep(2)
    # data, server = c.recvfrom(4096)
    # c.close()

def RequestTicket(port, buy_num, request_id, c):
    message = ('BUY:"{client_id}",{request_id},' +
               '{ticket_count}\n').format(
                           client_id=client_id,
                           request_id=request_id,
                           ticket_count=buy_num)
    Request(port, message, c)

def RequestShow(port, c):
    Request(port, 'SHOW:\n', c)

def RequestChange(port, new_config, c):
    # TODO: add functionality for config change
    Request(port, 'CHANGE:%s\n' % new_config, c)

def Interface_cmd(c):
    choice = True
    request_id = 0
    datacenter = CONFIG['datacenters']
    datacenter_list = []
    for i in range(1, len(datacenter)+1):
        datacenter_list.append(datacenter[str(i)]['port'])
        print('datacenter: '+ str(datacenter[str(i)]['port']))
    cmd = raw_input('Please choose a server to connect... or exit...\t')
    if cmd.startswith('exit'):
        choice = False
    else:
        server_selected = int(cmd)
    while choice:
        
        command = raw_input('Command: buy {numberOfTicket} / show / change {param1, param2} / exit?\t')

        if command.startswith('buy'):
            RequestTicket(server_selected, int(command.lstrip('buy ')),
                              request_id, c)
            request_id += 1
        elif command.startswith('show'):
            RequestShow(server_selected, c)
        elif command.startswith('change'):
            RequestChange(server_selected, command.lstrip('change '), c)
        elif command.startswith('exit'):
            break
        time.sleep(5)
            



def main():
    print("\n*********************************\n")
    print("Welcome to SANDLAB Ticket Office!")
    print("The current time is " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("\n*********************************\n")

    c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t = threading.Thread(target=Receive, args = (c, ))
    t.daemon = True
    t.start()

    Interface_cmd(c)
    exit()

if __name__ == "__main__":
    main()

