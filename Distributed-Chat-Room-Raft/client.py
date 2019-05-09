import socket
import threading
import json
from cmd import Cmd


class Client(Cmd):
    prompt = ''
    intro = '[Welcome] Chat Room\n' + '[Welcome]\n'
    def __init__(self):
        super().__init__()
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__id = None
        self.__nickname = None

    def __receive_message_thread(self):
        while True:
            # noinspection PyBroadException
            try:
                buffer = self.__socket.recv(1024).decode()
                obj = json.loads(buffer)
                print('[' + str(obj['sender_nickname']) + '(' + str(obj['sender_id']) + ')' + ']', obj['message'])
            except Exception:
                print('[Client] Can\'t receive msg from server')

    def __send_message_thread(self, message):
        self.__socket.send(json.dumps({
            'type': 'broadcast',
            'sender_id': self.__id,
            'message': message
        }).encode())

    def start(self):
        self.__socket.connect(('127.0.0.1', 8888))
        self.cmdloop()

    def do_login(self, args):
        nickname = args.split(' ')[0]

        self.__socket.send(json.dumps({
            'type': 'login',
            'nickname': nickname
        }).encode())
        # noinspection PyBroadException
        try:
            buffer = self.__socket.recv(1024).decode()
            obj = json.loads(buffer)
            if obj['id']:
                self.__nickname = nickname
                self.__id = obj['id']
                print('[Client] Log in the chat room successfully')

                thread = threading.Thread(target=self.__receive_message_thread)
                thread.setDaemon(True)
                thread.start()
            else:
                print('[Client] Can\'t join in the chat room')
        except Exception:
            print('[Client] Can\'t receive msg from server')

    def do_send(self, args):
        message = args
        print('[' + str(self.__nickname) + '(' + str(self.__id) + ')' + ']', message)
        thread = threading.Thread(target=self.__send_message_thread, args=(message, ))
        thread.setDaemon(True)
        thread.start()

    def do_help(self, arg):
        command = arg.split(' ')[0]
        if command == '':
            print('[Help] login nickname - ')
            print('[Help] send message - ')
        elif command == 'login':
            print('[Help] login nickname - ')
        elif command == 'send':
            print('[Help] send message - ')
        else:
            print('[Help]')
