import socket
import threading
import json


class Server:
    def __init__(self):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__connections = list()
        self.__nicknames = list()

    def __user_thread(self, user_id):
        connection = self.__connections[user_id]
        nickname = self.__nicknames[user_id]
        print('[Server] User', user_id, nickname, 'join in the chat room')
        self.__broadcast(message='User ' + str(nickname) + '(' + str(user_id) + ')' + 'join in the chat room')

        while True:
            # noinspection PyBroadException
            try:
                buffer = connection.recv(1024).decode()
                obj = json.loads(buffer)
                if obj['type'] == 'broadcast':
                    self.__broadcast(obj['sender_id'], obj['message'])
                else:
                    print('[Server] wrong msg:', connection.getsockname(), connection.fileno())
            except Exception:
                print('[Server] connection failed:', connection.getsockname(), connection.fileno())
                self.__connections[user_id].close()
                self.__connections[user_id] = None
                self.__nicknames[user_id] = None

    def __broadcast(self, user_id=0, message=''):
        """
        :param user_id
        :param message
        """
        for i in range(1, len(self.__connections)):
            if user_id != i:
                self.__connections[i].send(json.dumps({
                    'sender_id': user_id,
                    'sender_nickname': self.__nicknames[user_id],
                    'message': message
                }).encode())

    def start(self):
        self.__socket.bind(('127.0.0.1', 8888))
        self.__socket.listen(10)
        print('[Server] server is starting')

        self.__connections.clear()
        self.__nicknames.clear()
        self.__connections.append(None)
        self.__nicknames.append('System')

        while True:
            connection, address = self.__socket.accept()
            print('[Server] get a new connection', connection.getsockname(), connection.fileno())

            # noinspection PyBroadException
            try:
                buffer = connection.recv(1024).decode()
                obj = json.loads(buffer)
                if obj['type'] == 'login':
                    self.__connections.append(connection)
                    self.__nicknames.append(obj['nickname'])
                    connection.send(json.dumps({
                        'id': len(self.__connections) - 1
                    }).encode())

                    thread = threading.Thread(target=self.__user_thread, args=(len(self.__connections) - 1,))
                    thread.setDaemon(True)
                    thread.start()
                else:
                    print('[Server] wrong msg:', connection.getsockname(), connection.fileno())
            except Exception:
                print('[Server] receiving failed:', connection.getsockname(), connection.fileno())
