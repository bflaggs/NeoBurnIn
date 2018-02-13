#!/usr/bin/env python
#
# Last Change: Tue Feb 13, 2018 at 03:25 PM -0500

import socket

from time import sleep
from datetime import datetime
from random import uniform

import sys
sys.path.insert(0, '..')


class NaiveClient():
    def __init__(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ip = ip
        self.port = port

        self.EOM = 'EOM'

    def send(self, msg):
        try:
            self.sock.connect((self.ip, self.port))
            self.sock.sendall(bytes(msg + self.EOM, 'utf-8'))

        finally:
            self.sock.close()


if __name__ == "__main__":
    while True:
        try:
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            dataset = dict()
            dataset[sys.argv[2]] = (date, str(uniform(1, 10)))

            msg = ''
            for key in dataset.keys():
                msg = msg + dataset[key][0] + '|'
                msg = msg + key + '|'
                msg = msg + dataset[key][1] + '\n'

            print(msg.strip('\n'))

            client = NaiveClient(sys.argv[1], 45678)
            client.send(msg)

            sleep(1)

        except KeyboardInterrupt:
            break
