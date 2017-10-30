#!/usr/bin/env python
#
# Last Change: Mon Oct 30, 2017 at 12:17 AM -0400

import socket
import threading

from bUrnIn.server.base import BaseSignalHandler


class TransmissionServer(BaseSignalHandler):
    '''
    Multi-threaded TCP server that is blocking.
    This server spawns client sockets in separate threads.
    It also handles SIGINT and SIGTERM so that it will wait for all threads to
    shut down before exit.
    '''
    def __init__(self, host, port,
                 size=4096, max_retries=3, max_connections=5, timeout=5,
                 ping_lifetime=600,
                 db_filename="",
                 log_filename=""):
        # Register handler for SIGINT and SIGTERM
        # so that this server can exit gracefully
        self.signal_handle()

        self.host = host
        self.port = port

        self.size = size
        self.max_retries = max_retries
        self.max_connections = max_connections
        self.timeout = timeout

        self.ping_lifetime = ping_lifetime
        self.db_filename = db_filename
        self.log_filename = log_filename

        # Provide a known client dict so that we can monitor when a client goes
        # offline

        # Provide locks for all clientsocket threads
        self.filelock = threading.Lock()
        self.dictlock = threading.Lock()

        # NOTE: our socket is a blocking socket
        #       once we receive a connection, we immediately create a dispatcher
        #       client socket to handle that and back to listening

        # SOCK_STREAM means that this is a TCP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # This allows OS to immediately reuse the address without waiting for
        # the existing socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(self.max_connections)

        while not self.stop:
            try:
                clientsocket, address = self.sock.accept()
                # Here the clientsocket is a non-blocking one!
                # where as our seversocket is blocking
                clientsocket.settimeout(self.timeout)

                handler = threading.Thread(target=self.client_handle,
                                           args=(clientsocket, address))
                # We set all clientsocket handler to be daemon threads
                # so that we can wait them to finish before exit the main
                # program
                handler.daemon = True
                handler.start()

            except OSError as err:
                if err.errno is 9:
                    # This is likely due to a SIGINT or SIGTERM signal
                    # and we are trying to shut down the server now
                    pass

                else:
                    raise(err)

        # Exit gracefully
        # Make sure all threads are (properly) closed
        for t in threading.enumerate():
            if t.daemon:
                t.join()

    def client_handle(self, clientsocket, address):
        retries = 0

        while retries <= self.max_retries:
            try:
                msg = clientsocket.recv(self.size)

            except clientsocket.timeout:
                # Keep trying until we reach the maximum retries
                retries += 1

            except clientsocket.error as err:
                self.dispatcher(msg, address, err)

            else:
                if not msg:
                    # We reached End-Of-Message
                    break
                else:
                    self.dispatcher(msg, address)

        clientsocket.close()

    def exit(self, signum, frame):
        print("Termination signal received, prepare to exit...")
        self.stop = True
        self.sock.close()

    def dispatcher(self, msg, address, err=None):
        pass