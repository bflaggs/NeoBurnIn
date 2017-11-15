#!/usr/bin/env python
#
# Last Change: Wed Nov 15, 2017 at 11:56 AM -0500

import asyncio
import logging
import logging.config

from multiprocessing import Process as Container

from bUrnIn.server.logging import generate_config_worker


class TransmissionServerAsync():
    '''
    Single-process TCP server with asyncio.
    '''
    def __init__(self, host, port,
                 msgs=None, errs=None, emails=None,
                 size=4096, max_retries=3, timeout=5):
        self.host = host
        self.port = port
        self.size = size
        self.max_retries = max_retries
        self.timeout = timeout

        # Bind all queues to the object
        self.msgs = msgs
        self.errs = errs
        self.emails = emails

        # Store all unterminated clients in a dictionary
        self.clients = dict()

    def client_accept(self, client_reader, client_writer):
        task = asyncio.ensure_future(
            self.client_handle(client_reader, client_writer))
        self.clients[task] = (client_reader, client_writer)

        def client_done(task):
            del self.clients[task]
            client_writer.close()

        task.add_done_callback(client_done)

    @asyncio.coroutine
    def client_handle(self, client_reader, client_writer):
        retries = 0

        # Here we design a very simple protocol:
        #   Messages can have variable length, but it's end is indicated by a
        #   'EOM' byte array.
        #   We require the receiving length should at least be 3.
        #   We also require the message be encoded in UTF-8.

        EOM = bytearray(b'EOM')
        data = bytearray()

        while True:
            # Asynchronously read chunks of data from the socket
            try:
                chunk = yield from asyncio.wait_for(
                    client_reader.read(self.size), timeout=self.timeout)
                data.extend(chunk)

            except asyncio.TimeoutError:
                # Keep trying until we reach the maximum retries
                retries += 1
                # Also, clear the full buffer and start from scratch
                data.clear()

                if retries is self.max_retries:
                    # self.errs.put((socket.timeout, time_stamp(self.time_format),
                                    # data))
                    break

            except Exception as err:
                # self.errs.put((err, time_stamp(self.time_format), data))
                break

            else:
                # Note that we require the length of the message to be no less
                # than 3.
                if data[-3:] == EOM:
                    self.msgs.put(bytes(data[:-3]).decode("utf-8"))
                    # We reached 'EOM': End-Of-Message
                    break

    def listen(self):
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.client_accept, self.host, self.port,
                                    loop=loop)
        server = loop.run_until_complete(coro)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            # This signals that we should shut down.
            pass
        finally:  # Exit gracefully
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()
            self.msgs.put(None)  # This tells the dispatcher to shutdown

    def start(self):
        server_process = Container(target=self.listen)
        server_process.start()
        self.server_process = server_process
