#!/usr/bin/env python
#
# Last Change: Wed Nov 15, 2017 at 12:42 PM -0500

import signal

from configparser import ConfigParser
from os import getcwd
from os.path import join, isfile
from multiprocessing import Queue, Event

from bUrnIn.server.transmission import TransmissionServerAsync
from bUrnIn.server.dispatcher import Dispatcher
from bUrnIn.server.logging import LoggerForMultiProcesses

def parse_config(cfg):
    config = ConfigParser()
    opts_dict = dict()

    config.read(cfg)

    for key in config:
        opts_dict[key] = config[key]

    return opts_dict


if __name__ == "__main__":
    #######################
    # Parse configuration #
    #######################
    GLOBAL_CFG  = '/etc/server-trans/config'
    DEFAULT_CFG = join(getcwd(), 'server-trans.cfg')

    if isfile(GLOBAL_CFG):
        opts = parse_config(GLOBAL_CFG)
    else:
        opts = parse_config(join(getcwd(), DEFAULT_CFG))

    ################################
    # Prepare inter-process queues #
    ################################
    msgs = Queue()
    logs = Queue()
    stop_event = Event()

    ################
    # Start logger #
    ################
    logger = LoggerForMultiProcesses(opts['log']['filename'], stop_event)
    logger.start()

    ####################
    # Start dispatcher #
    ####################
    dispatcher = Dispatcher(msgs=msgs, logs=logs,
                            db_filename=opts['db']['filename'])
    dispatcher.start()

    #################################################
    # Handle SIGTERM and SIGINT on the main process #
    #################################################
    def on_exit(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    ############################################
    # Start the TCP server on the main process #
    ############################################
    server = TransmissionServerAsync(
        opts['main']['ip'],
        int(opts['main']['port']),
        msgs=msgs,
        timeout=int(opts['main']['timeout']))
    server.listen()

    ###########
    # Cleanup #
    ###########
    dispatcher.dispatcher_process.join()
    stop_event.set()
    logger.listener_process.join()
