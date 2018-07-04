#!/usr/bin/env python
#
# Last Change: Wed Jul 04, 2018 at 03:30 PM -0400
# Too bad. Impurities everywhere.

import logging
import logging.config
import logging.handlers

from datetime import datetime

from NeoBurnIn.base import time_delta_in_seconds


class LoggingThread(object):
    def __init__(self, queue,
                 filename, maxSize, backupCount,
                 fromaddr, toaddrs, credentials, interval
                 ):
        # Handlers for listener
        console_handler = log_handler_console()
        file_handler = log_handler_file(filename, maxSize, backupCount)
        email_handler = log_handler_email(fromaddr, toaddrs, credentials,
                                          interval)
        # Record detailed messages
        file_handler.setFormatter(log_formatter_detailed())

        self.listener = logging.handlers.QueueListener(
            queue, console_handler, file_handler, email_handler,
            respect_handler_level=True)

        # For the main logger, attach queue handler only
        queue_handler = logging.handlers.QueueHandler(queue)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(queue_handler)

    def start(self):
        self.listener.start()

    def stop(self):
        self.listener.stop()


###################
# General helpers #
###################

def parse_size_limit(size):
    size_dict = {'B': 1, 'KB': 1024, 'MB': 1024*1024}
    size_parsed = size.split(' ')
    return int(size_parsed[0]) * size_dict[size_parsed[1]]


def parse_time_limit(time):
    time_dict = {'SEC': 1, 'MIN': 60*1, 'HRS': 60*60}
    time_parsed = time.split(' ')
    return int(time_parsed[0] * time_dict[time_parsed[1]])


##############
# Formatters #
##############

def log_formatter_detailed(
    fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
):
    return logging.Formatter(fmt=fmt, datefmt=datefmt)


############
# Handlers #
############

def log_handler_console(level=logging.WARNING):
    handler = logging.StreamHandler()
    handler.setLevel(level)
    return handler


# It seems, after all, that it makes sense to log data with error messages.
def log_handler_file(filename,
                     maxSize, backupCount,
                     level=logging.INFO):
    handler = logging.handlers.RotatingFileHandler(
        filename=filename,
        maxBytes=parse_size_limit(maxSize),
        backupCount=backupCount
    )
    handler.setLevel(level)
    return handler


class AntiFloodSMTPHandler(logging.handlers.SMTPHandler):
    def __init__(self, interval_in_seconds, *args, **kwargs):
        self.last_sent = None
        self.interval_in_seconds = interval_in_seconds

        super().__init__(*args, **kwargs)

    def emit(self, record):
        if self.last_sent is None:
            # ...which means that we've never sent any email before
            self.last_sent = datetime.now()
            super().emit(record)

        else:
            now = datetime.now()
            time_elapsed_since_last_sent = time_delta_in_seconds(
                now, self.last_sent
            )
            if time_elapsed_since_last_sent >= self.interval_in_seconds:
                self.last_sent = now
                super().emit(record)


def log_handler_email(fromaddr, toaddrs, credentials, interval,
                      subject='[BurnIn]: Summary / An error has occurred',
                      mailhost=('smtp.gmail.com', 587),
                      level=logging.CRITICAL
                      ):
    # Split 'toaddrs'
    toaddrs_splitted = toaddrs.split(',')
    handler = AntiFloodSMTPHandler(
        parse_time_limit(interval),
        mailhost, fromaddr, toaddrs_splitted, subject, (fromaddr, credentials),
        secure=()  # 'secure' cannot be 'None'
    )
    handler.setLevel(level)
    return handler