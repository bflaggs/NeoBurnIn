#!/usr/bin/env python
#
# Last Change: Fri Feb 07, 2020 at 02:13 PM +0800

import abc
import sys
import yaml

from datetime import datetime
from collections import defaultdict, namedtuple
from statistics import mean, stdev
from pathlib import Path

standard_time_format = "%Y-%m-%d %H:%M:%S.%f"


###########
# Helpers #
###########

def time_now_formatted():
    '''
    Generate standardized time that should be used throughout when storing
    datetime as string.
    '''
    return datetime.now().strftime(standard_time_format)


def nested_dict():
    '''
    Recursive defaultdict that handles variable depth keys.
    '''
    return defaultdict(nested_dict)


def time_delta_in_seconds(later_time, previous_time):
    '''
    Computer time difference.
    '''
    return (later_time - previous_time).total_seconds()


def parse_config(config_file):
    if Path(config_file).exists():
        with open(config_file) as cfg:
            parsed = yaml.safe_load(cfg)
        return parsed

    else:
        print('{}: configuration file does not exist.'.format(config_file))
        sys.exit(1)


class ThreadTerminator(object):
    def __init__(self, stop_event, thread_list):
        self.stop_event = stop_event
        self.thread_list = thread_list

    def killall(self):
        self.stop_event.set()
        for th in self.thread_list:
            # Blocking until all slave threads are terminated.
            th.cleanup()


##########################
# Data structure classes #
##########################

class DataStream(list):
    '''
    Store received data, up to 'max_length'. Data will be appended to a string
    on append. The string will be used for json output, which will be used for
    visualization.
    '''
    def __init__(self, *args,
                 max_length=5, **kwargs):
        self.max_length = max_length
        self.json_str = ''
        self.list_is_full = False

        super().__init__(*args, **kwargs)

    def append(self, item):
        # Append unconditionally.
        super().append(item)

        # If the total length is strictly greater than designated upper limit,
        # pop the oldest item.
        if self.list_is_full:
            self.pop(0)
        elif len(self) > self.max_length:
            self.pop(0)
            self.list_is_full = True

        # This ensures that the datastream length is at most equal to the
        # designated upper limit.
        self.json_str = ','.join(str(x) for x in self)

        return self.list_is_full


class DataStats(DataStream):
    '''
    Store data, up to 'max_length', and generate related statistics on list
    full.  The first statistics is stored for future reference.

    If 'defer_until_full_renewal' is set to True, a new statistics will
    NOT be generated until the list is full again with each single entry
    renewed.

    Otherwise a new statistics is generated on next 'append()'.
    '''
    def __init__(self, *args,
                 defer_until_full_renewal=True, **kwargs):
        self.defer_until_full_renewal = defer_until_full_renewal
        if self.defer_until_full_renewal:
            self.renewal_counter = 0

        self.reference_exists = False
        self.reference_mean = None
        self.reference_stdev = None

        super().__init__(*args, **kwargs)

    def append(self, item):
        # Compute statistics only if the list is full.
        if super().append(item):
            if self.defer_until_full_renewal:
                return self.post_append_full_defer()  # The reference is computed only once
            else:
                return self.post_append_partial_defer()  # The reference is computed continuously

        else:
            return False

    def post_append_full_defer(self):
        self.renewal_counter += 1

        # If we've never updated the learning result, do it.
        if not self.reference_exists:
            stats = self.compute_mean_and_std()
            self.store_reference(*stats)
            return stats

        # If it is full again, compute stats, but don't store them internally.
        elif self.renewal_counter > self.max_length:
            self.renewal_counter = 1
            return self.compute_mean_and_std()

        else:
            return False

    def post_append_partial_defer(self):
        stats = self.compute_mean_and_std()
        self.store_reference(*stats)
        return stats

    def compute_mean_and_std(self):
        return (mean(self), stdev(self))

    def store_reference(self, reference_mean, reference_stdev):
        self.reference_mean = reference_mean
        self.reference_stdev = reference_stdev
        self.reference_exists = True


DataPassthru = namedtuple('DataPassthru', ('date name value'),
                          defaults=(None,)*3)


####################
# Abstract classes #
####################

class BaseDataSource(metaclass=abc.ABCMeta):
    @abc.abstractclassmethod
    def run(self, interval):
        '''
        Continuously providing data at given INTERVAL. Running in the current
        thread.
        '''

    @abc.abstractclassmethod
    def get(self):
        '''
        Return readout data once, if supported.
        '''

    @abc.abstractmethod
    def cleanup(self):
        '''
        Join spawned thread.
        '''


class BaseDataSink(metaclass=abc.ABCMeta):
    @abc.abstractclassmethod
    def on(self, ch):
        '''
        Set ch to on.
        '''

    @abc.abstractclassmethod
    def off(self, ch):
        '''
        Set ch to off.
        '''


class BaseClient(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        '''
        Start client and send all received msgs from a queue.
        '''

    @abc.abstractmethod
    def cleanup(self):
        '''
        Do cleanups on exit. Typically it is needed to clear the queue.
        '''

    @abc.abstractmethod
    def send(self, msg):
        '''
        Send a single MSG.
        '''

    @abc.abstractmethod
    def loop_getter(self):
        '''
        Return current event loop, if exists.
        '''


class BaseServer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        '''
        Start the server.
        '''

    @abc.abstractmethod
    def dispatch(self, msg):
        '''
        Dispatch received, decoded MSG.
        '''

    @abc.abstractmethod
    def loop_getter(self):
        '''
        Return current event loop, if exists.
        '''
