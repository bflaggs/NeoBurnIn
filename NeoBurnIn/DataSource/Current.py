#!/usr/bin/env python
#
# Last Change: Fri Feb 28, 2020 at 02:23 PM +0800

import logging

from threading import Thread
from labSNMP.wrapper.Wiener import WienerControl

from NeoBurnIn.base import BaseDataSource, DataPassthru
from NeoBurnIn.base import time_now_formatted

logger = logging.getLogger(__name__)


class CurrentMaratonDataSource(Thread, BaseDataSource, ):
    def __init__(self, stop_event, queue, interval, ipAddr, *args,
                 displayName=None, psuChannels=list(), **kwargs):
        self.stop_event = stop_event
        self.queue = queue
        self.interval = interval
        self.displayName = displayName
        self.psuChannels = psuChannels

        self.psu = WienerControl(ipAddr)

        super().__init__(*args, **kwargs)

    def run(self):
        while not self.stop_event.wait(self.interval):
            currs = self.get()
            for c in currs:
                self.queue.put(c)

    def get(self):
        result = []
        for ch in self.psuChannels:
            curr = self.psu.ChCurrent(ch)
            result.append(
                DataPassthru(time_now_formatted(), self.displayName+str(ch),
                             curr[2]))
        return result

    def cleanup(self):
        self.join()
