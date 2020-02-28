#!/usr/bin/env python
#
# Last Change: Wed Feb 19, 2020 at 11:54 PM +0800

import logging

from NeoBurnIn.base import BaseDataSink

logger = logging.getLogger(__name__)


class PowerSupplySink(BaseDataSink):
    def __init__(self, host, port, psu_ip, entry='psu'):
        self.host = host
        self.port = port
        self.psuIp = psuIp
        self.entry = entry

    def on(self, ch):
        return 'http://{}:{}/{}/{}/{}/{}'.format(
            self.host, self.port, self.entry, self.psuIp, ch, 'on'
        )

    def off(self, ch):
        return 'http://{}:{}/{}/{}/{}/{}'.format(
            self.host, self.port, self.entry, self.psuIp, ch, 'off'
        )
