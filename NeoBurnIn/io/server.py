#!/usr/bin/env python
#
# Last Change: Fri Feb 28, 2020 at 02:33 PM +0800

import logging
import datetime as dt
import aiohttp_cors

from aiohttp import web
from collections import defaultdict
from datetime import datetime

from rpi.burnin.USBRelay import set_relay_state, ON, OFF
from rpi.burnin.USBRelay import get_all_device_paths
from labSNMP.wrapper.Wiener_async import WienerControl

from NeoBurnIn.base import BaseServer
from NeoBurnIn.base import DataStream, DataStats
from NeoBurnIn.base import standard_time_format

logger = logging.getLogger(__name__)


class GroundServer(BaseServer):
    def __init__(self, host='localhost', port=45678):
        self.host = host
        self.port = port

        self.app = web.Application()
        self.register_routes()

    def run(self):
        web.run_app(self.app, host=self.host, port=self.port, access_log=None)

    def loop_getter(self):
        return None

    def register_routes(self):
        pass


class DataServer(GroundServer):
    def __init__(self, *args,
                 stdevRange=3, **kwargs):
        self.stdevRange = stdevRange
        self.stash = self.stash_create()

        super().__init__(*args, **kwargs)

    ###############
    # HTTP routes #
    ###############

    def register_routes(self):
        self.app.add_routes([
            web.post(
                '/datacollect',
                self.handler_data_collect
            ),
            web.get(
                '/get/{ch_name}',
                self.handler_stats_timeseries
            ),
            web.post(
                '/get/{ch_name}',
                self.handler_stats_timeseries
            ),
            web.post(
                '/stats/timeseries/{ch_name}',
                self.handler_stats_timeseries
            ),
        ])

        # Add CORS support
        self.cors = aiohttp_cors.setup(self.app, defaults={
            '*': aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers='*',
                allow_headers='*',
                max_age=21600
            )
        })

        for route in list(self.app.router.routes()):
            self.cors.add(route)

    async def handler_stats_timeseries(self, request):
        ch_name = request.match_info['ch_name']

        try:
            data_dump = {
                'time': self.stash[ch_name]['time'],
                'data': self.stash[ch_name]['data'],
                # 'summary': self.stash[ch_name]['summary']
            }
            return web.json_response(data_dump)

        except Exception as err:
            logger.warning('Cannot form json response due to {}'.format(
                err.__class__.__name__
            ))
            raise web.HTTPNotFound

    async def handler_data_collect(self, request):
        msg = await request.text()
        self.dispatch(msg)
        return web.Response(text='Successfully received.')

    ############
    # Dispatch #
    ############

    def dispatch(self, msg):
        splitted = self.split_input(msg)
        for entry in splitted:
            date, ch_name, value = self.validate_input(entry)

            # Proceed only if the data entry is valid
            if date is not None:
                # Log the whole entry
                logger.info('Received: {}'.format(entry))

                # Now check if this data point is OK.
                if self.stash[ch_name]['data'].reference_exists:
                    mean = self.stash[ch_name]['data'].reference_mean
                    envelop = self.stash[ch_name]['data'].reference_stdev * \
                        self.stdevRange
                    if value <= mean-envelop or value >= mean+envelop:
                        logger.critical('Channel {} measured a value of {}, which is outside of {} stds.'.format(
                            ch_name, value, self.stdevRange
                        ))

                # Store the data first.
                results = self.stash[ch_name]['data'].append(value)
                if results is not False:
                    self.stash[ch_name]['summary'].append(results[0])

                # Now check if this data point is OK.
                if self.stash[ch_name]['data'].reference_exists:
                    mean = self.stash[ch_name]['data'].reference_mean
                    envelop = self.stash[ch_name]['data'].reference_stdev * \
                        self.stdevRange
                    if value <= mean-envelop or value >= mean+envelop:
                        logger.critical('Channel {} measured a value of {}, which is outside of {} stds.'.format(
                            ch_name, value, self.stdevRange
                        ))

                # Store the time unconditionally.
                self.stash[ch_name]['time'].append(
                    self.convert_to_bokeh_time(date))

    @staticmethod
    def split_input(msg, delimiter='\n'):
        splitted = msg.split(delimiter)
        if splitted[-1] == '':
            return splitted[:-1]
        else:
            return splitted

    @staticmethod
    def validate_input(entry, delimiter='|'):
        try:
            date, ch_name, value = entry.split(delimiter)
        except Exception:
            logger.error('Entry not correctly delimited: {}'.format(entry))
            return (None, None, None)

        try:
            date = dt.datetime.strptime(date, standard_time_format)
        except Exception:
            logger.error('Datetime is not formatted correctly: {}'.format(date))
            return (None, None, None)

        try:
            value = float(value)
        except Exception:
            logger.error('Measured value is corrupt: {}'.format(value))
            return (None, None, None)

        return (date, ch_name, value)

    @staticmethod
    def convert_to_bokeh_time(date, scale=1000):
        epoch_date = dt.datetime.utcfromtimestamp(0)
        return scale * (date - epoch_date).total_seconds()

    #############################
    # Create initial data stash #
    #############################

    def stash_create(self, overall_stats_length=10000):
        '''
        Store data. If a root-level key does not exist, create it, along with
        specified empty leaves.
        '''
        stash = defaultdict(self.default_item)
        return stash

    @staticmethod
    def default_item(item_length=1000):
        return {
            'summary': DataStream(max_length=item_length),
            'time': DataStream(max_length=item_length),
            'data': DataStats(max_length=item_length,
                              defer_until_full_renewal=False)
        }


class CtrlServer(GroundServer):
    def __init__(self, *args,
                 minTimeOut=60, **kwargs):
        self.minTimeOut = minTimeOut
        self.relay_timer = None
        self.test_timer = None
        self.psu_timer = None

        super().__init__(*args, **kwargs)

    ###############
    # HTTP routes #
    ###############

    def register_routes(self):
        self.app.add_routes([
            web.post(
                '/relay/{dev_name}/{ch_name}/{state}',
                self.handler_relay_ctrl
            ),
            web.post(
                '/relay/list',
                self.handler_relay_list
            ),
            web.post(
                '/psu/{dev_ip_addr}/{ch_name}/{state}',
                self.handler_psu_ctrl
            ),
            web.post(
                '/test/{ch_name}/{state}',
                self.handler_test
            ),
        ])

    async def handler_relay_ctrl(self, request):
        dev_name = bytes(request.match_info['dev_name'], 'utf-8')
        ch_name = int(request.match_info['ch_name'])
        raw_state = request.match_info['state']
        state = self.translate_relay_state(raw_state)

        allowed_to_control = self.execute_if_not_too_recent(
            'relay_timer', self.minTimeOut)

        if not allowed_to_control:
            logger.warning('Operation denied for changing relay state to {}'.format(
                raw_state
            ))
            return web.Response(text='Operation denied: Previous operation too recent.')

        elif state:
            try:
                ret_code = set_relay_state(dev_name, ch_name, state)
                logger.info('Turning {} USB relay channel {}'.format(
                    raw_state.lower(), ch_name))

                # NOTE: The magic number 9 indicates when the relay control
                # operation was successful.
                if ret_code != 9:
                    logger.critical('Relay control failed with error code: {}'.format(
                        ret_code
                    ))
                    return web.Response(text='Failed with error code {}'.format(
                        ret_code
                    ))
                else:
                    return web.Response(text='Success')

            except Exception as err:
                warning = 'Relay control error: {}'.format(
                    err.__class__.__name__
                )
                logger.warning(warning)
                return web.Response(text=warning)

        else:
            return web.Response(text='Invalid state: {}'.format(raw_state))

    async def handler_relay_list(self, request):
        devs = '\n'.join([p.decode('utf-8') for p in get_all_device_paths()])
        return web.Response(text=devs)

    async def handler_psu_ctrl(self, request):
        dev_ip_addr = request.match_info['dev_ip_addr']
        ch_name = request.match_info['ch_name']
        state = request.match_info['state']

        allowed_to_control = self.execute_if_not_too_recent(
            'psu_timer', self.minTimeOut)

        psu = WienerControl(dev_ip_addr)

        if not allowed_to_control:
            logger.warning('Operation denied for changing PSU channel {} to state {}'.format(
                ch_name, state
            ))
            return web.Response(text='Operation denied: Previous operation too recent.')

        elif state == 'on':
            await psu.PowerOnCh(ch_name)

        elif state == 'off':
            await psu.PowerOffCh(ch_name)

        else:
            return web.Response(text='Unknown state: {}'.format(state))

        return web.Response(text='Success')

    async def handler_test(self, request):
        ch_name = request.match_info['ch_name']
        state = request.match_info['state']

        allowed_to_control = self.execute_if_not_too_recent(
            'test_timer', self.minTimeOut
        )

        if not allowed_to_control:
            logger.warning('Test command denied: Too frequent.')
            return web.Response(text='Operation denied: Previous operation too recent.')

        else:
            logger.info('Test command with channel: {} and state: {}'.format(
                ch_name, state
            ))
            return web.Response(text='Success')

    ###############################
    # Helpers for device controls #
    ###############################

    def execute_if_not_too_recent(self, timer_name, timer_period):
        cur_time = datetime.now()

        prev_time = getattr(self, timer_name)
        if prev_time:
            if (cur_time-prev_time).total_seconds() >= timer_period:
                setattr(self, timer_name, cur_time)
                return True
            else:
                return False

        else:
            setattr(self, timer_name, cur_time)
            return True

    @staticmethod
    def translate_relay_state(state):
        if state.lower() == 'off':
            return OFF
        elif state.lower() == 'on':
            return ON
        else:
            return None

    #######################################
    # Implement required abstract methods #
    #######################################

    def dispatch(self):
        pass
