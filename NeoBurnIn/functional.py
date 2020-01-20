#!/usr/bin/env python
#
# Last Change: Mon Jan 20, 2020 at 02:48 AM -0500

import re


##############
# "Functors" #
##############

def name(data, reg_pattern):
    return bool(re.search(reg_pattern, data.name))


def valueGt(data, thresh):
    return data.value > thresh


def valueLt(data, thresh):
    return data.value < thresh


##########################
# "Functor" construction #
##########################

def combinator_and(functors):
    def combined(data):
        result = [f(data) for f in functors]
        return False if False in result else True
    return combined


def construct_functors(match):
    return [lambda x: globals()[f](x, match[f]) for f in match.keys()]


def parse_directive(rules):
    parsed = {}

    for rule in rules:
        functors = construct_functors(rule['match'])
        action = rule['action']

        combined = combinator_and(functors)
        executor = lambda sink: \
            sink[action['sink']].getattr(action['state'])(action['ch'])

        parsed[combined] = executor

    return parsed
