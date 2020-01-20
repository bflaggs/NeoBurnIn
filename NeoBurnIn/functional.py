#!/usr/bin/env python
#
# Last Change: Mon Jan 20, 2020 at 03:04 AM -0500

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


# NOTE: The peculiar form of the lambda function arguments is needed to bind
# variables early, namely **during** the list comprehension, not **after**!
def construct_functors(match):
    return [lambda x, f=f, arg=arg: globals()[f](x, arg)
            for f, arg in match.items()]


def parse_directive(rules):
    parsed = {}

    for rule in rules:
        functors = construct_functors(rule['match'])
        action = rule['action']

        combined = combinator_and(functors)
        executor = lambda sink, state=action['state'], ch=action['ch']: \
            sink[action['sink']].getattr(state)(ch)

        parsed[combined] = executor

    return parsed
