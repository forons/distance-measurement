#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from enum import Enum
from .HungarianAlg import HungarianAlg


class Alg(Enum):
    HUNGARIAN = 0
    GREEDY = 1
    AUCTION = 2

    @staticmethod
    def determine_alg(given_alg, df, size):
        try:
            alg = given_alg.upper()
            if Alg[alg] is Alg.HUNGARIAN:
                return HungarianAlg(df, size)
            elif Alg[alg] is Alg.GREEDY:
                return HungarianAlg(df, size)
            elif Alg[alg] is Alg.AUCTION:
                return HungarianAlg(df, size)
            else:
                raise IndexError('The given value {} is not an algorithm supported'.format(alg))
        except Exception:
            raise IndexError('The given value {} is not an algorithm supported'.format(given_alg))
