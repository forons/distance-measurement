#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from pyspark.sql import functions as F
from .AbstractDistanceAlg import AbstractDistanceAlg


class AuctionAlg(AbstractDistanceAlg):

    def __init__(self, df, size):
        super().__init__(df, size)

    def compute_matching(self):
        assignments = np.full(self.size, -1)
        used = np.zeros(self.size, dtype=bool)

        zeros = self.df.filter(F.col('distance') == 0.0).collect()
        for row, col, dist in zeros:
            assignments[row] = col
            used[col] = True
            self.matches.append((row, col, dist))

        empty = self.df.filter(F.col('distance') != 0.0)

        if len(zeros) > 0:
            fst_ids = [elem[0] for elem in zeros]
            snd_ids = [elem[1] for elem in zeros]
            empty = empty.filter(not F.col('idA').isin(fst_ids)).filter(not F.col('idB').isin(snd_ids))

        empty.cache()

        distances = empty.sort(F.asc('distance')).repartition(3000).collect()

        for row, col, dist in distances:
            if assignments[row] == -1 and not used[col]:
                assignments[row] = col
                used[col] = True
                self.matches.append((row, col, dist))

        return self.matches
