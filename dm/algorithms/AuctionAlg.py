#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from collections import deque
from pyspark.sql import functions as F
from .AbstractDistanceAlg import AbstractDistanceAlg


class AuctionAlg(AbstractDistanceAlg):

    def __init__(self, df, size):
        super().__init__(df, size)

    def compute_matching(self):
        fst_assigned = set()
        snd_assigned = set()
        p = np.full(self.size, 0)
        owner = np.full(self.size, -1)
        epsilon = 1.0 / (self.size + 1)

        zeros = self.df.filter(F.col('distance') == 0.0).collect()
        for row, col, dist in zeros:
            fst_assigned.add(row)
            snd_assigned.add(col)
            self.matches.append((row, col, dist))

        distances = self.df \
            .filter(F.col('distance') != 0.0) \
            .filter(not F.col('idA').isin([elem[0] for elem in zeros])) \
            .filter(not F.col('idB').isin([elem[0] for elem in zeros])) \
            .collect()

        distances_map = AuctionAlg.from_list_to_map(distances)
        queue = deque()
        for idx in range(0, self.size):
            if idx not in fst_assigned:
                queue.append(idx)

        while queue:
            row = queue.popleft()
            col_idx = -1
            col_value = float('inf')
            for col in range(0, self.size):
                if col not in snd_assigned:
                    distance = distances_map.get((row, col), (row, col, 1.0))[2]
                    curr_value = 1.0 - distance - p[0]
                    if col_value < curr_value:
                        col_value = curr_value
                        col_idx = col

            if col_value >= 0:
                if not owner[col_idx] == -1:
                    queue.append(owner[col_idx])
                owner[col_idx] = row
                p[col_idx] = p[col_idx] + epsilon

        for idx, elem in enumerate(owner):
            if idx not in snd_assigned:
                self.matches.append(distances_map.get((elem, idx), (elem, idx, 1.0)))

        return self.matches
