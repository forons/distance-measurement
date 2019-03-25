#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from scipy import optimize, sparse
from .AbstractDistanceAlg import AbstractDistanceAlg


class HungarianAlg(AbstractDistanceAlg):

    def __init__(self, df, size):
        super().__init__(df, size)

    def compute_matching(self):
        distances = self.df.collect()
        cost_matrix = sparse.coo_matrix((self.size, self.size), dtype=np.float32)
        for row, col, dist in distances:
            cost_matrix[row, col] = dist
        results = optimize.linear_sum_assignment(cost_matrix)
        for row, col in results:
            self.matches.append((row, col, cost_matrix[row, col]))
        return self.matches
