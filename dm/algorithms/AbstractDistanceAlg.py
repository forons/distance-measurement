#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class AbstractDistanceAlg(ABC):

    def __init__(self, df, size):
        self.df = df
        self.size = size
        self.matches = []
        self.already_computed = False

    @abstractmethod
    def compute_matching(self):
        pass

    def get_distance(self):
        if not self.already_computed:
            self.compute_matching()

        distance = 0.0
        for match in self.matches:
            distance = distance + match.distance

        if not self.matches:
            return 1.0
        else:
            return distance / len(self.matches)

    @staticmethod
    def from_list_to_map(matches):
        return {(x.id_a, x.id_b): x for x in matches}
