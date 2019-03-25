#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql.dataframe import DataFrame


def transform(self, f):
    return f(self)


DataFrame.transform = transform
