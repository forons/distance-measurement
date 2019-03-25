#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql.types import *
from scipy.optimize import linear_sum_assignment

REPARTITION = 3000
THRESHOLD = 1.0


class DistanceComputer:

    def __init__(self, spark):
        self.spark = spark
        self.size = 0

    def compute_distances(self, fst, snd, distance_types, cols):
        if not fst or not snd:
            raise IndexError('Either the first or the second dataset is null!')

        if len(fst.columns) != len(snd.columns):
            raise IndexError('The two datasets have a different number of columns!')

        fst_size = fst.count()
        snd_size = snd.count()
        self.size = max(fst_size, snd_size)

        if fst_size < snd_size:
            fst, snd, fst_size, snd_size = DistanceComputer.swap_dataset(fst, snd, fst_size, snd_size)

        fst_cols = [fst.columns[idx] for idx in cols]
        snd_cols = [snd.columns[idx] for idx in cols]
        fst = fst.select(fst_cols)
        snd = snd.select(snd_cols)

        columns = fst.columns
        id_col_name = '__id__'
        fst = DistanceComputer.rename_columns(fst, columns, suffix='_n1')
        snd = DistanceComputer.rename_columns(snd, columns, suffix='_n2')
        fst_cols = fst.columns
        snd_cols = snd.columns
        fst = DistanceComputer.add_index_column(fst, idx_col='{}1'.format(id_col_name))
        snd = DistanceComputer.add_index_column(snd, idx_col='{}2'.format(id_col_name))

        # If there is a column to match (e.g., where cluster id is different)
        # We find the best matching with the help of Hungarian Algorithm
        fst = self.preprocess_match_column(fst, snd, fst_cols, snd_cols, distance_types)

        joined = fst.crossJoin(snd).repartition(REPARTITION)

        # Handle smart cases
        result = self.compute(joined, fst_cols, snd_cols, distance_types)

        result.show(truncate=False)
        result = result.select(F.col('{}1'.format(id_col_name)).cast(IntegerType()).alias('idA'),
                               F.col('{}2'.format(id_col_name)).cast(IntegerType()).alias('idB'),
                               F.col('distance').cast(DoubleType()))
        result.show(truncate=False)
        return result.filter(F.col('distance') < THRESHOLD).repartition(REPARTITION)

    @staticmethod
    def compute(data, fst_cols, snd_cols, distance_types):
        if 'values' in distance_types and len(distance_types['values']) == 2:
            return data.withColumn('distance',
                                   DistanceComputer.distance_values(
                                           fst_cols[distance_types['values'][0]],
                                           snd_cols[distance_types['values'][0]],
                                           fst_cols[distance_types['values'][1]],
                                           snd_cols[distance_types['values'][1]]))
        elif 'values' in distance_types and len(distance_types['values']) == 1 and \
                'matches' in distance_types and len(distance_types['matches']) == 1:
            return data.withColumn('distance',
                                   DistanceComputer.distance_values(
                                           fst_cols[distance_types['values'][0]],
                                           snd_cols[distance_types['values'][0]],
                                           fst_cols[distance_types['matches'][0]],
                                           snd_cols[distance_types['matches'][0]]))
        elif 'values' in distance_types and len(distance_types['values']) == 1 and \
                'differences' in distance_types and len(distance_types['differences']) == 1:
            difference = DistanceComputer.compute_difference(data, distance_types, fst_cols, snd_cols)
            return data.withColumn('distance',
                                   DistanceComputer.distance_value_difference(
                                           fst_cols[distance_types['values'][0]],
                                           snd_cols[distance_types['values'][0]],
                                           fst_cols[distance_types['differences'][0]],
                                           snd_cols[distance_types['differences'][0]],
                                           difference))
        else:
            # It is not a smart case, so base scenario
            # TODO: implement base scenario
            return data

    def preprocess_match_column(self, fst, snd, fst_cols, snd_cols, distance_types):
        if 'values' in distance_types and 'matches' in distance_types:
            if len(distance_types['values']) == 1 and len(distance_types['values']) == 1:
                value_col = distance_types['values'][0]
                match_col = distance_types['matches'][0]
                value_fst = fst.groupBy(fst_cols[match_col]).agg(F.collect_list(fst_cols[value_col])).collect()
                value_snd = snd.groupBy(snd_cols[match_col]).agg(F.collect_list(snd_cols[value_col])).collect()
                cost_matrix = np.zeros([len(value_fst), len(value_snd)])
                for row in range(0, cost_matrix.shape[0]):
                    for col in range(row, cost_matrix.shape[1]):
                        cost_cell = -len(DistanceComputer.intersection(value_fst[row][1], value_snd[col][1]))
                        cost_matrix[row][col] = cost_cell
                        cost_matrix[col][row] = cost_cell
                row_ind, col_ind = linear_sum_assignment(cost_matrix)

                value_map = dict()
                for idx in row_ind:
                    value_map[value_fst[idx][0]] = value_snd[col_ind[idx]][0]
                fst = fst.withColumn(fst_cols[match_col], self.handle_match_udf(value_map)(F.col(fst_cols[match_col])))
        return fst

    @staticmethod
    def handle_match_fun(elem, value_map):
        if elem in value_map:
            return value_map[elem]
        return None

    @staticmethod
    def handle_match_udf(value_map):
        return F.udf(lambda elem: DistanceComputer.handle_match_fun(elem, value_map), IntegerType())

    @staticmethod
    def compute_difference(joined, distance_types, fst_cols, snd_cols):
        max_fst = DistanceComputer.get_max(joined, fst_cols[distance_types['differences'][0]])
        max_snd = DistanceComputer.get_max(joined, snd_cols[distance_types['differences'][0]])
        min_fst = DistanceComputer.get_min(joined, fst_cols[distance_types['differences'][0]])
        min_snd = DistanceComputer.get_min(joined, snd_cols[distance_types['differences'][0]])
        difference = max(abs(max_fst - min_snd), abs(max_snd - min_fst))
        if difference == 0.0:
            difference = 1
        return difference

    @staticmethod
    def distance_value_difference(col_val1, col_val2, col_diff1, col_diff2, difference):
        return F.when(F.col(col_val1) == F.col(col_val2),
                      F.abs(F.col(col_diff1) - F.col(col_diff2)) / difference).otherwise(
            (1.0 + F.abs(col_diff1 - col_diff2) / difference) / 2.0)

    @staticmethod
    def distance_values(col_id1, col_id2, col_val1, col_val2):
        return F.when(F.col(col_id1).eqNullSafe(F.col(col_id2)) & F.col(col_val1).eqNullSafe(F.col(col_val2)),
                      0.0).otherwise(
            F.when(F.col(col_id1).eqNullSafe(F.col(col_id2)) | F.col(col_val1).eqNullSafe(F.col(col_val2)),
                   0.5).otherwise(1.0))

    @staticmethod
    def intersection(lst1, lst2):
        return list(set(lst1) & set(lst2))

    @staticmethod
    def get_min(df, col):
        return float(df.agg(F.min(col)).head[0])

    @staticmethod
    def get_max(df, col):
        return float(df.agg(F.max(col)).head[0])

    @staticmethod
    def add_index_column(df, idx_col):
        cols = df.columns
        return df.rdd.zipWithIndex().map(lambda row: (row[1],) + tuple(row[0])).toDF([idx_col] + cols)

    @staticmethod
    def rename_columns(df, cols, suffix):
        return reduce(lambda data, idx: data.withColumnRenamed(cols[idx], '{}{}'.format(cols[idx], suffix)),
                      range(0, len(cols)), df)

    @staticmethod
    def swap_dataset(fst, snd, fst_size, snd_size):
        return snd, fst, snd_size, fst_size
