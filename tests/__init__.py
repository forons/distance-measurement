from pyspark.sql import SparkSession
from dm.calculators.DistanceComputer import DistanceComputer
from dm.algorithms.Alg import Alg
import logging

# import findspark


if __name__ == '__main__':
    # findspark.init('/Users/forons/Downloads/spark-2.3.2-bin-hadoop2.7')
    logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
    spark = SparkSession.builder\
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .appName('NoiseGenerators - Test').getOrCreate()
    df1 = spark.read.csv(
        'hdfs://zeus.disi.unitn.it:8020/datastore/daniele/dataset/analysis/clustering/clean/adult.data/0/',
        inferSchema=True, header=True).limit(100)
    df2 = spark.read.csv(
        'hdfs://zeus.disi.unitn.it:8020/datastore/daniele/dataset/analysis/clustering/noise/adult.data/0/NULL/0.6/',
        inferSchema=True, header=True).limit(100)
    computer = DistanceComputer(spark)
    output = computer.compute_distances(df1, df2, {'values': [0], 'matches': [1]}, [0, 1])
    # output.repartition(3000).cache().repartition(3000)
    output.show()

    matching = Alg.determine_alg('HUNGARIAN', output, computer.size)
    result = matching.compute_matching()


