package eu.unitn.disi.distance

import eu.unitn.disi.db.spark.sql.DFHelper
import eu.unitn.disi.distance.metrics.Hung
import eu.unitn.disi.distance.utilities.{DistanceTypes, DistanceWrapper}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.postfixOps

class DistanceComputer

object DistanceComputer {

  val log: Logger = LoggerFactory.getLogger(classOf[DistanceComputer])

  def computeDistances(spark: SparkSession,
                       dfFst: Dataset[Row],
                       dfSnd: Dataset[Row],
                       distanceTypes: DistanceTypes,
                       cols: Array[Int],
                       sizeShingles: Int = 3,
                       threshold: Double = 1.0): Dataset[DistanceWrapper] = {

    var fst = dfFst.select(cols.map(c => col(dfFst.columns(c))): _*)
    var snd = dfSnd.select(cols.map(c => col(dfSnd.columns(c))): _*)

    if (!(fst.columns sameElements snd.columns)) {
      throw new UnsupportedOperationException(
        "The provided distance measure is null")
    }

    val columns = fst.columns

    val idColumnName = "__id__"

    fst = columns.foldLeft(fst) { (df, colName) =>
      df.withColumnRenamed(colName, s"${colName}_n1")
    }
    val fstColumns = fst.columns
    fst = DFHelper.addIndexColumn(spark, fst, s"${idColumnName}1")

    snd = columns.foldLeft(snd) { (df, colName) =>
      df.withColumnRenamed(colName, s"${colName}_n2")
    }
    val sndColumns = snd.columns
    snd = DFHelper.addIndexColumn(spark, snd, s"${idColumnName}2")

    import spark.implicits._

    if (distanceTypes.values != null && distanceTypes.matches != null &&
        distanceTypes.values.length == 1 && distanceTypes.matches.length == 1) {
      val valuesFst =
        fst
          .groupBy(col(fstColumns(distanceTypes.matches(0))))
          .agg(collect_list(fstColumns(distanceTypes.values(0))))
          .collect()
      val valuesSnd =
        snd
          .groupBy(col(sndColumns(distanceTypes.matches(0))))
          .agg(collect_list(sndColumns(distanceTypes.values(0))))
          .collect()
      val rows = valuesFst.length
      val cols = valuesSnd.length
      val matrix = Array.ofDim[Int](rows, cols)
      for (i <- 0 until rows) {
        for (j <- 0 until cols) {
          matrix(i)(j) = -valuesFst(i)
            .getList(1)
            .asScala
            .intersect(valuesSnd(j).getList(1).asScala)
            .length
        }
      }
      val assignments = new Hung(matrix)
      val results = assignments.execute()
      if (!results.exists(elem => elem < 0)) {
        val map: Map[Int, Int] = (for (i <- 0 until results.length)
          yield valuesFst(i).getInt(0) -> valuesSnd(results(i)).getInt(0)).toMap
        fst = fst.withColumn(
          fstColumns(distanceTypes.matches(0)),
          handleMatchUDF(map)(col(fstColumns(distanceTypes.matches(0)))))
      }
    }

    val joined = fst.crossJoin(snd).repartition(3000)

    val distancesFull: Dataset[Row] =
      if (distanceTypes.values != null && distanceTypes.matches != null &&
          distanceTypes.differences != null &&
          distanceTypes.differences.length + distanceTypes.values.length + distanceTypes.shingles.length + distanceTypes.matches.length == 2) {
        if (distanceTypes.values.length == 2) {
          joined.withColumn(
            "distance",
            customDistanceValues(
              col(fstColumns(distanceTypes.values(0))),
              col(sndColumns(distanceTypes.values(0))),
              col(fstColumns(distanceTypes.values(1))),
              col(sndColumns(distanceTypes.values(1)))
            )
          )
        } else if (distanceTypes.values.length + distanceTypes.matches.length == 2) {
          joined.withColumn(
            "distance",
            customDistanceValues(
              col(fstColumns(distanceTypes.values(0))),
              col(sndColumns(distanceTypes.values(0))),
              col(fstColumns(distanceTypes.matches(0))),
              col(sndColumns(distanceTypes.matches(0)))
            )
          )
        } else if (distanceTypes.values.length == 1 && distanceTypes.differences.length == 1) {
          val maximum1 = joined
            .agg(max(col(fstColumns(distanceTypes.differences(0)))))
            .head
            .get(0)
            .toString
            .toDouble
          val maximum2 = joined
            .agg(max(col(sndColumns(distanceTypes.differences(0)))))
            .head
            .get(0)
            .toString
            .toDouble
          val minimum1 = joined
            .agg(min(col(fstColumns(distanceTypes.differences(0)))))
            .head
            .get(0)
            .toString
            .toDouble
          val minimum2 = joined
            .agg(min(col(sndColumns(distanceTypes.differences(0)))))
            .head
            .get(0)
            .toString
            .toDouble
          var difference = Math.max(Math.abs(maximum1 - minimum2),
                                    Math.abs(maximum2 - minimum1))
          difference = if (difference == 0.0) {
            1
          } else {
            difference
          }
          joined.withColumn(
            "distance",
            when(
              col(fstColumns(distanceTypes.values(0)))
                .equalTo(col(sndColumns(distanceTypes.values(0)))),
              abs(
                col(fstColumns(distanceTypes.differences(0)))
                  .minus(col(sndColumns(distanceTypes.differences(0)))))
                .divide(difference)
            ).otherwise(
              lit(1.0)
                .plus(
                  abs(col(fstColumns(distanceTypes.differences(0)))
                    .minus(col(sndColumns(distanceTypes.differences(0)))))
                    .divide(difference))
                .divide(2.0))
          )
        } else {
          joined
        }.
      } else {
        joined.transform(transformElems(columns, distanceTypes, sizeShingles))
      }

    distancesFull
      .select(
        col(idColumnName + "1").cast(DataTypes.IntegerType).as("idA"),
        col(idColumnName + "2").cast(DataTypes.IntegerType).as("idB"),
        col("distance").cast(DataTypes.DoubleType)
      )
      .as[DistanceWrapper]
      .filter(col("distance").lt(threshold))
      .repartition(3000)
  }

  def customDistanceValues(colId1: Column,
                           colId2: Column,
                           colVal1: Column,
                           colVal2: Column): Column = {
    when(colId1.equalTo(colId2).and(colVal1.equalTo(colVal2)), lit(0.0))
      .otherwise(when(colId1.equalTo(colId2).or(colVal1.equalTo(colVal2)),
                      lit(0.5)).otherwise(lit(1.0)))
  }

  def customDistanceValDiff(colId1: Column,
                            colId2: Column,
                            colDiffNormalized: Column): Column = {
    when(colId1.equalTo(colId2).and(colDiffNormalized.equalTo(0.0)), lit(0.0))
      .otherwise(
        when(colId1.equalTo(colId2), colDiffNormalized.divide(lit(2.0)))
          .otherwise(lit(1.0).plus(colDiffNormalized).divide(lit(2.0))))
  }

  def transformElems(cols: Array[String],
                     distanceTypes: DistanceTypes,
                     shingleSize: Int)(df: Dataset[Row]): Dataset[Row] = {
    var output = df
    for (colIdx <- cols.indices) {
      if (distanceTypes.values.contains(colIdx)) {
        output = output.transform(transformValue(cols(colIdx)))
      } else if (distanceTypes.differences.contains(colIdx)) {
        output = output.transform(transformDifference(cols(colIdx)))
      } else if (distanceTypes.shingles.contains(colIdx)) {
        output = output.transform(transformShingle(cols(colIdx), shingleSize))
      }
      // TODO: MISSING LSH
    }
    output.withColumn("distance",
                      output.columns
                        .filter(_.contains("_result"))
                        .map(col)
                        .reduce(_ + _)
                        .divide(lit(cols.length)))
  }

  def transformValue(column: String)(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn(
      s"${column}_result",
      when(col(s"${column}_n1").equalTo(col(s"${column}_n2")), lit(0.0))
        .otherwise(lit(1.0)))
  }

  def transformDifference(column: String)(df: Dataset[Row]): Dataset[Row] = {
    var output = df
    output = output.withColumn(
      s"${column}_result",
      abs(col(s"${column}_n1").minus(col(s"${column}_n2"))))
    val maximum = output.agg(max(col(s"${column}_result"))).head.getDouble(0)
    output.withColumn(s"${column}_result",
                      col(s"${column}_result").divide(maximum))
  }

  def transformShingle(column: String, size: Int)(
      df: Dataset[Row]): Dataset[Row] = {
    var output = df
    output = output
      .withColumn(s"${column}_n1_shingles",
                  nGramsUDF(size)(col(s"${column}_n1")))
      .withColumn(s"${column}_n2_shingles",
                  nGramsUDF(size)(col(s"${column}_n2")))
    output.withColumn(
      s"${column}_result",
      jaccardUDF(col(s"${column}_n1_shingles"), col(s"${column}_n2_shingles")))
  }

  def nGramsUDF(size: Int): UserDefinedFunction = {
    udf[Option[Array[String]], String] { elem: String =>
      nGrams(elem, size)
    }
  }

  def nGrams(elem: String, size: Int): Option[Array[String]] = {
    if (elem == null || elem.isEmpty) {
      return Option(Array.empty)
    }
    val length: Int = Math.min(size, elem.length)
    val maxStartIndex = elem.length - length
    Option(
      (for (i <- 0 to maxStartIndex)
        yield elem.substring(i, i + length).toLowerCase).toArray)
  }

  def jaccardUDF: UserDefinedFunction = {
    udf[Option[Double],
        mutable.WrappedArray[String],
        mutable.WrappedArray[String]] {
      (fst: mutable.WrappedArray[String], snd: mutable.WrappedArray[String]) =>
        jaccard(fst, snd)
    }
  }

  def jaccard(fst: mutable.WrappedArray[String],
              snd: mutable.WrappedArray[String]): Option[Double] = {
    val unionSize: Int = Set(fst.union(snd): _*).size
    if (unionSize == 0) {
      Option(1.0)
    } else {
      Option(1 - (fst.intersect(snd).length.toDouble / unionSize.toDouble))
    }
  }

  def handleMatchUDF(map: Map[Int, Int]): UserDefinedFunction = {
    udf[Option[Int], Int] { elem: Int =>
      handleMatch(elem, map)
    }
  }

  def handleMatch(elem: Int, map: Map[Int, Int]): Option[Int] = {
    Option(map.getOrElse(elem, elem))
  }

  //  def transformLSH(cols: Array[String], size: Int)(
  //      df: Dataset[Row]): Dataset[Row] = {
  //    var output = df
  //    for (column <- cols) {
  //      output = output
  //        .withColumn(s"${column}_n1_shingles",
  //                    nGramsUDF(size)(col(s"${column}_n1")))
  //        .withColumn(s"${column}_n2_shingles",
  //                    nGramsUDF(size)(col(s"${column}_n2")))
  //      val vectorAssembler = new VectorAssembler()
  //        .setInputCols(Array(s"${column}_n1_shingles", s"${column}_n2_shingles"))
  //        .setOutputCol(s"${column}_shingles")
  //      val mh = new MinHashLSH()
  //        .setNumHashTables(5)
  //        .setInputCol(vectorAssembler.getOutputCol)
  //        .setOutputCol(s"${vectorAssembler.getOutputCol}_hashes")
  //
  //      val model: MinHashLSHModel = mh.fit(output)
  //      model.transform(output)
  //
  //      output = output.withColumn(s"${column}_result",
  //                                 jaccardUDF(col(s"${column}_n1_shingles"),
  //                                            col(s"${column}_n2_shingles")))
  //    }
  //    output.withColumn("distance",
  //                      output.columns
  //                        .filter(_.contains("_result"))
  //                        .map(col)
  //                        .reduce(_ + _)
  //                        .divide(lit(cols.length)))
  //  }
}
