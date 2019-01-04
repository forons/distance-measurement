package eu.unitn.disi.distance

import eu.unitn.disi.distance.metrics._
import eu.unitn.disi.distance.utilities.{DistanceTypes, DistanceWrapper}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class Calculator(spark: SparkSession) {

  val log: Logger = LoggerFactory.getLogger(classOf[Calculator])

  val SIZE_SHINGLES: Int = 3
  val SIMILARITY_THRESHOLD: Double = 1.0
  var dim: Int = _
  var collectTime: Long = _

  def computeDistances(dfFst: Dataset[Row],
                       dfSnd: Dataset[Row],
                       distanceTypes: DistanceTypes,
                       cols: Array[Int]): Dataset[DistanceWrapper] = {
    if (dfFst == null || dfSnd == null) {
      throw new NullPointerException(
        "Either the first or the second dataset are null!")
    }
    if (dfFst.columns.length != dfSnd.columns.length) {
      throw new UnsupportedOperationException(
        "Different number of columns in the two datasets!")
    }

    var fst = dfFst
    var snd = dfSnd
    if (fst.count() > snd.count()) {
      val tmp = snd
      snd = fst
      fst = tmp
    }
    val sizeFst = fst.count
    val sizeSnd = snd.count
    dim = Math.max(sizeFst, sizeSnd).toInt

    val start = System.currentTimeMillis()

    val distances: Dataset[DistanceWrapper] =
      DistanceComputer.computeDistances(spark,
                                        dfFst = dfFst,
                                        dfSnd = dfSnd,
                                        distanceTypes = distanceTypes,
                                        cols = cols,
                                        sizeShingles = SIZE_SHINGLES,
                                        threshold = SIMILARITY_THRESHOLD)

    distances
  }

  def computeMatching(distances: Dataset[DistanceWrapper],
                      metric: MetricType): Array[DistanceWrapper] = {

    val algorithm: IAlg = metric match {
      case MetricType.GREEDY      => new GreedyAlg(spark, distances, dim)
      case MetricType.APPROXIMATE => ???
      case MetricType.AUCTION     => new AuctionAlg(spark, distances, dim)
      case MetricType.HUNGARIAN   => new HungarianAlg(spark, distances, dim)
      case _                      => ???
    }
    val matching = algorithm.computeMatching()
    collectTime = algorithm.timeToCollect
    matching
  }
}
