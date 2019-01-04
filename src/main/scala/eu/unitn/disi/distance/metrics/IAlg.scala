package eu.unitn.disi.distance.metrics

import eu.unitn.disi.distance.utilities.DistanceWrapper
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

abstract class IAlg(spark: SparkSession,
                    distances: Dataset[DistanceWrapper],
                    dim: Int) {

  val log: Logger = LoggerFactory.getLogger(classOf[IAlg])

  var matches: Array[DistanceWrapper] = Array.empty[DistanceWrapper]

  var timeToCollect: Long = _

  def computeMatching(): Array[DistanceWrapper]

  def getDistance: Double = {
    if (matches == null || matches.isEmpty) {
      throw new IllegalArgumentException("The matches set has to be computed")
    }
    var distance = 0.0
    for (m <- matches) {
      distance += m.distance
    }
    distance / matches.length
  }

  def fromListToMap(
      list: Array[DistanceWrapper]): Map[(Int, Int), DistanceWrapper] = {
    list.map(elem => (elem.idA, elem.idB) -> elem).toMap
  }
}
