package eu.unitn.disi.distance.metrics

import eu.unitn.disi.distance.utilities.DistanceWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

class AuctionAlg(spark: SparkSession,
                 distances: Dataset[DistanceWrapper],
                 dim: Int)
    extends IAlg(spark, distances, dim) {

  val p: Array[Double] = Array.fill(dim)(0)
  val owner: Array[Int] = Array.fill(dim)(-1)
  val epsilon: Double = 1.0 / (dim + 1)
  val queue: mutable.Queue[Int] = mutable.Queue[Int]()
  var distancesMap: Map[(Int, Int), DistanceWrapper] = _

  override def computeMatching(): Array[DistanceWrapper] = {
    val start = System.currentTimeMillis()
    var assignedA: Set[Int] = Set.empty[Int]
    var assignedB: Set[Int] = Set.empty[Int]

    val zeros = distances.filter(col("distance") === 0.0).collect()
    for (elem <- zeros) {
      assignedA = assignedA ++ Seq(elem.idA)
      assignedB = assignedB ++ Seq(elem.idB)
      matches = matches ++ Seq(elem)
    }

    val distancesList: Array[DistanceWrapper] = distances
      .filter(col("distance") =!= 0.0)
      .filter(
        !col("idA")
          .isin(zeros.map(_.idA): _*)
          .or(!col("idB").isin(zeros.map(_.idB): _*)))
      .collect()

    timeToCollect = System.currentTimeMillis() - start

    log.info(s"Collected distances in ${timeToCollect}ms")

    distancesMap = fromListToMap(distancesList)

    List
      .range(0, dim)
      .foreach(elem =>
        if (!assignedA.contains(elem)) {
          queue.enqueue(elem)
      })

    while (queue.nonEmpty) {
      val i = queue.dequeue()
      var max = Double.MinValue
      var maxIndex = -1
      for (j <- 0 until dim if !assignedB.contains(j)) {
        val dist =
          distancesMap.getOrElse((i, j), DistanceWrapper(i, j, 1.0)).distance
        if (max < (1.0 - dist - p(j))) {
          max = 1.0 - dist - p(j)
          maxIndex = j
        }
      }
      if (max >= 0) {
        if (owner(maxIndex) != -1) {
          queue.enqueue(owner(maxIndex))
        }
        owner(maxIndex) = i
        p(maxIndex) += epsilon
      }
    }

    for (idx <- owner.indices if !assignedB.contains(idx)) {
      if (!assignedA.contains(owner(idx))) {
        matches = matches ++ Seq(
          distancesMap.getOrElse((owner(idx), idx),
                                 DistanceWrapper(owner(idx), idx, 1.0)))
      }
    }
    matches
  }
}
