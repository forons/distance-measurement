package eu.unitn.disi.distance.metrics

import eu.unitn.disi.distance.utilities.DistanceWrapper
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{asc, col}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GreedyAlg(spark: SparkSession,
                distances: Dataset[DistanceWrapper],
                dim: Int)
    extends IAlg(spark: SparkSession,
                 distances: Dataset[DistanceWrapper],
                 dim: Int) {

  override def computeMatching(): Array[DistanceWrapper] = {
    val start = System.currentTimeMillis()
    val matching: Array[Int] = Array.fill[Int](dim)(-1)
    val used: Array[Boolean] = Array.fill[Boolean](dim)(false)

    val zeros = distances.filter(col("distance") === 0.0).collect()
//    val broadcast = spark.sparkContext.broadcast(zeros)
    for (elem <- zeros) {
      matching(elem.idA) = elem.idB
      used(elem.idB) = true
      matches = matches ++ Seq(elem)
    }

    var checkingEmpty = distances
      .filter(col("distance") =!= 0.0)

    checkingEmpty = if (zeros.length <= 0) {
      checkingEmpty
    } else {
      checkingEmpty.filter(
        !col("idA")
          .isin(zeros.map(_.idA): _*)
          .or(!col("idB").isin(zeros.map(_.idB): _*)))
    }

    checkingEmpty.cache()

    val distancesList: Array[DistanceWrapper] =
      checkingEmpty.sort(asc("distance")).repartition(3000).collect()

    timeToCollect = System.currentTimeMillis() - start

    log.info(s"Collected distances in ${timeToCollect}ms")

    for (elem <- distancesList if matching(elem.idA) == -1 && !used(elem.idB)) {
      matching(elem.idA) = elem.idB
      used(elem.idB) = true
      matches = matches ++ Seq(elem)
    }

    var missingFst = ListBuffer.empty[Int]
    var missingSnd = mutable.Queue.empty[Int]
    for (i <- matching.indices) {
      if (matching(i) == -1) {
        missingFst += i
      }
      if (!used(i)) {
        missingSnd += i
      }
    }

//    println(s"MISSING FST ${missingFst.length}")
//    println(s"MISSING SND ${missingSnd.length}")
//
//    val distancesMap: Map[(Int, Int), DistanceWrapper] = fromListToMap(
//      distancesList)
//    for (elemFst <- missingFst) {
//      val elemSnd = missingSnd.dequeue()
//      matching(elemFst) = elemSnd
//      used(elemSnd) = true
//      println(
//        distancesMap.getOrElse((elemFst, elemSnd),
//                               DistanceWrapper(elemFst, elemSnd, 1.0)))
//      matches = matches ++ Seq(
//        distancesMap.getOrElse((elemFst, elemSnd),
//                               DistanceWrapper(elemFst, elemSnd, 1.0)))
//    }
    matches
  }
}
