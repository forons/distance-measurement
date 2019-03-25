package eu.unitn.disi.distance

import eu.unitn.disi.db.spark.io.SparkReader
import eu.unitn.disi.distance.parser.CLParser
import eu.unitn.disi.distance.utilities.{DistanceWrapper, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class Main

object Main {

  val log: Logger = LoggerFactory.getLogger(classOf[Main])
  var dim: Long = _

  def main(args: Array[String]): Unit = {
    val config = CLParser.parse(args)
    val spark: SparkSession =
      Utils
        .builder(s"${config.fst} - ${config.snd} - ${config.metric}")
        .getOrCreate

    val fst =
      SparkReader.read(spark, config.getOptionMap, config.format, config.fst)
    val snd =
      SparkReader.read(spark, config.getOptionMap, config.format, config.snd)

    val columns = if (config.cols.isEmpty) {
      fst.columns.indices.toArray
    } else {
      config.cols
    }

    val start: Long = System.currentTimeMillis()
    var distancesTime: Long = System.currentTimeMillis()
    var collectTime: Long = 0L
    var totalTime: Long = System.currentTimeMillis()

    val dist = {
      try {
        val calculator = new Calculator(spark)
        val distances: Dataset[DistanceWrapper] =
          calculator.computeDistances(
            fst,
            snd,
            config.distanceTypes,
            columns
          )
        distances.repartition(3000).cache().repartition(3000)

        distancesTime = System.currentTimeMillis() - start
        log.info(s"Completed distances computation in ${distancesTime}ms")

        val matching =
          calculator.computeMatching(distances, config.metric)
        collectTime = calculator.collectTime
        totalTime = System.currentTimeMillis() - start

        val distance = calculator.computeDistance(matching)
        distance
      } catch {
        case e: NullPointerException =>
          log.error("The computation of the distance lead to an error")
          log.error(s"${e.getMessage}")
          1.0
        case e: UnsupportedOperationException =>
          log.error("The computation of the distance lead to an error")
          log.error(s"${e.getMessage}")
          1.0
      }
    }
    log.info(
      s"fst:${config.fst}|snd:${config.snd}|metric:${config.metric}|distance:$dist|" +
        s"distTime:$distancesTime|collectTime:$collectTime|totalTime:$totalTime|" +
        s"time:${System.currentTimeMillis() - start}"
    )
  }
}
