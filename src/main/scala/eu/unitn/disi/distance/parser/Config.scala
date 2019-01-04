package eu.unitn.disi.distance.parser

import eu.unitn.disi.db.spark.io.Format
import eu.unitn.disi.distance.metrics.MetricType
import eu.unitn.disi.distance.utilities.DistanceTypes

case class Config(fst: String,
                  snd: String,
                  format: Format = Format.CSV,
                  nullValues: String = "",
                  quote: String = "\"",
                  hasHeader: Boolean = true,
                  inferSchema: Boolean = true,
                  metric: MetricType,
                  distanceTypes: DistanceTypes,
                  cols: Array[Int]) {

  def getOptionMap: Map[String, String] = {
    Map[String, String]("inferSchema" -> inferSchema.toString,
                        "header" -> hasHeader.toString,
                        "quote" -> quote,
                        "nullValue" -> nullValues)
  }

  override def toString: String =
    s"Config($fst, $snd, $format, $nullValues, $quote, $hasHeader, $metric, $distanceTypes, ${cols.toList}"
}
