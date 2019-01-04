package eu.unitn.disi.distance.utilities

import java.io.File
import java.util.logging.{Level, Logger}

import org.apache.spark.sql._

import scala.collection.mutable

object Utils {

  val appName = "DistanceCalculation"

  def builder(name: String): SparkSession.Builder = {
    builder.appName(s"$appName - $name")
  }

  def builder: SparkSession.Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger
      .getLogger("org.apache.spark.storage.BlockManager")
      .setLevel(Level.OFF)
    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.rpc.askTimeout", "1200s")
      .config("spark.network.timeout", "1200s")
      .config("spark.sql.broadcastTimeout", "1200")
      .config("spark.driver.maxResultSize", "0")
      .config("dfs.client.block.write.replace-datanode-on-failure.policy",
              "ALWAYS")
      .config("dfs.client.block.write.replace-datanode-on-failure.best-effort",
              "true")
      .config("spark.ui.showConsoleProgress", "false")

    if (new File("/Users/forons/").exists()) {
      builder.config("spark.driver.host", "localhost").master("local[*]")
    } else {
      builder
    }
  }

  def handleColumns(df: Dataset[Row],
                    columns: Array[Int],
                    idColumns: Array[Int]): Array[Int] = {
    if (idColumns.isEmpty) {
      if (columns.isEmpty) {
        (for (i <- df.columns.indices) yield i).toArray
      } else {
        columns
      }
    } else {
      if (columns.isEmpty) {
        (for (i <- df.columns.indices if !idColumns.contains(i))
          yield i).toArray
      } else {
        columns.filterNot(idColumns.contains(_))
      }
    }
  }

  def keepColumnsByIndex(columns: Array[String],
                         ints: Array[Int]): mutable.Buffer[Column] = {
    var cols: mutable.Buffer[Column] = mutable.Buffer.empty
    for (index <- ints) {
      cols += functions.col(columns(index))
    }
    cols
  }
}
