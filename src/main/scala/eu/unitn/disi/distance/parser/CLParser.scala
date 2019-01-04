package eu.unitn.disi.distance.parser

import eu.unitn.disi.db.spark.io.Format
import eu.unitn.disi.distance.metrics.MetricType
import eu.unitn.disi.distance.utilities.DistanceTypes
import org.apache.commons.cli.{GnuParser, Option, Options}

object CLParser {

  def parse(args: Array[String]): Config = {
    val options = buildOptions()
    val parser = new GnuParser
    val cmd = parser.parse(options, args)
    var fst: String = null
    var snd: String = null
    var format: Format = Format.CSV
    var header = false
    var nullValues = ""
    var quote = "\""
    var metric: MetricType = null
    var values: Array[Int] = Array.emptyIntArray
    var differences: Array[Int] = Array.emptyIntArray
    var shingles: Array[Int] = Array.emptyIntArray
    var matches: Array[Int] = Array.emptyIntArray
    var cols: Array[Int] = Array.emptyIntArray

    for (opt <- cmd.getOptions) {
      val value = if (opt.getValue != null) {
        opt.getValue.trim
      } else null
      opt.getLongOpt match {
        case "first-dataset"  => fst = value
        case "second-dataset" => snd = value
        case "input-format" =>
          format = Format.valueOf(value.toUpperCase.replace(".", ""))
        case "header"      => header = true
        case "quote"       => quote = value
        case "null"        => nullValues = value
        case "metric"      => metric = MetricType.valueOf(value.toUpperCase)
        case "columns"     => cols = opt.getValues.map(_.trim.toInt)
        case "values"      => values = opt.getValues.map(_.trim.toInt)
        case "differences" => differences = opt.getValues.map(_.trim.toInt)
        case "shingles"    => shingles = opt.getValues.map(_.trim.toInt)
        case "matches"     => matches = opt.getValues.map(_.trim.toInt)
      }
    }
    Config(
      fst = fst,
      snd = snd,
      format = format,
      nullValues = nullValues,
      quote = quote,
      hasHeader = header,
      metric = metric,
      distanceTypes = DistanceTypes(values, differences, matches, shingles),
      cols = cols
    )
  }

  def buildOptions(): Options = {
    val options: Options = new Options()
    options.addOption(new Option("f", "first-dataset", true, "First"))
    options.addOption(new Option("s", "second-dataset", true, "Second"))
    options.addOption(new Option("if", "input-format", true, "Input Format"))
    options.addOption(new Option("header", "header", false, "Has header?"))
    options.addOption(new Option("quote", "quote", true, "Quote handle"))
    options.addOption(new Option("null", "null", true, "Null values handle"))
    options.addOption(new Option("m", "metric", true, "Metric"))
    val colsOption = new Option("col", "The considered columns (omitted = all)")
    colsOption.setLongOpt("columns")
    colsOption.setArgs(Option.UNLIMITED_VALUES)
    colsOption.setValueSeparator(' ')
    options.addOption(colsOption)
    val valuesOption = new Option("val", "The distance is by value")
    valuesOption.setLongOpt("values")
    valuesOption.setArgs(Option.UNLIMITED_VALUES)
    valuesOption.setValueSeparator(' ')
    options.addOption(valuesOption)
    val differencesOption = new Option("diff", "The distance is by difference")
    differencesOption.setLongOpt("differences")
    differencesOption.setArgs(Option.UNLIMITED_VALUES)
    differencesOption.setValueSeparator(' ')
    options.addOption(differencesOption)
    val matchesOption = new Option(
      "mat",
      "The distance is by value, but first performs a matching")
    matchesOption.setLongOpt("matches")
    matchesOption.setArgs(Option.UNLIMITED_VALUES)
    matchesOption.setValueSeparator(' ')
    options.addOption(matchesOption)
    val shinglesOption = new Option("shing", "The distance is by shingle")
    shinglesOption.setLongOpt("shingles")
    shinglesOption.setArgs(Option.UNLIMITED_VALUES)
    shinglesOption.setValueSeparator(' ')
    options.addOption(shinglesOption)
    options
  }
}
