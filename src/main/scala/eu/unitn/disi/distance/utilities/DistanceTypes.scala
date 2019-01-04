package eu.unitn.disi.distance.utilities

case class DistanceTypes(values: Array[Int],
                         differences: Array[Int],
                         matches: Array[Int],
                         shingles: Array[Int]) {
  override def toString: String =
    s"DistanceTypes(${values.toList}, ${differences.toList}, ${shingles.toList})"
}
