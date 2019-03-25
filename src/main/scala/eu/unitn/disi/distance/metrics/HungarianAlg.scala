package eu.unitn.disi.distance.metrics

import eu.unitn.disi.distance.utilities.DistanceWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import util.control.Breaks._

class HungarianAlg(spark: SparkSession,
                   distances: Dataset[DistanceWrapper],
                   dim: Int)
    extends IAlg(spark: SparkSession,
                 distances: Dataset[DistanceWrapper],
                 dim: Int) {

  var labelByWorker: Array[Double] = Array.ofDim[Double](dim)
  var labelByJob: Array[Double] = Array.ofDim[Double](dim)
  var minSlackWorkerByJob: Array[Int] = Array.ofDim[Int](dim)
  var minSlackValueByJob: Array[Double] = Array.ofDim[Double](dim)
  var matchJobByWorker: Array[Int] = Array.ofDim[Int](dim)
  var matchWorkerByJob: Array[Int] = Array.ofDim[Int](dim)
  var parentWorkerByCommittedJob: Array[Int] = Array.ofDim[Int](dim)
  var committedWorkers: Array[Boolean] = Array.ofDim[Boolean](dim)
  var distancesMap: Map[(Int, Int), DistanceWrapper] = _
  var updatedDistances: mutable.Map[(Int, Int), DistanceWrapper] = _
  var assigned: Set[(Int, Int)] = Set.empty[(Int, Int)]
  var assignedWorker: Set[Int] = Set.empty[Int]
  var assignedJob: Set[Int] = Set.empty[Int]

  override def computeMatching(): Array[DistanceWrapper] = {
    val start = System.currentTimeMillis()
    matchJobByWorker = Array.fill[Int](dim)(-1)
    matchWorkerByJob = Array.fill[Int](dim)(-1)

    val zeros = distances.filter(col("distance") === 0.0).collect()
    for (elem <- zeros) {
      assigned = assigned ++ Seq((elem.idA, elem.idB))
      assignedWorker = assignedWorker ++ Seq(elem.idA)
      assignedJob = assignedJob ++ Seq(elem.idB)
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
H
    distancesMap = fromListToMap(distancesList)
    updatedDistances = mutable.Map(distancesMap.toSeq: _*)

    /*
     * Heuristics to improve performance: Reduce rows and columns by their smallest element, compute
     * an initial non-zero dual feasible solution and create a greedy matching from workers to jobs
     * of the cost matrix.
     */
    reduce()
    computeInitialFeasibleSolution()
    greedyMatch()

    var w = fetchUnmatchedWorker()
    breakable {
      while (w < dim) {
        if (w < 0) {
          break
        }
        if (!assignedWorker.contains(w)) {
          initializePhase(w)
          executePhase()
          w = fetchUnmatchedWorker()
        }
      }
    }

    for (idx <- matchJobByWorker.indices if !assignedWorker.contains(idx)) {
      if (!assignedJob.contains(matchJobByWorker(idx))) {
        matches = matches ++ Seq(
          distancesMap.getOrElse(
            (idx, matchJobByWorker(idx)),
            DistanceWrapper(idx, matchJobByWorker(idx), 1.0)))
      }
    }
    matches
  }

  def computeInitialFeasibleSolution(): Unit = {
    for (j <- 0 until dim if !assignedJob.contains(j)) {
      labelByJob(j) = Double.PositiveInfinity
    }
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        if (dist < labelByJob(j)) {
          labelByJob(j) = dist
        }
      }
    }
  }

  def fetchUnmatchedWorker(): Int = {
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      if (matchJobByWorker(w) == -1) {
        return w
      }
    }
    -1
  }

  def executePhase(): Unit = {
    breakable {
      while (true) {
        var minSlackWorker = -1
        var minSlackJob = -1
        var minSlackValue = Double.PositiveInfinity
        for (j <- 0 until dim if !assignedJob.contains(j)) {
          if (parentWorkerByCommittedJob(j) == -1 && minSlackValueByJob(j) < minSlackValue) {
            minSlackValue = minSlackValueByJob(j)
            minSlackWorker = minSlackWorkerByJob(j)
            minSlackJob = j
          }
        }
        if (minSlackValue > 0) {
          updateLabeling(minSlackValue)
        }
        parentWorkerByCommittedJob(minSlackJob) = minSlackWorker
        if (matchWorkerByJob(minSlackJob) == -1) {
          /*
           * An augmenting path has been found.
           */
          var committedJob = minSlackJob
          var parentWorker = parentWorkerByCommittedJob(committedJob)
          while (true) {
            val temp = matchJobByWorker(parentWorker)
            matchTuple(parentWorker, committedJob)
            committedJob = temp
            if (committedJob == -1) break
            parentWorker = parentWorkerByCommittedJob(committedJob)
          }
          return
        } else {
          /*
           * Update slack values since we increased the size of the committed workers set.
           */
          val worker = matchWorkerByJob(minSlackJob)
          committedWorkers(worker) = true
          for (j <- 0 until dim if !assignedJob.contains(j)) {
            if (parentWorkerByCommittedJob(j) == -1) {
              val slack = updatedDistances
                .getOrElse((worker, j), DistanceWrapper(worker, j, 1.0))
                .distance - labelByWorker(worker) - labelByJob(j)
              if (minSlackValueByJob(j) > slack) {
                minSlackValueByJob(j) = slack
                minSlackWorkerByJob(j) = worker
              }
            }
          }

        }
      }
    }
  }

  def greedyMatch(): Unit = {
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        if (matchJobByWorker(w) == -1 && matchWorkerByJob(j) == -1 && dist - labelByWorker(
              w) - labelByJob(j) == 0) {
          matchTuple(w, j)
        }
      }
    }
  }

  def initializePhase(w: Int): Unit = {
    committedWorkers = Array.fill[Boolean](dim)(false)
    parentWorkerByCommittedJob = Array.fill[Int](dim)(-1)
    committedWorkers(w) = true
    for (j <- 0 until dim if !assignedJob.contains(j)) {
      minSlackValueByJob(j) = updatedDistances
        .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
        .distance - labelByWorker(w) - labelByJob(j)
      minSlackWorkerByJob(j) = w
    }
  }

  def matchTuple(w: Int, j: Int): Unit = {
    matchJobByWorker(w) = j
    matchWorkerByJob(j) = w
  }

  def reduce(): Unit = {
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      var min: Double = Double.PositiveInfinity
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist: Double =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        if (dist < min) {
          min = dist
        }
      }
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist: Double =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        updatedDistances((w, j)) = DistanceWrapper(w, j, dist - min)
      }
    }
    val min: Array[Double] = Array.ofDim[Double](dim)
    for (j <- 0 until dim if !assignedJob.contains(j)) {
      min(j) = Double.PositiveInfinity
    }
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist: Double =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        if (dist < min(j)) {
          min(j) = dist
        }
      }
    }
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      for (j <- 0 until dim if !assignedJob.contains(j)) {
        val dist: Double =
          updatedDistances
            .getOrElse((w, j), DistanceWrapper(w, j, 1.0))
            .distance
        updatedDistances((w, j)) = DistanceWrapper(w, j, dist - min(j))
      }
    }
  }

  def updateLabeling(slack: Double): Unit = {
    for (w <- 0 until dim if !assignedWorker.contains(w)) {
      if (committedWorkers(w)) {
        labelByWorker(w) = labelByWorker(w) + slack
      }
    }
    for (j <- 0 until dim if !assignedJob.contains(j)) {
      if (parentWorkerByCommittedJob(j) != -1) {
        labelByJob(j) = labelByJob(j) - slack
      } else {
        minSlackValueByJob(j) = minSlackValueByJob(j) - slack
      }
    }
  }
}
