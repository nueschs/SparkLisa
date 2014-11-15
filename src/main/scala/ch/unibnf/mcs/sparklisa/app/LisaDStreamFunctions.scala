package ch.unibnf.mcs.sparklisa.app

import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
 * Functions used in multiple differentd algorithms
 */
trait LisaDStreamFunctions {

  /**
   * For each past "network state" k, standardises values in the form (v-m)/S
   * where v is the value, m is the mean and S is the standard deviation.
   *
   * @return DStream containing at each node key an array of standardised past values
   */
  def createPastStandardisedValues(pastValues: DStream[(Int, Array[Double])]) = {
    import org.apache.spark.streaming.StreamingContext._
    import org.apache.spark.SparkContext._
    // map each value to its position position in the array, which represents k
    val allValuesMappedPerK: DStream[(Int, Map[Int, Double])] = pastValues.mapValues(a => a.zipWithIndex.map(t => t.swap).toMap)
    // the node key is dismissed for mean and standard deviation
    val valuesByK: DStream[(Int, Double)] = allValuesMappedPerK.flatMap(t => (t._2).toList)
    val meanByK: DStream[(Int, Double)] = valuesByK.groupByKey().mapValues(i => i.sum / i.size.toDouble)
    val stdevByK: DStream[(Int, Double)] = valuesByK.join(meanByK).mapValues(t => math.pow(t._1-t._2, 2.0))
      .groupByKey().mapValues(i => math.sqrt(i.sum / i.size.toDouble))
    pastValues.transformWith(meanByK, (valRDD, meanRDD: RDD[(Int, Double)]) => {
      val meansByK: collection.Map[Int, Double] = meanRDD.collectAsMap()
      // value -> (value-mean)
      valRDD.mapValues(a => a.zipWithIndex.map(t=>t.swap)).mapValues(a => for (v <- a) yield v._2 - meansByK(v._1))
    }).transformWith(stdevByK, (valRDD, stdevRDD: RDD[(Int, Double)]) => {
      val stdevsByK: collection.Map[Int, Double] = stdevRDD.collectAsMap()
      // (value-mean) -> (value-mean) / standard deviation
      valRDD.mapValues(a => a.zipWithIndex.map(t => t.swap)).mapValues(a => for (v <- a) yield v._2/stdevsByK(v._1))
    })
  }

  /**
   * Standardise values (name is misleading)
   *
   * value -> (value-mean)/standard deviation
   *
   * @param nodeValues
   * @param runningMean
   * @param stdDev
   * @return
   */
  def createStandardisedValues(nodeValues: DStream[(Int, Double)], runningMean: DStream[Double], stdDev: DStream[Double]): DStream[(Int, Double)] = {
    import org.apache.spark.SparkContext._
    nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      var mean_ = 0.0

      /*
       * DStream contains only one value, using reduce to access it.
       * UnsupportedOperationException is thrown in case of an empty collection.
       * This may happen at application startup, and would terminate job execution if not handled.
       */
      try {mean_ = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      nodeRDD.mapValues(d => d-mean_)
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      var stdDev_ = 0.0

      /*
       * DStream contains only one value, using reduce to access it.
       * UnsupportedOperationException is thrown in case of an empty collection.
       * This may happen at application startup, and would terminate job execution if not handled.
       */
      try {stdDev_ = stdDevRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      nodeDiffRDD.mapValues(d => d/stdDev_)
    })
  }

  /**
   * Standard deviation: sqrt( sum( (value-mean)² ) / count)
   * @param values
   * @param countStream
   * @param meanStream
   * @return
   */
  def createStandardDev(values: DStream[(Int, Double)],  countStream:DStream[Long],
                        meanStream: DStream[Double]):  DStream[Double] = {

    val variance = values.transformWith(meanStream, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0

      /*
       * DStream contains only one value, using reduce to access it.
       * UnsupportedOperationException is thrown in case of an empty collection.
       * This may happen at application startup, and would terminate job execution if not handled.
       */
      try {mean = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      valueRDD.map(t => {
        math.pow(t._2 - mean, 2.0)
      })
    })

    variance.transformWith(countStream, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0

      /*
       * DStream contains only one value, using reduce to access it.
       * UnsupportedOperationException is thrown in case of an empty collection.
       * This may happen at application startup, and would terminate job execution if not handled.
       */
      try{variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })
  }

  /**
   * Retrieve all neighbours' keys for the node which sent this value, and map the value to all of them.
   * Thereby, groupByKey(key) yields all neighbour values for a key (using for calculating their average)
   *
   * @param value
   * @param nodeMap
   * @tparam T
   * @return
   */
  def mapToNeighbourKeys[T](value: (Int, T), nodeMap: mutable.Map[Int, NodeType]):
      mutable.Traversable[(Int, T)] = {
    var mapped: mutable.MutableList[(Int, T)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (n <- nodeMap.getOrElse(value._1, new NodeType()).getNeighbour) {
      mapped += ((n.substring(4).toInt, value._2))
    }
    return mapped
  }
}
