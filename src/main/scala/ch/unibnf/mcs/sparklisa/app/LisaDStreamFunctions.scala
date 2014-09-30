package ch.unibnf.mcs.sparklisa.app

import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

trait LisaDStreamFunctions {

  /**
   * Calculates, for each k, the standardized values supplied by pastValues
   *
   */
  def createPastLisaValues(pastValues: DStream[(Int, Array[Double])]) = {
    import org.apache.spark.streaming.StreamingContext._
    import org.apache.spark.SparkContext._
    val allValuesMappedPerK: DStream[(Int, Map[Int, Double])] = pastValues.mapValues(a => a.zipWithIndex.map(t => t.swap).toMap)
    val valuesByK: DStream[(Int, Double)] = allValuesMappedPerK.flatMap(t => (t._2).toList)
    val meanByK: DStream[(Int, Double)] = valuesByK.groupByKey().mapValues(i => i.sum / i.size.toDouble)
    val stdevByK: DStream[(Int, Double)] = valuesByK.join(meanByK).mapValues(t => math.pow(t._1-t._2, 2.0))
      .groupByKey().mapValues(i => math.sqrt(i.sum / i.size.toDouble))
    pastValues.transformWith(meanByK, (valRDD, meanRDD: RDD[(Int, Double)]) => {
      val meansByK: collection.Map[Int, Double] = meanRDD.collectAsMap()
      valRDD.mapValues(a => a.zipWithIndex.map(t=>t.swap)).mapValues(a => for (v <- a) yield v._2 - meansByK(v._1))
    }).transformWith(stdevByK, (valRDD, stdevRDD: RDD[(Int, Double)]) => {
      val stdevsByK: collection.Map[Int, Double] = stdevRDD.collectAsMap()
      valRDD.mapValues(a => a.zipWithIndex.map(t => t.swap)).mapValues(a => for (v <- a) yield v._2/stdevsByK(v._1))
    })
  }

  def createLisaValues(nodeValues: DStream[(Int, Double)], runningMean: DStream[Double], stdDev: DStream[Double]): DStream[(Int, Double)] = {
    import org.apache.spark.SparkContext._
    nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      var mean_ = 0.0
      try {mean_ = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      nodeRDD.mapValues(d => d-mean_)
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      var stdDev_ = 0.0
      try {stdDev_ = stdDevRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      nodeDiffRDD.mapValues(d => d/stdDev_)
    })
  }

  def createStandardDev(values: DStream[(Int, Double)],  countStream:DStream[Long],
                        meanStream: DStream[Double]):  DStream[Double] = {

    val variance = values.transformWith(meanStream, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0
      try {mean = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      valueRDD.map(t => {
        math.pow(t._2 - mean, 2.0)
      })
    })

    variance.transformWith(countStream, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0
      try{variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })
  }


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
