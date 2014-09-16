package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.receiver.{TimeBasedTopologySimulatorActorReceiver, TopologySimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


import scala.collection.mutable

/**
 * Created by Stefan Nüesch on 16.06.14.
 */
object SparkLisaTimeBasedStreamingJob extends SparkLisaApp {

  override def run_calculations(topology: Topology, nodeMap: mutable.Map[String, NodeType],
                                ssc: StreamingContext, numBaseStations: Int, rate: Double, k: Int) = {
    import org.apache.spark.streaming.StreamingContext._

    val allValues: DStream[(String, Array[Double])] = createAllValues(ssc, topology, numBaseStations, k, rate)

    val currentValues: DStream[(String, Double)] = allValues.map(t => (t._1, t._2(0)))
    val pastValues: DStream[(String, Array[Double])] = allValues.map(t => (t._1, t._2.takeRight(t._2.size-1)))
    val pastValuesFlat: DStream[(String, Double)] = pastValues.flatMapValues(a => a.toList)
    val allValuesFlat: DStream[(String, Double)] = currentValues.union(pastValuesFlat)
    val runningCount: DStream[Long] = allValuesFlat.count()
    val runningMean: DStream[Double] = allValuesFlat.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)

    val variance = allValuesFlat.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      val mean = meanRDD.reduce(_ + _)
      valueRDD.map(t => {
        math.pow(t._2 - mean, 2.0)
      })
    })

    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      val variance: Double = varianceRDD.reduce(_ + _)
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })

    val allLisaValues = createLisaValues(currentValues, runningMean, stdDev)

    val allNeighbourValues: DStream[(String, Double)] = allLisaValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))

    val allPastLisaValues: DStream[(String, Double)] = createLisaValues(pastValuesFlat, runningMean, stdDev)
    val neighboursNormalizedSums = allNeighbourValues.union(allPastLisaValues).groupByKey()
      .map(t => (t._1, t._2.sum / t._2.size.toDouble))

    val finalLisaValues = allLisaValues.join(neighboursNormalizedSums).map(t => (t._1, t._2._1 * t._2._2))
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues
      .flatMapValues(a => a.toList.zipWithIndex.map(t => ("k-"+t._2.toString, t._1)))
      .map(t => ((t._1, t._2._1), t._2._2))
      .saveAsTextFiles(HdfsPath + "/results/timedMappedValues")
    allValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")

}

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, k: Int, rate: Double): DStream[(String, Array[Double])] = {
    val nodesPerBase = topology.getNode.size() / numBaseStations
    var values: DStream[(String, Array[Double])] = null
    for (i <- 0 until numBaseStations) {
      if (values == null) {
        values = ssc.actorStream[(String, Array[Double])](Props(classOf[TimeBasedTopologySimulatorActorReceiver],
          topology.getNode.toList.slice(i * nodesPerBase, (i + 1) * nodesPerBase), rate, k), "receiver")
      } else {
        values = values.union(ssc.actorStream[(String, Array[Double])]
          (Props(classOf[TimeBasedTopologySimulatorActorReceiver],
            topology.getNode.toList.slice(i * nodesPerBase, (i + 1) * nodesPerBase), rate, k), "receiver"))
      }
    }
    return values
  }
}
