package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.receiver.TopologySimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by Stefan Nüesch on 16.06.14.
 */
trait SparkLisaStreamingTrait extends SparkLisaApp {

  var allLisaValues: DStream[(String, Double)] = null

  override def run_calculations(topology: Topology, nodeMap: mutable.Map[String, NodeType],
                                ssc: StreamingContext, numBaseStations: Int, rate: Double, k: Int) = {
    import org.apache.spark.streaming.StreamingContext._

    val allValues: DStream[(String, Double)] = createAllValues(ssc, topology, numBaseStations, rate)

    val runningCount = allValues.count()

    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)

    val variance = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0
      try { mean = meanRDD.reduce(_ + _) } catch {
        case use: UnsupportedOperationException => {}
      }
      valueRDD.map(value => {
        math.pow(value._2 - mean, 2.0)
      })
    })


    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0
      try { variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })

    allLisaValues = createLisaValues(allValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(String, Double)] = allLisaValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))
    val neighboursNormalizedSums = allNeighbourValues.groupByKey().map(t => (t._1, t._2.sum / t._2.size.toDouble))
    val finalLisaValues = allLisaValues.join(neighboursNormalizedSums).map(t => (t._1, t._2._1 * t._2._2))
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    runningCount.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allCount")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")
    finalLisaValues.count().saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalCount")
  }

  def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, rate: Double): DStream[(String, Double)] = {
    val nodesPerBase = topology.getNode.size()/numBaseStations
    var values: DStream[(String, Double)] = null
//    for (i <- 0 until numBaseStations){
//      if (values == null){
    values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList, rate), "receiver")
//    values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), rate), "receiver")
//      } else {
//        values = values.union(ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), rate), "receiver"))
//      }
//    }
    values.repartition(numBaseStations)
    return values
  }
}

object SparkLisaStreamingJob extends SparkLisaStreamingTrait {
  override def main(args: Array[String]) = {
    super.main(args)
  }
}
