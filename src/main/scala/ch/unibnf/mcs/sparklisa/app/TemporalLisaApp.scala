package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.receiver.TemporalTopologySimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object TemporalLisaApp extends LisaDStreamFunctions with LisaAppConfiguration{

//  val Master: String = "spark://saight02:7077"
  val Master: String = "local[4]"

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()

    val batchDuration: Int = args(0).toInt
    val rate: Double = args(1).toDouble
    val numBaseStations: Int = args(2).toInt
    val timeout: Int = args(3).toInt
    val topologyPath: String = args(4)
    val k: Int = args(5).toInt

    import org.apache.spark.streaming.StreamingContext._
    val conf: SparkConf = createSparkConf()
    conf.set("spark.default.parallelism", s"$numBaseStations")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchDuration))
    ssc.addStreamingListener(new LisaStreamingListener())
    val topology: Topology = TopologyHelper.topologyFromBareFile(topologyPath, numBaseStations)
    val tempMap: mutable.Map[Integer, NodeType] = TopologyHelper.createNumericalNodeMap(topology).asScala
    val nodeMap: mutable.Map[Int, NodeType] = for ((k,v) <- tempMap; (nk,nv) = (k.intValue, v)) yield (nk,nv)


    val allValues: DStream[(Int, Array[Double])] = createAllValues(ssc, topology, numBaseStations, k, rate)
    allValues.repartition(numBaseStations)

    /*
    * The array of values emitted from the receiver contains the current as well as k past values.
    * For the calculation of the temporal LISA, current values need to separated from the past values.
    */
    val currentValues: DStream[(Int, Double)] = allValues.map(t => (t._1, t._2(0)))
    val pastValues: DStream[(Int, Array[Double])] = allValues.map(t => (t._1, t._2.takeRight(t._2.size-1)))
    val runningCount: DStream[Long] = currentValues.count()
    val runningMean: DStream[Double] = currentValues.map(t => (t._2, 1.0)).reduce((a, b) =>
      (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)
    val currentStdDev = createStandardDev(currentValues, runningCount, runningMean)

    val allStandardisedValues = createStandardisedValues(currentValues, runningMean, currentStdDev)

    val allNeighbourValues: DStream[(Int, Double)] = allStandardisedValues.flatMap(t => mapToNeighbourKeys[Double](t, nodeMap))

    val allPastStandardisedValues: DStream[(Int, Array[Double])] = createPastStandardisedValues(pastValues)
    val pastNeighbourValues: DStream[(Int, Array[Double])] = allPastStandardisedValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))

    // include past values of a node as "neighbour"
    val allPastNeighbouringValues: DStream[(Int, Double)] = allPastStandardisedValues.join(pastNeighbourValues)
      .flatMapValues(t => t._1 ++ t._2)
    /*
    * With current and past neighbour values, as well as past values from the node itself, the average of
    * these values (right part in the LISA formula) can be calculated.
    */
    val neighboursStandardisedAverages = allNeighbourValues.union(allPastNeighbouringValues).groupByKey()
      .map(t => (t._1, t._2.sum / t._2.size.toDouble))

    val finalLisaValues = allStandardisedValues.join(neighboursStandardisedAverages).map(t => (t._1, t._2._1 * t._2._2))
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues
      .flatMapValues(a => a.toList.zipWithIndex.map(t => ("k-"+t._2.toString, t._1)))
      .saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")


    ssc.start()
    ssc.awaitTermination(timeout*1000)

  }

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, k: Int,
                              rate: Double): DStream[(Int, Array[Double])] = {
    val values: DStream[(Int, Array[Double])] = ssc.actorStream[(Int, Array[Double])](
      Props(classOf[TemporalTopologySimulatorActorReceiver],topology.getNode.toList, rate, k), "receiver")
    return values
  }
}
