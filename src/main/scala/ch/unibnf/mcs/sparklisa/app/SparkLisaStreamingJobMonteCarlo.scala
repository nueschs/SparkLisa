package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.receiver.TopologySimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
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
object SparkLisaStreamingJobMonteCarlo extends SparkLisaStreamingTrait{

  val statGen = RandomTupleGenerator

  override def main(args: Array[String]) = {
    super.main(args)
  }

  override def run_calculations(topology: Topology, nodeMap: mutable.Map[String, NodeType],
                                ssc: StreamingContext, numBaseStations: Int, rate: Double, k: Int) = {

    super.run_calculations(topology, nodeMap, ssc, numBaseStations, rate, k)
    createLisaMonteCarlo(allLisaValues, nodeMap, topology)
  }

  private def getRandomNeighbours(value: (String, Double), nodeMap: mutable.Map[String, NodeType], topology: Topology):
  mutable.MutableList[(String, (Double, List[String]))]  = {

    val randomNeighbours = statGen.createRandomNeighboursList(nodeMap.get(value._1).get.getNodeId, 1000, topology.getNode.size())
    var mapped: mutable.MutableList[(String, (Double, List[String]))] = mutable.MutableList()
    randomNeighbours.foreach(n => {
      mapped += ((value._1, (value._2, n)))
    })
    return mapped
  }

  private def createLisaMonteCarlo(allLisaValues: DStream[(String, Double)], nodeMap: mutable.Map[String,
    NodeType], topology: Topology) = {
    val numberOfBaseStations: Int = topology.getBasestation.size()
    val numberOfNodes: Int = topology.getNode.size()
    import org.apache.spark.streaming.StreamingContext._

    val lisaValuesWithRandomNeighbourIds: DStream[(String, (Double, List[String]))] = allLisaValues
      .flatMap(value => getRandomNeighbours(value, nodeMap, topology))

    val lisaValuesWithRandomNeighbourLisaValues: DStream[((String, Double), (String,(Double, List[String])))] =
      lisaValuesWithRandomNeighbourIds.transformWith(allLisaValues, (neighbourRDD, lisaRDD: RDD[(String,
      Double)]) => lisaRDD.cartesian(neighbourRDD))
      .filter(value => value._2._2._2.contains(value._1._1))

    val randomNeighbourSums: DStream[((String, List[String]), Double)] = lisaValuesWithRandomNeighbourLisaValues
      .map(t => ((t._2._1, t._2._2._2), t._1._2))
      .groupByKey()
      .map(t => (t._1, t._2.sum / t._2.size.toDouble))

    val randomLisaValues: DStream[(String, Double)] = randomNeighbourSums.map(t => (t._1._1, t._2))
      .join(allLisaValues)
      .map(t => (t._1, t._2._2*t._2._1))

    val measuredValuesPositions = randomLisaValues.groupByKey()
      .join(allLisaValues)
      .map(t => {
        (t._1, (t._2._1.filter(_ < t._2._2).size.toDouble/t._2._1.size.toDouble))
      })

//    lisaValuesWithRandomNeighbourIds.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/lisaValuesWithRandomNeighbourIds")
//    lisaValuesWithRandomNeighbourLisaValues.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/lisaValuesWithRandomNeighbourLisaValues")
//    randomNeighbourSums.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/randomNeighbourSums")
//    randomLisaValues.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/randomLisaValues")
    measuredValuesPositions.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/measuredValuesPositions")
  }
}
