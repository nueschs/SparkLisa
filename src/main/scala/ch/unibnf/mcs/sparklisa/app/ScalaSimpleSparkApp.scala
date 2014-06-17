package ch.unibnf.mcs.sparklisa.app

import java.io.FileWriter
import javax.xml.bind.JAXBContext

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import scala.collection.JavaConversions

/**
 * Created by snoooze on 16.06.14.
 */
object ScalaSimpleSparkApp {

  val SumKey: String = "SUM_KEY"

//  var oldM : Double = null
//  var oldS : Double = null

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App").setMaster("local")
      .setSparkHome("/home/snoooze/spark/spark-1.0.0")
      .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    return conf
  }

  private def readXml(): Topology = {
//    val is = getClass().getClassLoader().getResourceAsStream("xml/simple_topology.xml")
//    val context = JAXBContext.newInstance(classOf[Topology])
//    val unmarshaller = context.createUnmarshaller()
//    val t: Topology = unmarshaller.unmarshal(is).asInstanceOf[Topology]
    return TopologyHelper.createSimpleTopology();
  }

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, new Duration(15000L))
    import StreamingContext._
    ssc.checkpoint(".checkpoint")
    val topology: Topology = readXml()
    val node1: NodeType = new NodeType()
    node1.setNodeId("node1")
    val node2: NodeType = new NodeType()
    node2.setNodeId("node2")
    val node3: NodeType = new NodeType()
    node3.setNodeId("node3")
    val node4: NodeType = new NodeType()
    node4.setNodeId("node4")

    node1.getNeighbour.add(node2)
    node2.getNeighbour.add(node1)
    node2.getNeighbour.add(node3)
    node3.getNeighbour.add(node2)
    node3.getNeighbour.add(node4)
    node4.getNeighbour.add(node3)


    val node1Values: DStream[(NodeType, Double)] = ssc.actorStream[(NodeType, Double)](Props(new SensorSimulatorActorReceiver(node1)), "Node1Receiver")
    val node2Values: DStream[(NodeType, Double)] = ssc.actorStream[(NodeType, Double)](Props(new SensorSimulatorActorReceiver(node2)), "Node2Receiver")
    val node3Values: DStream[(NodeType, Double)] = ssc.actorStream[(NodeType, Double)](Props(new SensorSimulatorActorReceiver(node3)), "Node3Receiver")
    val node4Values: DStream[(NodeType, Double)] = ssc.actorStream[(NodeType, Double)](Props(new SensorSimulatorActorReceiver(node4)), "Node4Receiver")

    val allValues: DStream[(NodeType, Double)] = node1Values.union(node2Values).union(node3Values).union(node4Values)
    val runningCount = allValues.count()
    val runningSum = allValues.map(value => (SumKey, value._2)).reduceByKey(_ + _).map(value => value._2)

    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1+b._1, a._2 + b._2)).map(t => t._1/t._2)

    val meanDiff = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      val mean = meanRDD.reduce(_ + _)
      valueRDD.map(value => {
        math.pow(value._2 - mean, 2.0)
      })
    })

    val stdDev = meanDiff.transformWith(runningCount, (diffRDD, countRDD: RDD[Long]) => {
      val diffSum: Double = diffRDD.reduce(_ + _)
      countRDD.map(cnt => {
        math.sqrt(diffSum / cnt.toDouble)
      })
    })

    val allLisaVals = createLisaValues(allValues, runningMean, stdDev)
    allLisaVals.print()
    var nodes = Set[NodeType]()
    allLisaVals.filter(t => {
      nodes = nodes + t._1
      node1.getNeighbour.contains(t._1)
    }).print()


    ssc.start()
    ssc.awaitTermination()

  }

  private def createLisaValues(nodeValues : DStream[(NodeType, Double)], runningMean : DStream[Double], stdDev : DStream[Double]) : DStream[(NodeType, Double)] = {
      return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD : RDD[Double]) => {
         nodeRDD.cartesian(meanRDD).map(cart => (cart._1._1, cart._1._2 - cart._2))
      }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD : RDD[Double]) => {
        nodeDiffRDD.cartesian(stdDevRDD).map(cart => (cart._1._1, cart._1._2/cart._2))
      })
  }

  private def printDStream(stream: DStream[_ <: Any]) = {
    println("eval started")
    stream.foreachRDD(rdd => {
      val cnt = rdd.count()
      val first = rdd.first()
      println("RDD has " + cnt + " values")
      println("First Element is " + first)
    })
  }

}
