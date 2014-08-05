package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.redisson.Redisson

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by snoooze on 16.06.14.
 */
object ScalaSimpleSparkAppEval {

  val SumKey: String = "SUM_KEY"

  val Master: String = "spark://saight02:7077"
//  val Master: String = "local[2]"

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App").setMaster(Master)
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
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(10))
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

    val nodeMap : mutable.Map[String, NodeType] = mutable.Map()
    nodeMap += (node1.getNodeId -> node1)
    nodeMap += (node2.getNodeId -> node2)
    nodeMap += (node3.getNodeId -> node3)
    nodeMap += (node4.getNodeId -> node4)

    node1.getNeighbour.add(node2.getNodeId)
    node2.getNeighbour.add(node1.getNodeId)
    node2.getNeighbour.add(node3.getNodeId)
    node3.getNeighbour.add(node2.getNodeId)
    node3.getNeighbour.add(node4.getNodeId)
    node4.getNeighbour.add(node3.getNodeId)


    val node1Values: DStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiver(node1)), "Node1Receiver")
    val node2Values: DStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiver(node2)), "Node2Receiver")
    val node3Values: DStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiver(node3)), "Node3Receiver")
    val node4Values: DStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiver(node4)), "Node4Receiver")

    val allValues: DStream[(String, Double)] = node1Values.union(node2Values).union(node3Values).union(node4Values)

    val runningCount = allValues.count()


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

    val allNeighbourVals : DStream[(String, Double)] = allLisaVals.flatMap(value => mapToNeighbourKeys(value, nodeMap))
    val neighboursNormalizedSums = allNeighbourVals.map(value => (value._1, (1.0, value._2))).reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).map(value => (value._1, value._2._2/value._2._1))

    val finalLisaValues = allLisaVals.join(neighboursNormalizedSums).map(value => (value._1, value._2._1 * value._2._2))

//    allValues.print()
    finalLisaValues.print()

    storeStringPairDStream(allValues, "allValues")
    storeLongDStream(allLisaVals.count(), "allLisaCnt")
    storeLongDStream(neighboursNormalizedSums.count(), "sums")

    storeStringPairDStream(allLisaVals, "allLisaVals")
    storeStringPairDStream(neighboursNormalizedSums, "neighboursNormalizedSums")


    ssc.start()
    ssc.awaitTermination()

  }

  private def mapToNeighbourKeys(value : (String, Double), nodeMap : mutable.Map[String, NodeType]) : mutable.Traversable[(String, Double)] = {
    var mapped : mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for(n <- nodeMap.get(value._1.split("_")(0)).get.getNeighbour()) {
      mapped += ((n+"_"+value._1.split("_")(1), value._2))
    }
    return mapped
  }

  private def storeDoubleDStream(stream : DStream[Double], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, timestamp.toString, value.toString)
      })
    })
  }

  private def storeLongDStream(stream : DStream[Long], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, timestamp.toString, value.toString)
      })
    })
  }

  private def storeNodePairDStream(stream : DStream[(NodeType, Double)], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, value._1.getNodeId+"_"+timestamp, value._2.toString)
      })
    })
  }

  private def storeStringPairDStream(stream : DStream[(String, Double)], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, value._1+"_"+timestamp, value._2.toString)
      })
    })
  }

  /*
  * returns a DStream[(NodeType, Double)]
   */
  private def createLisaValues(nodeValues : DStream[(String, Double)], runningMean : DStream[Double], stdDev : DStream[Double]) : DStream[(String, Double)] = {
      return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD : RDD[Double]) => {
         nodeRDD.cartesian(meanRDD).map(cart => (cart._1._1, cart._1._2 - cart._2))
      }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD : RDD[Double]) => {
        nodeDiffRDD.cartesian(stdDevRDD).map(cart => (cart._1._1, cart._1._2/cart._2))
      })
  }

  private def storeKeyValueMap(mapkey: String, key : String, value : String) = {
    val redisson : Redisson = Redisson.create()
    redisson.getMap(mapkey).put(key, value)
    redisson.shutdown()
  }

}
