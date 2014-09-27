package ch.unibnf.mcs.sparklisa.app

import java.io.File
import java.util.Properties

import akka.actor._
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.{TimeBasedTopologySimulatorActorReceiver, RandomTupleReceiver,
TopologySimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
import ch.unibnf.mcs.sparklisa.topology.{BasestationType, Topology, NodeType}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

import scala.collection
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.nio.file.{StandardOpenOption, OpenOption, Paths, Files}
import java.nio.charset.StandardCharsets

object TestApp {

//  val Master: String = "local[4]"
  val Master: String = "spark://saight02:7077"
  var gt: Thread = null

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]
  var statGen = RandomTupleGenerator

  def main(args: Array[String]){
    initConfig()
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(10))
//    val topology = TopologyHelper.createSimpleTopology()
    val topology = TopologyHelper.topologyFromBareFile(args(1), 16)
    val numBaseStations = args(2).toInt
    val nodesPerBase = topology.getNode.size() / numBaseStations
    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala
    val k = 5

    val values: DStream[(Int, Array[Double])] = ssc.actorStream[(Int, Array[Double])](Props(classOf[TimeBasedTopologySimulatorActorReceiver], topology.getNode.toList, 6.0, k), "receiver1")
    val randomNeighbourTuples = ssc.actorStream[(String, List[List[String]])](Props(classOf[RandomTupleReceiver], topology.getNode.toList, 0.01, 10), "receiver2")

    values.foreachRDD(rdd => rdd.foreach(value => {
      if (value._2.size == 5){
        Files.write(Paths.get("/home/snoooze/msctr/testAppOutput/values.txt"), (value._1.toString+"; "+value._2.mkString(";")+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
      }
    }))

    values.mapValues(a => a.zipWithIndex.map(t => t.swap)).flatMapValues(a => a).map(t => ((t._1, t._2._1), t._2._2))
      .foreachRDD(rdd =>
      if (rdd.count() == topology.getNode.size()*k){
        rdd.foreach(value => {
          Files.write(Paths.get("/home/snoooze/msctr/testAppOutput/mapped.txt"), (value.toString()+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }

  private def getRandomNeighbours(value: (String, Double), nodeMap: mutable.Map[String, NodeType], topology: Topology):
  mutable.MutableList[(String, List[String])]  = {

    val randomNeighbours = statGen.createRandomNeighboursList(nodeMap.get(value._1).get.getNodeId, 10, topology.getNode.size())
    var mapped: mutable.MutableList[(String, List[String])] = mutable.MutableList()
    randomNeighbours.foreach(n => {
      mapped += ((value._1, n))
    })
    return mapped
  }

  private def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("File Input LISA Streaming Job")
    if ("local" == Env) {
      conf.setMaster(Master)
        .setSparkHome("/home/snoooze/spark/spark-1.0.0")
        .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    }

    return conf
  }

  private def initConfig() = {
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    HdfsPath = config.getProperty("hdfs.path." + Env)
    Strategy = Some(config.getProperty("receiver.strategy"))
  }
}
