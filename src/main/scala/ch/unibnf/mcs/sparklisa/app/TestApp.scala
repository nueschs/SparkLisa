package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import akka.actor._
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.{RandomTupleReceiver, TopologySimulatorActorReceiver}
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

object TestApp {

  val Master: String = "local[4]"
//  val Master: String = "spark://saight02:7077"
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
    val topology = TopologyHelper.topologyFromBareFile(args(1), 4)
    val numBaseStations = args(2).toInt
    val nodesPerBase = topology.getNode.size() / numBaseStations
    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala

    val values: DStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList, 0.01), "receiver1")
    val randomNeighbourTuples = ssc.actorStream[(String, List[List[String]])](Props(classOf[RandomTupleReceiver], topology.getNode.toList, 0.01), "receiver2")
    val t4: DStream[(String, List[String])] = randomNeighbourTuples.flatMapValues(l => l)
    import org.apache.spark.SparkContext._

    val t6: DStream[(String, Double)] = t4.transformWith(values, (t4Rdd, valueRdd: RDD[(String, Double)]) => {
      val t7: collection.Map[String, Double] = valueRdd.collectAsMap()
      t4Rdd.mapValues{case l => {
        val randomValues: List[Double] = t7.filter(t => l.contains(t._1)).values.toList
        randomValues.foldLeft(0.0)(_+_) / randomValues.foldLeft(0.0)((r,c) => r+1)
      }}
    })

    val t7 = t6.join(values).mapValues{ case t => t._1*t._2}
    val t8: DStream[(String, Iterable[Double])] = t7.groupByKey()
    val t9: DStream[(String, (Iterable[Double], Double))] = t8.join(values)
    val t10: DStream[(String, Double)] = t9.mapValues{ case t => t._1.count(_ < t._2)/ t._1.size.toDouble}

    values.foreachRDD(rdd => {rdd.foreach(f => println(f))})
    t4.foreachRDD(rdd => {rdd.foreach(f => println(f))})
    t6.foreachRDD(rdd => {rdd.foreach(f => println(f))})
    t7.foreachRDD(rdd => {rdd.foreach(f => println(f))})
    t10.foreachRDD(rdd => {rdd.foreach(f => println(f))})

//    val values: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new TopologySimulatorActorReceiver(topology.getNode.toList.slice(0,8), 60)), "receiver")
//    val values2:ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new TopologySimulatorActorReceiver(topology.getNode.toList.slice(8,16), 60)), "receiver")




//     ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology, 60), "receiver")
//    values.slice(Time)

//    values.count().print()
//    values2.print()

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
