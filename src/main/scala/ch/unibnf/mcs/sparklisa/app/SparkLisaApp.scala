package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConverters._

import scala.collection.mutable

/**
 * Created by Stefan Nüesch on 16.09.14.
 */
abstract class SparkLisaApp extends Serializable {

  val SumKey: String = "SUM_KEY"

  //  val Master: String = "spark://saight02:7077"
  val Master: String = "local[32]"

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]

  def main(args: Array[String]) = {
    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()

    val batchDuration: Int = args(0).toInt
    val rate: Double = args(1).toDouble
    val numBaseStations: Int = args(2).toInt
    val timeout: Int = args(3).toInt
    val topologyPath: String = args(4)
    val k: Int = if (args.length >= 6) args(5).toInt else -1

    val conf: SparkConf = createSparkConf()

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchDuration))
    ssc.addStreamingListener(new LisaStreamingListener())

    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(topologyPath, numBaseStations)

    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala

    run_calculations(topology, nodeMap, ssc, numBaseStations, rate, k)

    ssc.start()
    ssc.awaitTermination(timeout*1000)
  }

  def run_calculations(topology: Topology, nodeMap: mutable.Map[String, NodeType],
                                ssc: StreamingContext, numBaseStations: Int, rate: Double, k: Int)

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

  /*
* returns a DStream[(NodeType, Double)]
 */
  def createLisaValues(nodeValues: DStream[(String, Double)], runningMean: DStream[Double], stdDev: DStream[Double]): DStream[(String, Double)] = {
    return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      var mean_ = 0.0
      try {mean_ = meanRDD.reduce( _+_ )} catch {
        case use: UnsupportedOperationException => {}
      }
      nodeRDD.map(t => (t._1, t._2 - mean_))
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      var stdDev_ = 0.0
      try {stdDev_ = stdDevRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      nodeDiffRDD.map(t => (t._1, t._2 / stdDev_))
    })
  }

  def mapToNeighbourKeys(value: (String, Double), nodeMap: mutable.Map[String, NodeType]): mutable.Traversable[(String, Double)] = {
    var mapped: mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (n <- nodeMap.get(value._1).getOrElse(new NodeType()).getNeighbour()) {
      mapped += ((n, value._2))
    }
    return mapped
  }

}
