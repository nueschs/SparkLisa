package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.{TriggeringStreamingListener, LisaStreamingListener}
import ch.unibnf.mcs.sparklisa.receiver.TriggerableTopologySimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkLisaStreamingJobTriggered {

  val SumKey: String = "SUM_KEY"

//  val Master: String = "spark://saight02:7077"
      val Master: String = "local[2]"

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()

    val batchDuration: Int = args(0).toInt
    val rate: Double = args(1).toDouble
    val numBaseStations: Int = args(2).toInt
    val timeout: Int = args(3).toInt
    val topologyPath: String = args(4)

    import org.apache.spark.streaming.StreamingContext._
    val conf: SparkConf = createSparkConf(numBaseStations)
    conf.set("spark.default.parallelism", s"$numBaseStations")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchDuration))
    ssc.addStreamingListener(new TriggeringStreamingListener())

    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(topologyPath, numBaseStations)

    val tempMap: mutable.Map[Integer, NodeType] = TopologyHelper.createNumericalNodeMap(topology).asScala
    val nodeMap: mutable.Map[Int, NodeType] = for ((k,v) <- tempMap; (nk,nv) = (k.intValue, v)) yield (nk,nv)
    val allValues: DStream[(Int, Double)] = createAllValues(ssc, topology, numBaseStations, rate)

    val runningCount = allValues.count()
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)

    val variance = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0
      try {mean = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      valueRDD.map(value => {
        math.pow(value._2 - mean, 2.0)
      })
    })


    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0
      try {variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException =>
      }
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })

    //
    val allLisaValues = createLisaValues(allValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(Int, Double)] = allLisaValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))
    val neighboursNormalizedSums = allNeighbourValues.groupByKey().mapValues(l => l.sum / l.size.toDouble)
    val finalLisaValues = allLisaValues.join(neighboursNormalizedSums).mapValues(d => d._1 * d._2)
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues.count().saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.count().saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalCount")

    ssc.start()
    ssc.awaitTermination(timeout*1000)

  }

  private def createSparkConf(numberOfBaseStations: Int): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("File Input LISA Streaming Job")
    if ("local" == Env) {
      conf.setMaster(Master)
        .setSparkHome("/home/snoooze/spark/spark-1.0.0")
        .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    }
    conf.set("spark.default.parallelism", s"$numberOfBaseStations")

    return conf
  }

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, rate: Double): DStream[(Int, Double)] = {
    val values: DStream[(Int, Double)] =  ssc.actorStream[(Int, Double)](Props(classOf[TriggerableTopologySimulatorActorReceiver], topology.getNode.toList, rate), "receiver")
    values.repartition(numBaseStations)
    return values
  }

  private def initConfig() = {
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    HdfsPath = config.getProperty("hdfs.path." + Env)
    Strategy = Some(config.getProperty("receiver.strategy"))
  }

  private def mapToNeighbourKeys(value: (Int, Double), nodeMap: mutable.Map[Int, NodeType]): mutable.Traversable[(Int, Double)] = {
    var mapped: mutable.MutableList[(Int, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (n <- nodeMap.getOrElse(value._1, new NodeType()).getNeighbour) {
      mapped += ((n.substring(4).toInt, value._2))
    }
    return mapped
  }


  /*
  * returns a DStream[(NodeType, Double)]
   */
  private def createLisaValues(nodeValues: DStream[(Int, Double)], runningMean: DStream[Double], stdDev:
    DStream[Double]): DStream[(Int, Double)] = {
    import org.apache.spark.SparkContext._
    nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      var mean_ = 0.0
      try{mean_ = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      nodeRDD.mapValues(d => d-mean_)
    }).transformWith(stdDev, (varianceRDD, stdDevRDD: RDD[Double]) => {
      var stdDev_ = 0.0
      try {stdDev_ = stdDevRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      varianceRDD.mapValues(d => d/stdDev_)
    })
  }
}
