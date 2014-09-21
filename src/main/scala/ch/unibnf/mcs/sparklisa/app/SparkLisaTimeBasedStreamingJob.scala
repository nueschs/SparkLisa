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

object SparkLisaTimeBasedStreamingJob {

  val SumKey: String = "SUM_KEY"

//  val Master: String = "spark://saight02:7077"
      val Master: String = "local[32]"

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

    val currentValues: DStream[(String, Double)] = allValues.map(t => (t._1, t._2(0)))
    val pastValues: DStream[(String, Array[Double])] = allValues.map(t => (t._1, t._2.takeRight(t._2.size-1)))
    val pastValuesFlat: DStream[(String, Double)] = pastValues.flatMapValues(a => a.toList)
    val allValuesFlat: DStream[(String, Double)] = currentValues.union(pastValuesFlat)
    val runningCount: DStream[Long] = allValuesFlat.count()
    val runningMean: DStream[Double] = allValuesFlat.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)

    val variance = allValuesFlat.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0
      try {mean = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      valueRDD.map(t => {
        math.pow(t._2 - mean, 2.0)
      })
    })

    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0
      try{variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
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
//    allValues
//      .flatMapValues(a => a.toList.zipWithIndex.map(t => ("k-"+t._2.toString, t._1)))
//      .map(t => ((t._1, t._2._1), t._2._2))
//      .saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/timedMappedValues")
//    allValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")

    ssc.start()
    ssc.awaitTermination(timeout*1000)

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

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, k: Int,
                              rate: Double): DStream[(Int, Array[Double])] = {
    val values: DStream[(Int, Array[Double])] = ssc.actorStream[(Int, Array[Double])](
      Props(classOf[TimeBasedTopologySimulatorActorReceiver],topology.getNode.toList, rate, k), "receiver")
    return values
  }

  private def initConfig() = {
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    HdfsPath = config.getProperty("hdfs.path." + Env)
    Strategy = Some(config.getProperty("receiver.strategy"))
  }

  private def mapToNeighbourKeys(value: (String, Double), nodeMap: mutable.Map[String, NodeType]): mutable.Traversable[(String, Double)] = {
    var mapped: mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (n <- nodeMap.get(value._1).getOrElse(new NodeType()).getNeighbour()) {
      mapped += ((n, value._2))
    }
    return mapped
  }


  /*
  * returns a DStream[(NodeType, Double)]
   */
  private def createLisaValues(nodeValues: DStream[(String, Double)], runningMean: DStream[Double], stdDev: DStream[Double]): DStream[(String, Double)] = {
    import org.apache.spark.SparkContext._
    return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      var mean_ = 0.0
      try {mean_ = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      nodeRDD.mapValues(d => d-mean_)
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      var stdDev_ = 0.0
      try {stdDev_ = stdDevRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      nodeDiffRDD.mapValues(d => d/stdDev_)
    })
  }
}
