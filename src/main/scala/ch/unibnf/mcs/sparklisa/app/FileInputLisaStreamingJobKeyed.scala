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
 * Created by Stefan Nüesch on 16.06.14.
 */
object FileInputLisaStreamingJobKeyed {

  import org.apache.spark.streaming.StreamingContext._

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
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(args(2).toLong))
    ssc.addStreamingListener(new LisaStreamingListener())
    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(args(0), args(1).toInt)

    val allValues: DStream[(String, Double)] = createAllValues(ssc, topology)
    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala

    val runningCount = allValues.count()
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)
    //
    val variance = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      val mean = meanRDD.reduce(_ + _)
      valueRDD.map(value => {
        math.pow(value._2 - mean, 2.0)
      })
    })


    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      val variance: Double = varianceRDD.reduce(_ + _)
      countRDD.map(cnt => {
        math.sqrt(variance / cnt.toDouble)
      })
    })

    val allLisaValues: DStream[(String, Double)] = createLisaValues(allValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(String, (String, Double))] = allLisaValues.map(value => {
      (value._1.split("_")(1), (value._1.split("_")(0), value._2))
    }).flatMapValues(value => mapToNeighbourKeys(value, nodeMap))

    val neighboursNormalizedSums: DStream[((String, String), Double)] = allNeighbourValues.map(value => ((value._1,
      value._2._1), value._2._2))
      .groupByKey().map(value => (value._1, value._2.sum / value._2.size.toDouble))

    val finalLisaValues: DStream[((String, String), Double)] = allLisaValues.map(value => {
      ((value._1.split("_")(1), value._1.split("_")(0)), value._2)
    }).join(neighboursNormalizedSums).map(value => (value._1, value._2._1 * value._2._2))

    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues.saveAsTextFiles(HdfsPath + s"/results/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_${numberOfNodes}/finalLisaValues")

    ssc.start()
    ssc.awaitTermination()

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

  private def mapToNeighbourKeys(value: (String, Double), nodeMap: mutable.Map[String,
    NodeType]): mutable.Traversable[(String, Double)] = {
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
  private def createLisaValues(nodeValues: DStream[(String, Double)], runningMean: DStream[Double],
                               stdDev: DStream[Double]): DStream[(String, Double)] = {
    return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      val mean_ = meanRDD.reduce(_ + _)
      nodeRDD.map(value => (value._1, value._2 - mean_))
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      val stdDev_ = stdDevRDD.reduce(_ + _)
      nodeDiffRDD.map(value => (value._1, value._2 / stdDev_))
    })
  }

  private def createAllValues(ssc: StreamingContext, topology: Topology): DStream[(String, Double)] = {
    val srcPath: String = HdfsPath + "/values/" + topology.getNode.size().toString + "_" + topology.getBasestation
      .size().toString + "/"
    var allValues: DStream[(String, Double)] = null
    topology.getBasestation.asScala.foreach(station => {

      val stream = ssc.textFileStream(srcPath + station.getStationId.takeRight((1))).map(line => {
        val line_arr: Array[String] = line.split(";")
        (line_arr(0), line_arr(1).toDouble)
      })
      if (allValues == null) {
        allValues = stream
      } else {
        allValues = allValues.union(stream)
      }
    })
    return allValues
  }
}
