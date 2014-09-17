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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


import scala.collection.mutable

object SparkLisaStreamingJobMonteCarlo {

  val SumKey: String = "SUM_KEY"

//  val Master: String = "spark://saight02:7077"
      val Master: String = "local[32]"

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]
  val statGen = RandomTupleGenerator

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()

    val batchDuration: Int = args(0).toInt
    val rate: Double = args(1).toDouble
    val numBaseStations: Int = args(2).toInt
    val timeout: Int = args(3).toInt
    val topologyPath: String = args(4)

    import org.apache.spark.streaming.StreamingContext._
    val conf: SparkConf = createSparkConf()
    conf.set("spark.default.parallelism", numBaseStations.toString)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchDuration))
    ssc.addStreamingListener(new LisaStreamingListener())

    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(topologyPath, numBaseStations)

    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala
    val allValues: DStream[(String, Double)] = createAllValues(ssc, topology, numBaseStations, rate)


    val runningCount = allValues.count()
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)

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

    //
    val allLisaValues = createLisaValues(allValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(String, Double)] = allLisaValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))
    val neighboursNormalizedSums = allNeighbourValues.groupByKey().map(t => (t._1, t._2.sum / t._2.size.toDouble))
    val finalLisaValues = allLisaValues.join(neighboursNormalizedSums).map(t => (t._1, t._2._1 * t._2._2))
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")
    createLisaMonteCarlo(allLisaValues, nodeMap, topology)

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

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, rate: Double): DStream[(String, Double)] = {
    val nodesPerBase = topology.getNode.size()/numBaseStations
    var values: DStream[(String, Double)] = null
//    for (i <- 0 until numBaseStations){
//      if (values == null){
    values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList, rate), "receiver")
//    values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), rate), "receiver")
//      } else {
//        values = values.union(ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), rate), "receiver"))
//      }
//    }
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
    return nodeValues.transformWith(runningMean, (nodeRDD, meanRDD: RDD[Double]) => {
      val mean_ = meanRDD.reduce(_ + _)
      nodeRDD.map(t => (t._1, t._2 - mean_))
    }).transformWith(stdDev, (nodeDiffRDD, stdDevRDD: RDD[Double]) => {
      val stdDev_ = stdDevRDD.reduce(_ + _)
      nodeDiffRDD.map(t => (t._1, t._2 / stdDev_))
    })
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

    allLisaValues.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val lisaValuesWithRandomNeighbourIds: DStream[(String, (Double, List[String]))] = allLisaValues
      .flatMap(value => getRandomNeighbours(value, nodeMap, topology))

    val lisaValuesWithRandomNeighbourLisaValues: DStream[((String, Double), (String, (Double, List[String])))] =
      lisaValuesWithRandomNeighbourIds.map(t => ((t._1, t._2._1), t._2._2))
      .map(t => ((t._1, t._2), t._2))
      .flatMapValues(l => l)
      .map(t => (t._2, t._1))
      .join(allLisaValues)
      .map(t => ((t._1, t._2._2), (t._2._1._1._1, (t._2._1._1._2, t._2._1._2))))

//    val lisaValuesWithRandomNeighbourLisaValues: DStream[((String, Double), (String,(Double, List[String])))] =
//      lisaValuesWithRandomNeighbourIds.transformWith(allLisaValues, (neighbourRDD, lisaRDD: RDD[(String,
//      Double)]) => lisaRDD.cartesian(neighbourRDD))
//      .filter(value => value._2._2._2.contains(value._1._1))

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
