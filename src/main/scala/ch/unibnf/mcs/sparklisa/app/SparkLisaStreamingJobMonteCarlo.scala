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
      val Master: String = "local[5]"

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]
  val statGen = RandomTupleGenerator
  val log = Logger.getLogger(getClass)

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
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (sum, cnt) => sum/cnt}

    val variance = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[Double]) => {
      var mean = 0.0
      try {mean = meanRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
      valueRDD.map(value => {
        math.pow(value._2 - mean, 2.0)
      })
    })


    val stdDev = variance.transformWith(runningCount, (varianceRDD, countRDD: RDD[Long]) => {
      var variance = 0.0
      try {variance = varianceRDD.reduce(_ + _)} catch {
        case use: UnsupportedOperationException => {}
      }
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
    allLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allLisaValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")
    createLisaMonteCarlo(allLisaValues, finalLisaValues, nodeMap, topology)

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
      var mean_ = 0.0
      try{mean_ = meanRDD.reduce(_ + _)} catch {
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

  private def getRandomNeighbours(value: (String, Double), nodeMap: mutable.Map[String, NodeType], topology: Topology):
  mutable.MutableList[(String, (Double, List[String]))]  = {

    val randomNeighbours = statGen.createRandomNeighboursList(nodeMap.get(value._1).get.getNodeId, 1000, topology.getNode.size())
    var mapped: mutable.MutableList[(String, (Double, List[String]))] = mutable.MutableList()
    randomNeighbours.foreach(n => {
      mapped += ((value._1, (value._2, n)))
    })
    return mapped
  }

  private def createLisaMonteCarlo(allLisaValues: DStream[(String, Double)], finalLisaValues: DStream[(String, Double)], nodeMap: mutable.Map[String,
    NodeType], topology: Topology) = {
    val numberOfBaseStations: Int = topology.getBasestation.size()
    val numberOfNodes: Int = topology.getNode.size()
    import org.apache.spark.streaming.StreamingContext._

    allLisaValues.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val lisaValuesWithRandomNeighbourIds: DStream[(String, (Double, List[String]))] = allLisaValues
      .flatMap(value => getRandomNeighbours(value, nodeMap, topology))

//    val lisaValuesWithRandomNeighbourLisaValues: DStream[((String, Double), (String, (Double, List[String])))] =
//      lisaValuesWithRandomNeighbourIds.map(t => ((t._1, t._2._1), t._2._2))
//      .map(t => ((t._1, t._2), t._2))
//      .flatMapValues(l => l)
//      .map(t => (t._2, t._1))
//      .join(allLisaValues)
//      .map(t => ((t._1, t._2._2), (t._2._1._1._1, (t._2._1._1._2, t._2._1._2))))

    val t0 = lisaValuesWithRandomNeighbourIds.transform(rdd => {
      rdd.map { case t => remap1(t)}
    })

//    val t0: DStream[((String, Double), List[String])] = lisaValuesWithRandomNeighbourIds
//      .map { case (nodeId1, (value, randomNeighbours)) => ((nodeId1, value), randomNeighbours) }

    val t1: DStream[(((String, Double), List[String]), List[String])] = t0
      .map { case ((nodeId2, value), randomNeighbours) => (((nodeId2, value), randomNeighbours), randomNeighbours) }

    val t2: DStream[(((String, Double), List[String]), String)] = t1.flatMapValues(l => l)

    val t3: DStream[(String, ((String, Double), List[String]))] = t2
      .map { case (compositeKey, randomValue) => (randomValue, compositeKey)}

    val t4: DStream[(String, (((String, Double), List[String]), Double))] = t3.join(allLisaValues)

    val lisaValuesWithRandomNeighbourLisaValues: DStream[((String, Double), (String, (Double, List[String])))] =
      t4.map {
        case (randomNeighbourId, (((nodeId, value), randomNeighbours), randomValue)) =>
          ((randomNeighbourId, randomValue), (nodeId, (value, randomNeighbours)))
      }

    val randomNeighbourSums: DStream[((String, List[String]), Double)] = lisaValuesWithRandomNeighbourLisaValues
      .map {
        case ((randomNeighbourId, randomValue), (nodeId, (value, randomNeighbours))) =>
          ((nodeId, randomNeighbours), randomValue)
      }
      .groupByKey()
      .map { case (compositeKey, randomValues) =>  (compositeKey, randomValues.sum / randomValues.size.toDouble)}

    val randomLisaValues: DStream[(String, Double)] = randomNeighbourSums
      .map { case ((nodeId, _), randomNeighbourAverage) => (nodeId, randomNeighbourAverage)}
      .join(allLisaValues)
      .map { case (nodeId, (randomNeighbourAverage, lisaValue)) => (nodeId, randomNeighbourAverage*lisaValue)}

    val measuredValuesPositions = randomLisaValues.groupByKey()
      .join(finalLisaValues)
      .map { case (nodeId, (randomLisaValues, finalLisaValue)) =>
        (nodeId, randomLisaValues.count(_ < finalLisaValue).toDouble / randomLisaValues.size.toDouble) }

    lisaValuesWithRandomNeighbourIds.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/lisaValuesWithRandomNeighbourIds")
    lisaValuesWithRandomNeighbourLisaValues.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/lisaValuesWithRandomNeighbourLisaValues")
    randomNeighbourSums.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/randomNeighbourSums")
    t0.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/t0")
    t1.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/t1")
    t2.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/t2")
    t3.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/t3")
    t4.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/t4")
    randomLisaValues.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/randomLisaValues")
    measuredValuesPositions.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/measuredValuesPositions")
  }

  private def remap1(t: (String, (Double, List[String]))): ((String, Double), List[String]) = {
    val t1 = t._1
    val t21 = t._2._1
    val t22 = t._2._2
    log.info(s"remapping tuple ($t1,($t21, $t22)) to (($t1, $t21), $t22)")
    return ((t._1, t._2._1), t._2._2)
  }

}
