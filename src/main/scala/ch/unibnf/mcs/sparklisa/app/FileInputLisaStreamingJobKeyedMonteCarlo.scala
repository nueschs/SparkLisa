package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import cern.jet.random.engine.RandomSeedTable
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
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
object FileInputLisaStreamingJobKeyedMonteCarlo {

  import org.apache.spark.streaming.StreamingContext._

  val SumKey: String = "SUM_KEY"

    val Master: String = "spark://saight02:7077"
//  val Master: String = "local[2]"

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]
  val statGen = RandomTupleGenerator


  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(args(2).toLong))
    ssc.addStreamingListener(new LisaStreamingListener())
    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(args(0), args(1).toInt)

    val allValues: DStream[((String, String), Double)] = createAllValues(ssc, topology)
    val allValuesPerKey: DStream[(String, (String, Double))] = allValues.map(value => (value._1._2, (value._1._1,
      value._2)))
    val nodeMap: mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala

    val runningCount: DStream[(String, Double)] = allValuesPerKey.map(value => (value._1, 1.0)).reduceByKey(_ + _)
    val runningMean: DStream[(String, Double)] = allValues.map(value => (value._1._2, (value._2,
      1.0))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(value => (value._1, value._2._1 / value._2._2))

    val variance: DStream[(String, (String, Double))] = allValuesPerKey.join(runningMean)
      .map(value => (value._1, (value._2._1._1, math.pow(value._2._1._2 - value._2._2, 2.0))))

    val stdev: DStream[(String, Double)] = variance.map(value => (value._1, value._2._2)).reduceByKey(_ + _)
       .join(runningCount).map(value => (value._1, math.sqrt(value._2._1 / value._2._2)))

    val allLisaValues: DStream[(String, (String, Double))] = allValuesPerKey.join(runningMean)
      .map(value => (value._1, (value._2._1._1, value._2._1._2-value._2._2)))
      .join(stdev)
      .map(value => (value._1, (value._2._1._1, value._2._1._2/value._2._2)))

    val allNeighbourValues: DStream[(String, (String, Double))] = allLisaValues
      .flatMapValues(value => mapToNeighbourKeys(value, nodeMap))

    val neighboursStandardizedSums: DStream[((String, String), Double)] = allNeighbourValues.map(value => ((value._1,
      value._2._1), value._2._2))
      .groupByKey().map(value => (value._1, value._2.sum / value._2.size.toDouble))

    val finalLisaValues: DStream[((String, String), Double)] = allLisaValues.map(value => ((value._1, value._2._1), value._2._2))
      .join(neighboursStandardizedSums)
      .map(value => (value._1, value._2._1*value._2._2))


    createLisaMonteCarlo(allLisaValues, nodeMap, topology)
//    val lisaMonteCarlo = createLisaMonteCarlo(allLisaValues, nodeMap, topology)

    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
//    allValues.saveAsTextFiles(HdfsPath + s"/results/allValues")
//    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_${numberOfNodes}/finalLisaValues")
//    lisaMonteCarlo.saveAsTextFiles(HdfsPath+"results/statTest")

    ssc.start()
    ssc.awaitTermination()

  }

  private def createLisaMonteCarlo(allLisaValues: DStream[(String, (String, Double))], nodeMap: mutable.Map[String,
    NodeType], topology: Topology) = {

    val lisaValuesReMapped: DStream[((String, String), Double)] = allLisaValues.map(value => ((value._1, value._2._1), value._2._2))
    val lisaValuesWithRandomNeighbourIds: DStream[((String, String), (Double, List[String]))] = lisaValuesReMapped
      .flatMap(value => getRandomNeighbours(value, nodeMap, topology))



    val lisaValuesWithRandomNeighbourLisaValues: DStream[(((String, String), Double), ((String, String),
      (Double, List[String])))] = lisaValuesWithRandomNeighbourIds
      .transformWith(lisaValuesReMapped, (neighbourRDD, lisaRDD: RDD[((String, String),
      Double)]) => lisaRDD.cartesian(neighbourRDD))
      .filter(value => value._2._2._2.contains(value._1._1._2))

    lisaValuesWithRandomNeighbourIds
      .transformWith(lisaValuesReMapped, (neighbourRDD, lisaRDD: RDD[((String, String),
      Double)]) => lisaRDD )
//
//    val randomNeighboursSums: DStream[(((String, String), List[String]),
//      Double)] = lisaValuesWithRandomNeighbourLisaValues
//      .map(value => ((value._2._1, value._2._2._2), value._1._2)).groupByKey()
//      .map(value => (value._1, value._2.sum / value._2.size.toDouble))
//
//    val randomLisas: DStream[((String, String), Double)] = randomNeighboursSums
//      .map(value => (value._1._1, value._2)).join(lisaValuesReMapped)
//      .map(value => (value._1, value._2._2 * value._2._1))
//
//    val lisaPositionInRandomValues: DStream[((String, String), Double)] = randomLisas
//      .groupByKey().join(lisaValuesReMapped)
//      .map(value => {
//      (value._1, (value._2._1.filter(_ < value._2._2)).size.toDouble / value._2._1.size.toDouble)
//    })

    lisaValuesReMapped.saveAsTextFiles(HdfsPath+"/results/lisaValuesReMapped")
    lisaValuesWithRandomNeighbourIds.saveAsTextFiles(HdfsPath+"/results/statTest/lisaValuesWithRandomNeighbourIds")
    lisaValuesWithRandomNeighbourLisaValues.saveAsTextFiles(HdfsPath+"/results/statTest/lisaValuesWithRandomNeighbourLisaValues")
//    randomNeighboursSums.saveAsTextFiles(HdfsPath+"results/statTest/randomNeighboursSums")
//    randomLisas.saveAsTextFiles(HdfsPath+"results/statTest/randomLisas")
//    lisaPositionInRandomValues.saveAsTextFiles(HdfsPath+"results/statTest/randomLisas")

//    return lisaPositionInRandomValues

//    val t0: DStream[((String, String), Double)] = allLisaValues.map(value => ((value._1, value._2._1), value._2._2))
//    val t1: DStream[((String, String), (Double, List[String]))] = t0.flatMap(value => getRandomNeighbours(value, nodeMap, topology))
//    val t2: DStream[(((String, String), Double), ((String, String), (Double, List[String])))] = t1.transformWith(t0, (t1RDD, t0RDD: RDD[((String, String), Double)]) => {
//      t0RDD.cartesian(t1RDD)
//    })
//    val t3: DStream[(((String, String), Double), ((String, String), (Double, List[String])))] = t2.filter(value => {
//      value._2._2._2.contains(value._1._1._2)
//    })
//    val t4: DStream[(((String, String), List[String]), Double)] = t3.map(value => {
//      ((value._2._1, value._2._2._2),  value._1._2)
//    })
//    val t5: DStream[(((String, String), List[String]), Iterable[Double])] = t4.groupByKey()
//    val t6: DStream[(((String, String), List[String]), Double)] = t5.map(value => (value._1, value._2.sum / value._2.size.toDouble))
//    val t7: DStream[((String, String), Double)] = t6.map(value => (value._1._1, value._2))
//    val t8: DStream[((String, String), (Double, Double))] = t7.join(t0)
//    val t9: DStream[((String, String), Double)] = t8.map(value => (value._1, value._2._2*value._2._1))
//    val t10: DStream[((String, String), Iterable[Double])] = t9.groupByKey()
//    val t11: DStream[((String, String), (Iterable[Double], Double))] = t10.join(t0)
//    val t12: DStream[((String, String), Double)] = t11.map(value => {
//      (value._1, (value._2._1.filter(_ < value._2._2)).size.toDouble/value._2._1.size.toDouble)
//    })
  }

  private def getRandomNeighbours(value: ((String, String), Double), nodeMap: mutable.Map[String, NodeType], topology: Topology):
    mutable.MutableList[((String, String), (Double, List[String]))]  = {

    val randomNeighbours = statGen.createRandomNeighboursList(nodeMap.get(value._1._2).get.getNodeId, 1000, topology.getNode.size())
    var mapped: mutable.MutableList[((String, String), (Double, List[String]))] = mutable.MutableList()
    randomNeighbours.foreach(n => {
      mapped += (((value._1), (value._2, n)))
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

  private def mapToNeighbourKeys(value: (String, Double), nodeMap: mutable.Map[String,
    NodeType]): mutable.Traversable[(String, Double)] = {
    var mapped: mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (n <- nodeMap.get(value._1).getOrElse(new NodeType()).getNeighbour()) {
      mapped += ((n, value._2))
    }
    return mapped
  }


  private def createAllValues(ssc: StreamingContext, topology: Topology): DStream[((String, String), Double)] = {
    val srcPath: String = HdfsPath + "/values/" + topology.getNode.size().toString + "_" + topology.getBasestation
      .size().toString + "/"
    var allValues: DStream[((String, String), Double)] = null
    topology.getBasestation.asScala.foreach(station => {

      val stream = ssc.textFileStream(srcPath + station.getStationId.takeRight((1))).map(line => {
        val line_arr: Array[String] = line.split(";")
        val nodeId = line_arr(0).split("_")(0)
        val cnt = line_arr(0).split("_")(1)
        ((nodeId, cnt), line_arr(1).toDouble)
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
