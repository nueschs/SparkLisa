package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.{SensorSimulatorActorReceiverEval, SensorSimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.redisson.Redisson
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by snoooze on 16.06.14.
 */
object ScalaSimpleSparkApp {

  val SumKey: String = "SUM_KEY"

  val Master: String = "spark://saight02:7077"
//  val Master: String = "local[2]"

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App")
//      .setMaster(Master)
//      .setSparkHome("/home/snoooze/spark/spark-1.0.0")
//      .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    return conf
  }

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    import StreamingContext._
    ssc.checkpoint(".checkpoint")


    val topology: Topology = TopologyHelper.createSimpleTopology()
    val nodeMap : mutable.Map[String, NodeType] = TopologyHelper.createNodeMap(topology).asScala

    var allValues: DStream[(String, Double)] = null

    nodeMap.foreach(t2 => {
      val stream = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiver(t2._2)), t2._1+"Receiver")
      if (allValues == null) {
        allValues = stream
      } else {
        allValues = allValues.union(stream)
      }
    })

    val runningCount = allValues.count()
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1+b._1, a._2 + b._2)).map(t => t._1/t._2)


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

    val allLisaVals = createLisaValues(allValues, runningMean, stdDev)

    val allNeighbourVals : DStream[(String, Double)] = allLisaVals.flatMap(value => mapToNeighbourKeys(value, nodeMap))
    val neighboursNormalizedSums = allNeighbourVals.map(value => (value._1, (1.0, value._2))).reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).map(value => (value._1, value._2._2/value._2._1))

    val finalLisaValues = allLisaVals.join(neighboursNormalizedSums).map(value => (value._1, value._2._1 * value._2._2))

//    finalLisaValues.saveAsTextFiles("hdfs://localhost:9999/sparkLisa/finalLisaValues")
    finalLisaValues.print()

    ssc.start()
    ssc.awaitTermination()

  }

  private def mapToNeighbourKeys(value : (String, Double), nodeMap : mutable.Map[String, NodeType]) : mutable.Traversable[(String, Double)] = {
    var mapped : mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for(n <- nodeMap.get(value._1).get.getNeighbour()) {
      mapped += ((n, value._2))
    }
    return mapped
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
}
