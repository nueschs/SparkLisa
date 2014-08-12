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
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by snoooze on 16.06.14.
 */
object ScalaSimpleSparkAppEval {

  val SumKey: String = "SUM_KEY"

  val Master: String = "spark://saight02:7077"
//  val Master: String = "local[2]"

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App").setMaster(Master)
      .setSparkHome("/home/snoooze/spark/spark-1.0.2")
      .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    return conf
  }

  private def readXml(): Topology = {
//    val is = getClass().getClassLoader().getResourceAsStream("xml/simple_topology.xml")
//    val context = JAXBContext.newInstance(classOf[Topology])
//    val unmarshaller = context.createUnmarshaller()
//    val t: Topology = unmarshaller.unmarshal(is).asInstanceOf[Topology]
    return TopologyHelper.createSimpleTopology();
  }

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
    import StreamingContext._
    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.createSimpleTopology()
    val nodeMap = TopologyHelper.createNodeMap(topology).asScala

    var allValues: DStream[(String, Double)] = null

    nodeMap.foreach(t2 => {
      val stream = ssc.actorStream[(String, Double)](Props(new SensorSimulatorActorReceiverEval(t2._2)), t2._1+"Receiver")
      if (allValues == null) {
        allValues = stream
      } else {
        allValues = allValues.union(stream)
      }
    })

    // Create count for each "iteration" - i.e number of values emitted by receiver with the same receiver count
    val runningCount: DStream[(String, Long)] = allValues.map(value => (value._1.split("_")(1), 1L)).reduceByKey(_+_)

    // first, map count as key to tuple (value, 1.0), then reduceByKey these tuples by adding up values and counts, and finally calculate the mean by division
    val runningMean: DStream[(String, Double)] = allValues.map(t => (t._1.split("_")(1), (t._2, 1.0))).reduceByKey((a,b) => (a._1+b._1, a._2 + b._2)).map(t => (t._1, t._2._1/t._2._2))

    // for each key (i.e. count), calculate mean, then create the squared difference for each value of this key
    val meanDiff: DStream[(String, Double)] = allValues.transformWith(runningMean, (valueRDD, meanRDD: RDD[(String, Double)]) => {
      valueRDD.map(value => (value._1.split("_")(1), value._2)).join(meanRDD).map(value => (value._1, math.pow(value._2._1-value._2._2, 2.0)))
    })

    // create the sum of all differences, divide by count, and take the square root of the result to obtain standard deviation
    val stdDev: DStream[(String, Double)] = meanDiff.transformWith(runningCount, (diffRDD, countRDD: RDD[(String, Long)]) => {
      val diffSum: RDD[(String, Double)] = diffRDD.reduceByKey(_+_)
      countRDD.join(diffSum).map(value => (value._1, math.sqrt(value._2._2 / value._2._1.toDouble)))
    })

    val allLisaVals = createLisaValues(allValues, runningMean, stdDev)

    val allNeighbourVals : DStream[(String, Double)] = allLisaVals.flatMap(value => mapToNeighbourKeys(value, nodeMap))
    val neighboursNormalizedSums: DStream[(String, Double)] = allNeighbourVals.map(value => (value._1, (1.0, value._2))).reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).map(value => (value._1, value._2._2/value._2._1))

    val finalLisaValues = allLisaVals.join(neighboursNormalizedSums).map(value => (value._1, value._2._1 * value._2._2))

    storeStringPairDStream(allValues, "allValues")
    storeStringPairDStream(finalLisaValues, "finalLisaValues")

    ssc.start()
    ssc.awaitTermination()

  }

  private def mapToNeighbourKeys(value : (String, Double), nodeMap : mutable.Map[String, NodeType]) : mutable.Traversable[(String, Double)] = {
    var mapped : mutable.MutableList[(String, Double)] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for(n <- nodeMap.get(value._1.split("_")(0)).get.getNeighbour()) {
      mapped += ((n+"_"+value._1.split("_")(1), value._2))
    }
    return mapped
  }

  private def storeDoubleDStream(stream : DStream[Double], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, timestamp.toString, value.toString)
      })
    })
  }

  private def storeLongDStream(stream : DStream[Long], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, timestamp.toString, value.toString)
      })
    })
  }

  private def storeNodePairDStream(stream : DStream[(NodeType, Double)], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, value._1.getNodeId+"_"+timestamp, value._2.toString)
      })
    })
  }

  private def storeStringPairDStream(stream : DStream[(String, Double)], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, value._1+"_"+timestamp, value._2.toString)
      })
    })
  }

  private def storeStringLongPairDStream(stream : DStream[(String, Long)], mapKey : String) = {
    stream.foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      rdd.foreach(value => {
        storeKeyValueMap(mapKey, value._1+"_"+timestamp, value._2.toString)
      })
    })
  }

  /* First we map all values to their count key to be able to join the according DStream
  * to both mean and standard deviation in a second step.
  * Finally, we map the calculated "local LISA value" back to the original value key
  *
  * The joined DStream contains tuples structured as follows: (countKey, (((valueKey, value), mean), stddev))
  */

  private def createLisaValues(nodeValues : DStream[(String, Double)], runningMean : DStream[(String, Double)], stdDev : DStream[(String, Double)]) : DStream[(String, Double)] = {
    val valuesWithCountKey: DStream[(String, (String, Double))] = nodeValues.map(value => (value._1.split("_")(1), value))
    val allJoined: DStream[(String, (((String, Double), Double), Double))] = valuesWithCountKey.join(runningMean).join(stdDev)
    return allJoined.map(v => (v._2._1._1._1, (v._2._1._1._2-v._2._1._2)/v._2._2))
  }

  private def storeKeyValueMap(mapkey: String, key : String, value : String) = {
    val redisson : Redisson = Redisson.create()
    redisson.getMap(mapkey).put(key, value)
    redisson.shutdown()
  }

}
