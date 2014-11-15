package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.receiver.{NumericalRandomTupleReceiver, TemporalTopologySimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.util.Random

object TemporalLisaMonteCarloGlobalApp extends LisaDStreamFunctions with LisaAppConfiguration{

//  val Master: String = "spark://saight02:7077"
      val Master: String = "local[16]"

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.INFO)
    initConfig()

    val batchDuration: Int = args(0).toInt
    val rate: Double = args(1).toDouble
    val numBaseStations: Int = args(2).toInt
    val timeout: Int = args(3).toInt
    val topologyPath: String = args(4)
    val k: Int = args(5).toInt
    val numRandomValues: Int = args(6).toInt

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

    val randomNeighbours: ReceiverInputDStream[(Int, List[List[Int]])] =
      ssc.actorStream[(Int, List[List[Int]])](Props(
        classOf[NumericalRandomTupleReceiver], topology.getNode.toList, rate, numRandomValues), "receiver"
      )

    val currentValues: DStream[(Int, Double)] = allValues.mapValues(a => a(0))
    val pastValues: DStream[(Int, Array[Double])] = allValues.mapValues(a => a.takeRight(a.size-1))
    val runningCount: DStream[Long] = currentValues.count()
    val runningMean: DStream[Double] = currentValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2)).map(t => t._1 / t._2)
    val stdDev = createStandardDev(currentValues, runningCount, runningMean)
    val allStandardisedValues = createStandardisedValues(currentValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(Int, Double)] = allStandardisedValues.flatMap(t => mapToNeighbourKeys[Double](t, nodeMap))

    val allPastStandardisedValues: DStream[(Int, Array[Double])] = createPastStandardisedValues(pastValues)
    val pastNeighbourValues: DStream[(Int, Array[Double])] = allPastStandardisedValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))

    val allPastNeighbouringValues: DStream[(Int, Double)] = allPastStandardisedValues.join(pastNeighbourValues)
      .flatMapValues(t => t._1 ++ t._2)
    val neighboursNormalizedSums = allNeighbourValues.union(allPastNeighbouringValues).groupByKey()
      .map(t => (t._1, t._2.sum / t._2.size.toDouble))

    val finalLisaValues = allStandardisedValues.join(neighboursNormalizedSums).mapValues(t => t._1*t._2)
    val numberOfBaseStations = topology.getBasestation.size().toString
    val numberOfNodes = topology.getNode.size().toString
    allValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath + s"/results/${numberOfBaseStations}_$numberOfNodes/finalLisaValues")
    createLisaMonteCarlo(allStandardisedValues, allPastStandardisedValues, finalLisaValues, nodeMap, topology,
      randomNeighbours, k, stdDev, runningMean, numRandomValues)

    ssc.start()
    ssc.awaitTermination(timeout*1000)

  }

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, k: Int,
                              rate: Double): DStream[(Int, Array[Double])] = {
    val values: DStream[(Int, Array[Double])] = ssc.actorStream[(Int, Array[Double])](
      Props(classOf[TemporalTopologySimulatorActorReceiver],topology.getNode.toList, rate, k), "receiver")
    return values
  }

  private def createLisaMonteCarlo(currentStandardisedValues: DStream[(Int, Double)], pastStandardisedValues: DStream[(Int, Array[Double])],
    finalLisaValues: DStream[(Int, Double)], nodeMap: mutable.Map[Int, NodeType], topology: Topology, randomNeighbours:
    DStream[(Int, List[List[Int]])], k: Int, stdDev: DStream[Double], mean: DStream[Double], numRandomValues: Int) = {
    import org.apache.spark.SparkContext._
    import org.apache.spark.streaming.StreamingContext._

    val numberOfBaseStations: Int = topology.getBasestation.size()

    val randomNeighbourTuples: DStream[(Int, List[Int])] = randomNeighbours.flatMapValues(l => l)
    randomNeighbourTuples.repartition(numberOfBaseStations)

    val standardisedValuesCartesian: DStream[(Int, collection.Map[Int, Double])] =
      currentStandardisedValues.transform(valueRDD => {
        val valueMap: collection.Map[Int, Double] = valueRDD.collectAsMap()
        valueRDD.mapValues {case _ => valueMap }
      })

    val pastStandardisedValuesCartesian: DStream[(Int, collection.Map[Int, Array[Double]])] =
      pastStandardisedValues.transform(valueRDD => {
        val valueMap: collection.Map[Int, Array[Double]] = valueRDD.collectAsMap()
        valueRDD.mapValues {case _ => valueMap}
      })

    val standardisedValuesWithRandomNodes: DStream[(Int, (collection.Map[Int, Double], List[Int]))] =
      standardisedValuesCartesian.join(randomNeighbourTuples)


    val allRandomStandardisedValues: DStream[(Int, ((collection.Map[Int, Double], List[Int]),
                                      collection.Map[Int, Array[Double]]))] =
      standardisedValuesWithRandomNodes.join(pastStandardisedValuesCartesian)

    allRandomStandardisedValues.repartition(numberOfBaseStations)

    val randomNeighbourAverages = allRandomStandardisedValues.map(t => {
      val pastValuesSize = t._2._2.values.head.size
      val filteredMap = t._2._2.filter(me => me._1 != t._1)


      val randomPastValues: List[Double] = (for (i <- 0 until pastValuesSize) yield
        (for (_ <- 1 to new Random().nextInt(4)+1) yield
          new Random().shuffle(filteredMap.values).head(i)).toList).toList.flatten

      val randomCurrentValues: List[Double] = t._2._1._1.filter(me => t._2._1._2.contains(me._1)).values.toList
      val allRandomValues: List[Double] = randomCurrentValues ++ randomPastValues
      (t._1, allRandomValues.foldLeft(0.0)(_+_)/allRandomValues.foldLeft(0.0)((r,c) => r+1))
    })

    val randomLisaValues: DStream[(Int, Double)] = randomNeighbourAverages
      .join(currentStandardisedValues)
      .mapValues{ case t => t._1*t._2}

    val measuredValuesPositions = randomLisaValues.groupByKey()
      .join(finalLisaValues)
      .mapValues{ case t => (t._1.count(_ < t._2)+1)/ (t._1.size.toDouble+1.0)}
    val numberOfNodes = topology.getNode.size()
    measuredValuesPositions.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/measuredValuesPositions")
  }
}
