package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener
import ch.unibnf.mcs.sparklisa.receiver.{NumericalRandomTupleReceiver, NumericalTopologySimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * In addition to the spatial LISA, performs a Monte Carlo Simulation to test the significance of the LISA values
 * (first approach with poor performance)
 */
object SpatialLisaMonteCarloNaiveApp extends LisaDStreamFunctions with LisaAppConfiguration{


//  val Master: String = "spark://saight02:7077"
  val Master: String = "local[5]"
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
    val numRandomValues = args(5).toInt

    import org.apache.spark.streaming.StreamingContext._
    val conf: SparkConf = createSparkConf()
    conf.set("spark.default.parallelism", numBaseStations.toString)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchDuration))
    ssc.addStreamingListener(new LisaStreamingListener())

    ssc.checkpoint(".checkpoint")

    val topology: Topology = TopologyHelper.topologyFromBareFile(topologyPath, numBaseStations)

    val tempMap: mutable.Map[Integer, NodeType] = TopologyHelper.createNumericalNodeMap(topology).asScala
    val nodeMap: mutable.Map[Int, NodeType] = for ((k,v) <- tempMap; (nk,nv) = (k.intValue, v)) yield (nk,nv)
    val allValues: DStream[(Int, Double)] = createAllValues(ssc, topology, numBaseStations, rate)

    /*
     * The Receiver used here creates for each node a number of distinct sets of random neighbour keys
     * (i.e. select between one and four keys from the complete topology)
    */
    val randomNeighbours: ReceiverInputDStream[(Int, List[List[Int]])] =
      ssc.actorStream[(Int, List[List[Int]])](Props(
        classOf[NumericalRandomTupleReceiver], topology.getNode.toList, rate, numRandomValues), "receiver"
      )


    val runningCount = allValues.count()
    val runningMean = allValues.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (sum, cnt) => sum/cnt}

    val stdDev = createStandardDev(allValues, runningCount, runningMean)

    val allStandardisedValues = createStandardisedValues(allValues, runningMean, stdDev)
    val allNeighbourValues: DStream[(Int, Double)] = allStandardisedValues.flatMap(t => mapToNeighbourKeys(t, nodeMap))
    val neighboursNormalizedSums = allNeighbourValues.groupByKey().mapValues(l => l.sum / l.size.toDouble)
    val finalLisaValues = allStandardisedValues.join(neighboursNormalizedSums).mapValues(t => t._1*t._2)
    val numberOfNodes = topology.getNode.size()
    allValues.saveAsTextFiles(HdfsPath+ s"/results/${numBaseStations}_$numberOfNodes/allValues")
    finalLisaValues.saveAsTextFiles(HdfsPath+ s"/results/${numBaseStations}_$numberOfNodes/finalLisaValues")
    createLisaMonteCarlo(allStandardisedValues, finalLisaValues, nodeMap, topology, randomNeighbours)

    ssc.start()
    ssc.awaitTermination(timeout*1000)

  }

  private def createAllValues(ssc: StreamingContext, topology: Topology, numBaseStations: Int, rate: Double): DStream[(Int, Double)] = {
    val values: DStream[(Int, Double)] = ssc.actorStream[(Int, Double)](
      Props(classOf[NumericalTopologySimulatorActorReceiver], topology.getNode.toList, rate), "receiver")
    return values
  }


  private def createLisaMonteCarlo(allStandardisedValues: DStream[(Int, Double)], finalLisaValues: DStream[(Int, Double)], nodeMap: mutable.Map[Int,
    NodeType], topology: Topology, randomNeighbours: DStream[(Int, List[List[Int]])]) = {
    val numberOfBaseStations: Int = topology.getBasestation.size()
    val numberOfNodes: Int = topology.getNode.size()
    import org.apache.spark.SparkContext._
    import org.apache.spark.streaming.StreamingContext._

    /*
    * split up initial random neighbour DStream. Accordingly, the number values contained in batch
    * after the transformation is numRandomValues*numberOfNodes (before: numberOfNodes)
    */
    val randomNeighbourTuples: DStream[(Int, List[Int])] = randomNeighbours.flatMapValues(l => l)
    randomNeighbourTuples.repartition(numberOfBaseStations)


    // Calculates average for each of the simulated sets of neighbours
    val randomNeighbourAverages: DStream[(Int, Double)] = randomNeighbourTuples.transformWith(allStandardisedValues,
      (randomNeighbourRDD, valueRdd: RDD[(Int, Double)]) => {
      // collect complete set of values -> not parallelisable
      val valueMap: collection.Map[Int, Double] = valueRdd.collectAsMap()
      randomNeighbourRDD.mapValues{case l => {
        // retrieve values for the simulated node keys from the complete set
        val randomValues: List[Double] = valueMap.filter(t => l.contains(t._1)).values.toList
        /*
         * then calculate their average
         * (see http://oldfashionedsoftware.com/2009/07/30/lots-and-lots-of-foldleft-examples/)
        */
        randomValues.foldLeft(0.0)(_+_) / randomValues.foldLeft(0.0)((r,c) => r+1)
      }}
    })

    // calculate simulated LISA values with random neighbours
    val randomLisaValues: DStream[(Int, Double)] = randomNeighbourAverages
      .join(allStandardisedValues)
      .mapValues{ case t => t._1*t._2}

    // the significance level for a value is deduced from its position in the ordered list of simulated values
    val measuredValuesPositions = randomLisaValues.groupByKey()
      .join(finalLisaValues)
      // number of simulated values smaller than value under test / number of values
      .mapValues{ case t => (t._1.count(_ < t._2)+1)/ (t._1.size.toDouble+1.0)}
    measuredValuesPositions.saveAsTextFiles(HdfsPath+ s"/results/${numberOfBaseStations}_$numberOfNodes/measuredValuesPositions")
  }
}
