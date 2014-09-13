package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.{TimeBasedTopologySimulatorActorReceiver, SparkStreamingReceiver}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

object SparkStreamingApp {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Spark Streaming App")
      .setMaster("local[2]")
      .set("spark.scheduler.mode", "FAIR")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    import org.apache.spark.streaming.StreamingContext._

    val topology = TopologyHelper.createSimpleTopology()
    val values: ReceiverInputDStream[(String, Array[Double])] =
      ssc.actorStream[(String, Array[Double])](Props(classOf[TimeBasedTopologySimulatorActorReceiver], List(topology.getNode.toList(0)), 30, 4), "Receiver")
    values.flatMapValues(a => a.toList).saveAsTextFiles("hdfs://localhost:9999/sparkLisa/results/tbv")

//    values.saveAsTextFiles("/home/stefan/results/randVals1")

    ssc.start()
    ssc.awaitTermination()

  }

}
