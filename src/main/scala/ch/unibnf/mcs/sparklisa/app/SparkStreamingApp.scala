package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.receiver.SparkStreamingReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingApp {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Spark Streaming App")
      .setMaster("local[17]")
      .set("spark.scheduler.mode", "FAIR")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    val randomValues1: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues2: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues3: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues4: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues5: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues6: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues7: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues8: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues9: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues10: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues11: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues12: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues13: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues14: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues15: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
    val randomValues16: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver()), "Receiver")
//    val randomValues2: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new SparkStreamingReceiver("node2")), "Receiver")

    randomValues1.saveAsTextFiles("/home/snoooze/results/randVals1")
//    randomValues1.saveAsTextFiles("/home/stefan/results/randVals1")
//    randomValues2.saveAsTextFiles("<<OUTPUT_PATH>>/randVals2")

    ssc.start()
    ssc.awaitTermination()

  }

}
