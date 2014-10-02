package ch.unibnf.mcs.sparklisa.app

import akka.actor._
import ch.unibnf.mcs.sparklisa.receiver.{TestReceiver, TemporalTopologySimulatorActorReceiver}
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestApp extends LisaDStreamFunctions with LisaAppConfiguration{

  val Master: String = "local[4]"
//  val Master: String = "spark://saight02:7077"
  var gt: Thread = null

  var statGen = RandomTupleGenerator

  def main(args: Array[String]){
    initConfig()
    val conf: SparkConf = createSparkConf()
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(10))

    import org.apache.spark.streaming.StreamingContext._
    val ssc = new StreamingContext("local[2]", "NetworkWordCount", Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    val values: DStream[Int] = ssc.actorStream[Int](Props(classOf[TestReceiver], 6.0), "receiver1")
    val t0: DStream[(Int, Int)] = values.map(i => (i, i))
    val t1: DStream[(Int, (Int, Int))] = t0.flatMap(t => {
      for (i <- 2 to 5) yield (t._1, (i, i*t._2))
    })
    t1.filter(t => t._2._2/t._2._1 != t._1).print()




    ssc.start()
    ssc.awaitTermination()
  }
}
