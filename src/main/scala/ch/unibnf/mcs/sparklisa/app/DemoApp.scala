package ch.unibnf.mcs.sparklisa.app

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Simple Word Count App, as from Spark Streaming Tutorial
 */
object DemoApp {

  def main(args: Array[String]) = {
    val ssc = new StreamingContext("local[2]", "NetworkWordCount", Seconds(15))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
