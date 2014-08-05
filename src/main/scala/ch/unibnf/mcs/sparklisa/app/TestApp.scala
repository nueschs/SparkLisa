package ch.unibnf.mcs.sparklisa.app

import akka.actor.Props
import ch.unibnf.mcs.sparklisa.receiver.TestReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Milliseconds, Duration, StreamingContext}

import scala.util.Random

/**
 * Created by snoooze on 04.08.14.
 */
object TestApp {

  val Master: String = "local"

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App").setMaster(Master)
      .setSparkHome("/home/snoooze/spark/spark-1.0.0")
      .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    return conf
  }

  def main(args: Array[String]){
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

    val values = ssc.actorStream[Double](Props(new TestReceiver()), "receiver")
    val mappedValues : DStream[(String, Double)] = values.map(d => ("test_"+new Random().nextInt(3).toString, d))
    val flatMapped = mappedValues.flatMap(a => {
      List(("1", a._2), ("2", a._2), ("3", a._2))
    })

    flatMapped.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
