package ch.unibnf.mcs.sparklisa.app

import java.net.InetSocketAddress
import java.util.Properties

import akka.actor._
import akka.io.{IO, Tcp}
import ch.unibnf.mcs.sparklisa.app.FileInputLisaStreamingJob._
import ch.unibnf.mcs.sparklisa.receiver.TestReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * Created by snoooze on 04.08.14.
 */
object TestApp {

//  val Master: String = "local[2]"
  val Master: String = "spark://saight02:7077"
  var gt: Thread = null

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]

  def main(args: Array[String]){
    initConfig()
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

    val config: Properties = new Properties()
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    val hdfsPath = config.getProperty(config.getProperty("hdfs.path." + Env))
//    val masterHost = config.getProperty("master.host."+Env)
//
//
////    ActorSystem().actorOf(Props(classOf[Generator], 0, masterHost))
////    ActorSystem().actorOf(Props(classOf[Generator], 1, masterHost))
////    ActorSystem().actorOf(Props(classOf[Generator], 2, masterHost))
////    ActorSystem().actorOf(Props(classOf[Generator], 3, masterHost))
//
//    val b1 = ssc.socketTextStream("localhost", 25250)
//    val b2 = ssc.socketTextStream("localhost", 25251)
//    val b3 = ssc.socketTextStream("localhost", 25252)
//    val b4 = ssc.socketTextStream("localhost", 25253)
//
//    println(b1.getReceiver())
//    println(b2.getReceiver())
//    println(b3.getReceiver())
//    println(b4.getReceiver())
//
//    val vals = ssc.socketTextStream(masterHost, 25250)
//      .union(ssc.socketTextStream(masterHost, 25251))
//      .union(ssc.socketTextStream(masterHost, 25252))
//      .union(ssc.socketTextStream(masterHost, 25253))
//      .map(line => (line.split(";")(0), line.split(";")(1).toDouble))
//    vals.saveAsTextFiles(hdfsPath+"/results/test")


    val values = ssc.actorStream[(String, Double)](Props(classOf[TestReceiver], List("node1", "node2", "node3", "node4")), "receiver")
//    val values2 =  ssc.actorStream[(String, Double)](Props(classOf[TestReceiver], List("node5", "node6", "node7", "node8")), "receiver")
//    val values3 = ssc.actorStream[(String, Double)](Props(classOf[TestReceiver], List("node9", "node10", "node11", "node12")), "receiver")
//    val values4 = ssc.actorStream[(String, Double)](Props(classOf[TestReceiver], List("node13", "node14", "node15", "node16")), "receiver")
    values.saveAsTextFiles(hdfsPath+"/results/values")
//    values2.saveAsTextFiles(hdfsPath+"/results/values2")
//    values3.saveAsTextFiles(hdfsPath+"/results/values3")
//    values4.saveAsTextFiles(hdfsPath+"/results/values4")
//    val mappedValues : DStream[(String, Double)] = values.map(d => ("test_"+new Random().nextInt(3).toString, d))
//    val doubleMappedValues: DStream[(String, (String, Double))] = mappedValues.map(d => ("test_"+new Random().nextInt(3).toString, d))
//    doubleMappedValues.saveAsTextFiles(hdfsPath+"/results/test")
//    doubleMappedValues.print()

    ssc.start()
    ssc.awaitTermination()
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

//  class Generator(pos: Int, masterHost: String) extends Actor {
//
//    import akka.io.Tcp._
//    import akka.util.ByteString
//    import context.system
//
//    val random = new Random()
//
//    IO(Tcp) ! Bind(self, new InetSocketAddress(masterHost, ("2525"+pos.toString).toInt))
//
//    def receive = {
//      case c@Connected(remote, local) => {
//        val connection = sender
//        connection ! Register(self)
//
//        while(true) {
//          for (i <- 1 to 4) {
//            val test = ByteString("node" + (pos*4+i).toString + ";" + random.nextGaussian().toString + "\n")
//            sender ! Write(test)
//          }
//          Thread.sleep(500)
//        }
//      }
//    }
//  }

}
