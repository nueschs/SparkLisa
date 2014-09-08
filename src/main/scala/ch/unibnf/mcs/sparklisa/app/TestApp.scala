package ch.unibnf.mcs.sparklisa.app

import java.net.InetSocketAddress
import java.util.Properties

import akka.actor._
import akka.io.{IO, Tcp}
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.app.FileInputLisaStreamingJob._
import ch.unibnf.mcs.sparklisa.receiver.{TopologySimulatorActorReceiver, TestReceiver}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * Created by snoooze on 04.08.14.
 */
object TestApp {

  val Master: String = "local[2]"
//  val Master: String = "spark://saight02:7077"
  var gt: Thread = null

  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]

  def main(args: Array[String]){
    initConfig()
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(args(0).toInt))
    val topology = TopologyHelper.topologyFromBareFile(args(1), 4)


    val values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology, 60), "receiver")
    values.saveAsTextFiles(HdfsPath+"/results/values")

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
