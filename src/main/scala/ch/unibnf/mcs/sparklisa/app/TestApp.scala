package ch.unibnf.mcs.sparklisa.app

import java.net.InetSocketAddress
import java.util.Properties

import akka.actor.{Props, ActorSystem, Actor, ActorRef}
import akka.io.{IO, Tcp}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * Created by snoooze on 04.08.14.
 */
object TestApp {

  val Master: String = "local[2]"
  var gt: Thread = null
  var Env: String = null

  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Simple Streaming App")
//      .setMaster(Master)
//      .setSparkHome("/home/snoooze/spark/spark-1.0.0")
//      .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    return conf
  }

  def main(args: Array[String]){
    val conf: SparkConf = createSparkConf()
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    val config: Properties = new Properties()
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    val hdfsPath = config.getProperty(config.getProperty("hdfs.path." + Env))
    val masterHost = config.getProperty("master.host."+Env)

    ActorSystem().actorOf(Props(classOf[Generator], 0))
    ActorSystem().actorOf(Props(classOf[Generator], 1))
    ActorSystem().actorOf(Props(classOf[Generator], 2))
    ActorSystem().actorOf(Props(classOf[Generator], 3))

    val vals = ssc.socketTextStream(masterHost, 25250)
      .union(ssc.socketTextStream(masterHost, 25251))
      .union(ssc.socketTextStream(masterHost, 25252))
      .union(ssc.socketTextStream(masterHost, 25253))
      .map(line => (line.split(";")(0), line.split(";")(1).toDouble))
    vals.saveAsTextFiles(hdfsPath+"/results/test")

//    val values = ssc.actorStream[Double](Props(new TestReceiver()), "receiver")
//    val mappedValues : DStream[(String, Double)] = values.map(d => ("test_"+new Random().nextInt(3).toString, d))
//    val doubleMappedValues: DStream[(String, (String, Double))] = mappedValues.map(d => ("test_"+new Random().nextInt(3).toString, d))


    ssc.start()
    ssc.awaitTermination()
  }

  class Generator(pos: Int) extends Actor {

    import akka.io.Tcp._
    import akka.util.ByteString
    import context.system

    val random = new Random()

    IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", ("2525"+pos.toString).toInt))

    def receive = {
      case c@Connected(remote, local) => {
        val connection = sender
        connection ! Register(self)

        while(true) {
          for (i <- 1 to 4) {
            val test = ByteString("node" + (pos*4+i).toString + ";" + random.nextGaussian().toString + "\n")
            sender ! Write(test)
          }
          Thread.sleep(500)
        }
      }
    }
  }

}
