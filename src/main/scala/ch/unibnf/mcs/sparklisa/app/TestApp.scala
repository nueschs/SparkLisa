package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import akka.actor._
import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.receiver.TopologySimulatorActorReceiver
import ch.unibnf.mcs.sparklisa.topology.{BasestationType, Topology, NodeType}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
 * Created by snoooze on 04.08.14.
 */
object TestApp {

  val Master: String = "local[32]"
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
    val numBaseStations = args(2).toInt
    val nodesPerBase = topology.getNode.size() / numBaseStations
    println("nodesPerBase: "+nodesPerBase.toString)
    println("numBaseStations: "+numBaseStations.toString)

    var values: DStream[(String, Double)] = null

//    val values: ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new TopologySimulatorActorReceiver(topology.getNode.toList.slice(0,8), 60)), "receiver")
//    val values2:ReceiverInputDStream[(String, Double)] = ssc.actorStream[(String, Double)](Props(new TopologySimulatorActorReceiver(topology.getNode.toList.slice(8,16), 60)), "receiver")




    for (i <- 0 until numBaseStations){
      if (values == null){
        values = ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), 30), "receiver")
      } else {
        values = values.union(ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology.getNode.toList.slice(i*nodesPerBase, (i+1)*nodesPerBase), 30), "receiver"))
      }
    }


//     ssc.actorStream[(String, Double)](Props(classOf[TopologySimulatorActorReceiver], topology, 60), "receiver")
//    values.slice(Time)

//    values.count().print()
//    values2.print()
    values.saveAsTextFiles(HdfsPath+"/results/values")
    values.count().print()

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
