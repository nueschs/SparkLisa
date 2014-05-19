package ch.unibnf.mcs.sparklisa.receiver

import akka.util.ByteString
import akka.actor.Actor
import org.apache.spark.streaming.receivers.Receiver
import akka.actor.IOManager
import akka.actor.IO
import scala.util.Random
import java.io.InputStream
import ch.unibnf.mcs.sparklisa.xml.XmlParser
import scala.io.Source._
import javax.xml.bind.JAXBContext
import scala.collection.JavaConversions._
import ch.unibnf.mcs.sparklisa.topology.NodeType
import scala.io.Source

class SensorSimulatorActorReceiver(node: NodeType) extends Actor with Receiver {

  private final val sensorNode: NodeType = node

  private final val random = new Random()

  private final val FILE_NAME = "/node_values_4.txt"

  private var values : Array[Double] = Array[Double]();

  private var count : Int = 0

  override def preStart = init()

  case class SensorSimulator()


  def receive = {
    case _: SensorSimulator => pushNodeBlocks()
  }

  def pushNodeBlocks() = {
      Thread.sleep(50L);
      if (count < values.length){
        pushBlock((sensorNode, values(count)))
        self ! SensorSimulator()
        this.count += 1
      }
//      pushBlock((sensorNode, random.nextGaussian()))
      //    pushBlock((sensorNode, 1.0))
  }

  private def init() = {
    Thread.sleep(500L);
    val pos : Int = Integer.parseInt(sensorNode.getNodeId.replace("node", "")) - 1
    val text = Source.fromInputStream(getClass().getResourceAsStream(FILE_NAME)).mkString
    readValues(text, pos)
    self ! SensorSimulator()
  }

  private def readValues(text : String, pos: Int){
    val textArr = text.split("\n")
    val doubleStrings = textArr.map { l =>
      l.split(";")(pos)
    }
    val doubleValues = doubleStrings.map { s => s.toDouble}
    values = doubleValues
  }

}